"""ducklake target sink class, which handles writing streams."""

from __future__ import annotations

import os
import shutil
import uuid
from collections.abc import Sequence
from datetime import datetime, timezone

import polars as pl
import psutil
import pyarrow as pa
import pyarrow.parquet as pq
from singer_sdk import Target
from singer_sdk.sinks import SQLSink
from singer_sdk.sinks.core import JSONSchemaValidator

from target_ducklake.connector import (
    DELETED_AT_COLUMN,
    SINGER_DECIMAL_FORMAT,
    VERSION_COLUMN,
    DuckLakeConnector,
)
from target_ducklake.flatten import (
    flatten_record,
    flatten_schema,
    nullable_choices,
    truncate_column_names,
)
from target_ducklake.layout import (
    LayoutError,
    resolve_date_time_type,
    resolve_layout,
)
from target_ducklake.parquet_utils import (
    STRICT,
    StagingRules,
    concat_tables,
    staging_schema,
    staging_type,
)
from target_ducklake.type_hints import declares_base64_binary


def _is_timestamptz(column_type: str) -> bool:
    """Whether a DuckDB column type is TIMESTAMPTZ, by either of its names."""
    name = " ".join(column_type.upper().split())
    return name in {"TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE"}


def _is_fractional(staged: pa.DataType) -> bool:
    """Whether a staging type holds fractions: a decimal or a float."""
    return pa.types.is_decimal(staged) or pa.types.is_floating(staged)


class ducklakeSink(SQLSink):
    """ducklake target sink class."""

    connector_class = DuckLakeConnector

    def __init__(
        self,
        target: Target,
        stream_name: str,
        schema: dict,
        key_properties: Sequence[str] | None,
        connector: DuckLakeConnector | None = None,
    ) -> None:
        super().__init__(
            target, stream_name, schema, key_properties, connector=connector
        )

        self.base_temp_file_dir = self.config.get("temp_file_dir", "temp_files/")
        self.temp_file_dir = os.path.join(
            self.base_temp_file_dir, f"{self.stream_name}_{uuid.uuid4()}"
        )
        self.files_saved = 0
        # Overwrite: each batch's parquet file, loaded together at clean_up.
        self._staged_files: list[str] = []
        # ACTIVATE_VERSION: the table versions of the records received, and
        # the versions already announced by an earlier ACTIVATE_VERSION.
        self._versions_received: set[int] = set()
        self._versions_announced: set[int] = set()
        self.convert_tz_to_utc = self.config.get("convert_tz_to_utc", False)

        # NOTE: we probably don't need all this logging, but useful for debugging while target is in development
        # Log original schema for debugging
        self.logger.info(f"Original schema for stream '{stream_name}': {self.schema}")

        # Create pyarrow and Ducklake schemas
        self.flatten_max_level = self.config.get("flatten_max_level", 0)
        self.auto_cast_timestamps = self.config.get("auto_cast_timestamps", False)
        self.flatten_schema = flatten_schema(
            self.schema,
            max_level=self.flatten_max_level,
            auto_cast_timestamps=self.auto_cast_timestamps,
        )
        # Log flattened schema for debugging
        self.logger.info(
            f"Flattened schema for stream '{stream_name}': {self.flatten_schema}"
        )

        self._column_name_mapping: dict[str, str] = {}
        self._apply_column_name_truncation()

        # The column type of unhinted date-time properties, for this stream.
        self.date_time_type = resolve_date_time_type(self.config, self.stream_name)
        self.ducklake_schema = self.connector.json_to_ducklake_schema(
            self.flatten_schema, self.date_time_type
        )
        # Staged at the columns' own types, so every value they hold stages.
        self.pyarrow_schema = staging_schema(self.ducklake_schema)
        # Log pyarrow and ducklake schemas for debugging
        self.logger.info(
            f"PyArrow schema for stream '{stream_name}': {self.pyarrow_schema}"
        )
        self.logger.info(
            f"DuckLake schema for stream '{stream_name}': {self.ducklake_schema}"
        )

        # Partition keys and sort order (stream_defaults / stream_overrides,
        # table_config, or the older partition_fields), checked against the
        # stream's columns.
        self.layout = resolve_layout(
            self.config,
            self.stream_name,
            [column["name"] for column in self.ducklake_schema],
            self.logger,
            # Only table_config needs the target table; deriving it raises for
            # a stream without a schema name, which may not be needed yet.
            table=(
                (self.target_schema, self.target_table)
                if self.config.get("table_config")
                else None
            ),
            # A shared config names columns in full; Postgres catalogs truncate.
            aliases=self._column_name_mapping,
        )

        if not self.config.get("load_method"):
            self.logger.info(
                f"No load method provided for {self.stream_name}, using default merge"
            )
            self.load_method = "merge"
        else:
            self.logger.info(
                f"Load method {self.config.get('load_method')} provided for {self.stream_name}"
            )
            self.load_method = str(self.config.get("load_method"))

        # Determine if table should be overwritten
        if not self.key_properties and self.config.get("overwrite_if_no_pk", False):
            self.logger.info(
                f"Load method is overwrite for {self.stream_name}: no key properties and overwrite_if_no_pk is True"
            )
            self.should_overwrite_table = True
        elif self.load_method == "overwrite":
            self.logger.info(f"Load method is overwrite for {self.stream_name}")
            self.should_overwrite_table = True
        else:
            self.logger.info(
                f"Load method is {self.load_method} for {self.stream_name}"
            )
            self.should_overwrite_table = False
        self._overwrite_committed = False

    def get_validator(self) -> JSONSchemaValidator | None:
        """Validate records against the schema with anyOf/oneOf made nullable.

        See ``nullable_choices``. The SDK's default validates against
        ``self.schema`` itself, which stays exactly as the tap sent it.
        """
        if not self.validate_schema:
            return None
        return JSONSchemaValidator(
            nullable_choices(self.schema),
            validate_formats=self.validate_field_string_format,
        )

    @property
    def target_schema(self) -> str:
        """Get the target schema."""
        if self.config.get("default_target_schema"):
            # if default target schema is provided, use it
            self.logger.info(
                f"Using provided default target schema {self.config.get('default_target_schema')}"
            )
            return self.config.get("default_target_schema")  # type: ignore
        # if no default target schema is provided, try to derive it from the stream name
        # only works for database extractors (eg public-users becomes public.users)
        stream_name_dict = stream_name_to_dict(self.stream_name)
        if stream_name_dict.get("schema_name"):
            if self.config.get("target_schema_prefix"):
                target_schema = f"{self.config.get('target_schema_prefix')}_{stream_name_dict.get('schema_name')}"
            else:
                target_schema = stream_name_dict.get("schema_name")
            self.logger.info(
                f"Using derived target schema {target_schema} from stream name {self.stream_name}"
            )
            return target_schema
        raise ValueError(
            f"No schema name found for stream {self.stream_name} and no default target schema provided"
        )

    @property
    def target_table(self) -> str:
        """Get the target table."""
        stream_name_dict = stream_name_to_dict(self.stream_name)
        return stream_name_dict.get("table_name")

    @property
    def max_size(self) -> int:
        """Get max batch size.

        Returns:
            Max number of records to batch before `is_full=True`
        """
        return self.config.get("max_batch_size", 10000)

    def _apply_column_name_truncation(self) -> None:
        """Truncate column names for Postgres catalog compatibility.

        PostgreSQL silently truncates identifiers to 63 characters (NAMEDATALEN),
        which causes DuckLake binder errors. This method applies the same truncation
        upfront so all downstream schemas and records use consistent names.
        See: https://github.com/duckdb/ducklake/issues/619
        """
        if self.config.get("catalog_type", "postgres") != "postgres":
            return

        max_col_length = self.config.get("max_column_length", 63)
        if max_col_length <= 0:
            return

        self.flatten_schema, self._column_name_mapping = truncate_column_names(
            self.flatten_schema, max_length=max_col_length
        )
        if self._column_name_mapping:
            self.logger.info(
                f"Truncated {len(self._column_name_mapping)} column name(s) "
                f"to max length {max_col_length}: {self._column_name_mapping}"
            )

        # Uses _key_properties (the backing attribute) because key_properties is a
        # read-only property on the parent SQLSink class with no setter.
        if self._key_properties and self._column_name_mapping:
            self._key_properties = [
                self._column_name_mapping.get(kp, kp) for kp in self._key_properties
            ]

    def setup(self) -> None:
        # create the target schema if it doesn't exist
        self.connector.prepare_target_schema(target_schema_name=self.target_schema)
        # Before prepare_table applies the layout to the table.
        self._check_partition_types(
            self.connector.existing_column_types(self.target_schema, self.target_table)
        )

        # prepare the table
        table_types = self.connector.prepare_table(
            target_schema_name=self.target_schema,
            table_name=self.target_table,
            columns=self.ducklake_schema,
            layout=self.layout,
        )
        self._table_columns = list(table_types)
        # DuckDB matches identifiers case-insensitively, and the singer-sdk
        # lowercases key_properties while columns keep the property's case.
        self._table_types = {name.lower(): typ for name, typ in table_types.items()}
        self._check_blob_columns()
        self.pyarrow_schema = staging_schema(
            self._staged_columns(
                self.ducklake_schema, self._table_types, self.flatten_schema
            )
        )

    def _check_partition_types(self, existing_types: dict[str, str]) -> None:
        """Refuse an identity partition on a BLOB column.

        DuckLake stores identity partition values as text and fails on bytes
        that aren't UTF-8 ("Invalid unicode (byte sequence mismatch)"), so the
        batch would fail at insert for most binary values. A bucket works.

        The table's own type decides for a column it has (one created as
        VARCHAR keeps its text, whatever the stream now declares), the
        stream's for a new one.
        """
        types = {c["name"].lower(): c["type"] for c in self.ducklake_schema}
        types.update({name.lower(): typ for name, typ in existing_types.items()})
        for key in self.layout.partition or ():
            column_type = types.get(key.column.lower(), "")
            if key.transform == "identity" and pa.types.is_binary(
                staging_type(column_type)
            ):
                msg = (
                    f"stream {self.stream_name!r}: identity partition on BLOB "
                    f"column {key.column!r}. DuckLake stores identity partition "
                    "values as text and fails on bytes that aren't UTF-8; use "
                    "transform: bucket (with num_buckets) instead."
                )
                raise LayoutError(msg)

    def _check_blob_columns(self) -> None:
        r"""Refuse to load text into a BLOB column without its encoding.

        A table's BLOB column holds bytes; a property that doesn't declare
        ``contentEncoding: base64`` doesn't say how its text stands for them.
        DuckDB's VARCHAR to BLOB cast would store the text's own bytes
        ('hello'), read its backslash escapes ('a\x41' as 'aA'), or fail
        on other text without naming the property.
        """
        for column in self.ducklake_schema:
            name = column["name"]
            table_type = self._table_types.get(name.lower())
            if table_type is None or not pa.types.is_binary(staging_type(table_type)):
                continue
            if declares_base64_binary(self.flatten_schema.get(name, {})):
                continue
            msg = (
                f"Column {name!r} is BLOB, but property {name!r} doesn't declare "
                "contentEncoding: base64, so how its text stands for bytes is "
                "unknown. Declare contentEncoding: base64, as the column was "
                "loaded, or load the text into another column."
            )
            raise ValueError(msg)

    @staticmethod
    def _staged_columns(
        columns: list[dict[str, str]],
        table_types: dict[str, str],
        flattened: dict[str, dict],
    ) -> list[dict[str, str]]:
        """The columns to stage: each at its DDL type, or an existing column's.

        An existing column's own type is used where staging at the stream's
        type would change values before the insert casts them:
        - a number column the table has as DECIMAL, REAL or DOUBLE (staging at
          another scale rounded silently: a DECIMAL(38, 30) column staged at
          the default (38, 9) lost 21 fraction digits, and a DOUBLE column
          staged as a decimal stored 1.5e-12 as 0);
        - a number column the table has as an integer, so a value out of its
          range fails with a clear error and a fraction is rounded with a
          warning, or fails under strict ``value_type_checking``: staged as
          DECIMAL(38, 9), DuckDB's cast rounded 1.5 and 2.4 to 2 silently, and
          a key column ended up with two rows for key 2;
        - a string property the table has as a text column, whose text is then
          kept as sent, whatever its hint (a ``numeric`` hint would otherwise
          store '1.5' as '1.500000000');
        - an ``x-singer.decimal`` string the table has as DECIMAL, such as an
          unsized one in a table created as DECIMAL(18, 3) before it became
          VARCHAR: staged as text, DuckDB's cast rounded it silently, and a
          value too large failed with a bare conversion error.

        ``table_types`` is keyed by lowercased name; ``flattened`` is the
        flattened stream schema the columns were built from.
        """
        staged = []
        for column in columns:
            table_type = table_types.get(column["name"].lower())
            if table_type is None:
                staged.append(column)
                continue
            ours, theirs = staging_type(column["type"]), staging_type(table_type)
            prop = flattened.get(column["name"], {})
            types = prop.get("type") or []
            types = [types] if isinstance(types, str) else types
            use_table_type = (
                (_is_fractional(ours) and _is_fractional(theirs))
                or (_is_fractional(ours) and pa.types.is_integer(theirs))
                or (
                    pa.types.is_string(theirs)
                    and [t for t in types if t != "null"] == ["string"]
                )
                or (
                    pa.types.is_decimal(theirs)
                    and prop.get("format") == SINGER_DECIMAL_FORMAT
                )
            )
            staged.append({**column, "type": table_type} if use_table_type else column)
        return staged

    @property
    def _staging_rules(self) -> StagingRules:
        """What staging does with a value its column would round.

        From ``value_type_checking``; key columns are named so a rounded key
        is reported as one (it can merge records), or fails under strict.
        With ``date_time_type`` timestamptz, the table's TIMESTAMPTZ columns
        are named so a date-time without a zone is read as UTC.
        """
        utc_columns: frozenset[str] = frozenset()
        if self.date_time_type == "timestamptz":
            utc_columns = frozenset(
                name
                for name, column_type in self._table_types.items()
                if _is_timestamptz(column_type)
            )
        return StagingRules(
            strict=self.config.get("value_type_checking") == STRICT,
            key_columns=frozenset(key.lower() for key in self.key_properties or ()),
            utc_columns=utc_columns,
        )

    def _key_column_types(self) -> dict[str, str]:
        """Map each key property to its table column's actual type.

        Merge keys are cast to these types. They come from the table, read in
        setup, because an existing table's types can differ from this stream's
        declared types.
        """
        return {
            key: self._table_types[key.lower()]
            for key in self.key_properties
            if key.lower() in self._table_types
        }

    def process_record(self, record: dict, context: dict) -> None:
        """Process the record and flatten if necessary."""
        # Present when add_record_metadata is on: the record's table version.
        if (version := record.get(VERSION_COLUMN)) is not None:
            self._versions_received.add(version)
        record_flatten = flatten_record(
            record,
            flatten_schema=self.flatten_schema,
            max_level=self.flatten_max_level,
        )
        # Apply column name truncation mapping
        if self._column_name_mapping:
            record_flatten = {
                self._column_name_mapping.get(k, k): v
                for k, v in record_flatten.items()
            }
        super().process_record(record_flatten, context)

    def _remove_temp_table_duplicates(self, pyarrow_df):
        """Remove duplicates within PyArrow table based on key properties."""
        if not self.key_properties or pyarrow_df is None:
            return pyarrow_df

        # Lowercase column names, SQLSink parent class cleans schema objects to lowercase
        # https://github.com/meltano/sdk/blob/43502d246e94c5f51252dc7d00c758be8e6500f7/singer_sdk/sql/sink.py#L206
        pyarrow_df = pyarrow_df.rename_columns(
            [col.lower() for col in pyarrow_df.column_names]
        )

        # Check that all key properties exist in the pyarrow table
        available_columns = set(pyarrow_df.column_names)
        for key_property in self.key_properties:
            if key_property not in available_columns:
                raise ValueError(
                    f"Key property {key_property} not found in pyarrow temp file for {self.target_table}. Available columns: {list(available_columns)}"
                )

        original_row_count = len(pyarrow_df)
        # Convert to polars, drop duplicates, then back to pyarrow.
        # Keep the last occurrence: records arrive in stream order, so the last one
        # is the newest version of the row. Polars' default (keep="any") may keep
        # an older version and overwrite the newer one in the target table.
        polars_df = pl.from_arrow(pyarrow_df)
        polars_df = polars_df.unique(
            subset=self.key_properties, keep="last", maintain_order=True
        )
        pyarrow_df = polars_df.to_arrow()

        new_row_count = len(pyarrow_df)
        duplicates_removed = original_row_count - new_row_count
        if duplicates_removed > 0:
            self.logger.info(
                f"Removed {duplicates_removed} duplicate rows based on key properties: {self.key_properties}"
            )

        return pyarrow_df

    def _get_memory_usage_mb(self) -> float:
        """Get current process memory usage in MB."""
        process = psutil.Process()
        return process.memory_info().rss / (1024 * 1024)

    def write_temp_file(self, context: dict) -> str:
        """Write the current batch to a temporary parquet file."""
        # Track memory before writing temp file
        memory_before_mb = self._get_memory_usage_mb()
        self.logger.info(
            f"[Memory] Before write_temp_file for {self.stream_name}: {memory_before_mb:.2f} MB"
        )

        self.logger.info(
            f"Writing batch for {self.stream_name} with {len(context['records'])} records to parquet file."
        )
        memory_before_concat = self._get_memory_usage_mb()
        pyarrow_df = None
        pyarrow_df = concat_tables(
            records=context.get("records", []),
            pyarrow_table=pyarrow_df,
            pyarrow_schema=self.pyarrow_schema,  # type: ignore
            flattened_schema=self.flatten_schema,
            convert_tz_to_utc=self.convert_tz_to_utc,
            rules=self._staging_rules,
        )
        memory_after_concat = self._get_memory_usage_mb()
        memory_diff_concat = memory_after_concat - memory_before_concat
        self.logger.info(
            f"[Memory] After concatenating tables: {memory_after_concat:.2f} MB (was {memory_before_concat:.2f} MB before)"
            f"(delta: {memory_diff_concat:+.2f} MB, {memory_diff_concat / memory_before_concat * 100:.2f}%)"
        )

        # Drop duplicates based on key properties if they exist in temp file
        # TODO: we may be able to skip this step. Deduplication handled when merging into DuckLake.
        if self.key_properties and pyarrow_df is not None:
            memory_before_dedup = self._get_memory_usage_mb()
            pyarrow_df = self._remove_temp_table_duplicates(pyarrow_df)

            memory_after_dedup = self._get_memory_usage_mb()
            memory_diff_dedup = memory_after_dedup - memory_before_dedup
            self.logger.info(
                f"[Memory] After deduplicating temp file: {memory_after_dedup:.2f} MB (was {memory_before_dedup:.2f} MB before)"
                f"(delta: {memory_diff_dedup:+.2f} MB, {memory_diff_dedup / memory_before_dedup * 100:.2f}%)"
            )

        self.logger.info(
            f"Batch pyarrow table has ({len(pyarrow_df)} rows)"  # type: ignore
        )
        del context["records"]

        # Ensure temp directory exists
        os.makedirs(self.temp_file_dir, exist_ok=True)

        # Create a more descriptive filename with timestamp
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        file_name = f"{self.stream_name}_{timestamp}_{self.files_saved:05d}.parquet"
        full_temp_file_path = os.path.join(self.temp_file_dir, file_name)

        try:
            self.logger.info(f"Writing batch to temp file: {full_temp_file_path}")
            pq.write_table(pyarrow_df, full_temp_file_path)
            self.files_saved += 1
            row_count = len(pyarrow_df) if pyarrow_df is not None else 0
            self.logger.info(f"Successfully wrote {row_count} rows to {file_name}")

            # Track memory after writing temp file
            memory_after_mb = self._get_memory_usage_mb()
            memory_diff_mb = memory_after_mb - memory_before_mb
            self.logger.info(
                f"[Memory] After write_temp_file for {self.stream_name}: {memory_after_mb:.2f} MB (was {memory_before_mb:.2f} MB at start)"
                f"(delta: {memory_diff_mb:+.2f} MB, {memory_diff_mb / memory_before_mb * 100:.2f}%)"
            )

            return full_temp_file_path
        except Exception as e:
            self.logger.error(
                f"Failed to write parquet file {full_temp_file_path}: {e}"
            )
            raise

    def process_batch(self, context: dict) -> None:
        """Process a batch of records."""
        # first write each batch to a parquet file
        temp_file_path = self.write_temp_file(context)
        self.logger.info(f"Temp file path: {temp_file_path}")

        if self.should_overwrite_table:
            # Kept, and loaded with every other batch in one transaction at
            # clean_up, so readers see the old table until the new one is whole.
            self._staged_files.append(temp_file_path)
            return

        # get the columns from the file and the table
        file_columns = [col["name"] for col in self.ducklake_schema]
        table_columns = self._table_columns

        # If no key properties, or load method is append, we simply insert the data.
        if not self.key_properties or self.load_method == "append":
            self.connector.insert_into_table(
                file_location=temp_file_path,
                target_schema_name=self.target_schema,
                table_name=self.target_table,
                file_columns=file_columns,
                target_table_columns=table_columns,
            )

        # If load method is merge, we merge the data
        elif self.load_method == "merge":
            self.connector.merge_into_table(
                file_location=temp_file_path,
                target_schema_name=self.target_schema,
                table_name=self.target_table,
                file_columns=file_columns,
                target_table_columns=table_columns,
                key_properties=list(self.key_properties),
                key_column_types=self._key_column_types(),
            )

        # delete file after insert
        os.remove(temp_file_path)
        self.logger.info(
            f"[Memory] After merging temp file: {self._get_memory_usage_mb():.2f} MB"
        )

    def clean_up(self) -> None:
        """Perform per-stream cleanup.

        For an overwrite, replace the table's rows with every staged batch in
        one transaction first. The SDK cleans up after the final drain and
        before it writes STATE, so STATE follows the commit, and a failed
        replace fails the run with the old rows intact.
        """
        try:
            if self.has_uncommitted_overwrite:
                self.connector.replace_table_contents(
                    file_locations=self._staged_files,
                    target_schema_name=self.target_schema,
                    table_name=self.target_table,
                    file_columns=[col["name"] for col in self.ducklake_schema],
                    target_table_columns=self._table_columns,
                )
                self._overwrite_committed = True
            super().clean_up()
        finally:
            self._remove_temp_file_dir()

    @property
    def has_uncommitted_overwrite(self) -> bool:
        """Whether this is an overwrite whose replace hasn't been committed yet.

        Until it is, STATE must not be emitted (see
        ``Targetducklake._write_state_message``).
        """
        return self.should_overwrite_table and not self._overwrite_committed

    def _remove_temp_file_dir(self) -> None:
        """Remove this stream's dedicated temp directory."""
        if hasattr(self, "temp_file_dir"):
            self.logger.info(
                f"Cleaning up temp directory for stream {self.stream_name}: {self.temp_file_dir}"
            )
            try:
                if os.path.exists(self.temp_file_dir):
                    shutil.rmtree(self.temp_file_dir)
            except Exception as e:
                self.logger.warning(
                    f"Non-fatal error during temp directory cleanup for {self.stream_name}: {e}"
                )

    def activate_version(self, new_version: int) -> None:
        """Handle ACTIVATE_VERSION: retire rows that are not in the activated version.

        A full-table tap sends its records with a table version, then
        ACTIVATE_VERSION for it; rows of any other version (or none) are gone
        at the source. As in the SDK, they are marked deleted
        (``_sdc_deleted_at`` set) by default, and removed with ``hard_delete``. The target drains this sink first (see
        ``Targetducklake._process_activate_version_message``), so no record
        of the version is still buffered.

        Nothing is deleted when:
        - records carry no version (``add_record_metadata`` is off);
        - no record of this version has been received and the version
          wasn't announced before, as when a tap announces the version
          before sending its records, which would otherwise empty the table
          for the whole sync (a second ACTIVATE_VERSION with no records
          between means the source is empty, and every other row goes);
        - the stream is an overwrite, which replaces the whole table at
          clean_up anyway, and must not empty it early.
        """
        if not self.include_sdc_metadata_properties:
            self.logger.warning(
                f"Ignoring ACTIVATE_VERSION {new_version} for {self.stream_name}: "
                "records carry their table version only with add_record_metadata, "
                "so rows of older versions can't be told apart."
            )
            return
        if self.should_overwrite_table:
            self.logger.info(
                f"ACTIVATE_VERSION {new_version} for {self.stream_name}: the "
                "overwrite replaces the whole table at the end of the stream."
            )
            return
        if (
            new_version not in self._versions_received
            and new_version not in self._versions_announced
        ):
            # A second ACTIVATE_VERSION for the version, even with no records
            # between, ends its sync: the source is then empty.
            self._versions_announced.add(new_version)
            self.logger.info(
                f"ACTIVATE_VERSION {new_version} for {self.stream_name} arrived "
                "before any of its records; nothing to remove yet."
            )
            return
        if self.config.get("hard_delete", False):
            self.connector.delete_other_versions(
                self.target_schema, self.target_table, new_version
            )
            return
        # As the SDK does by default: mark them deleted, at the current UTC
        # time, and keep them. A row the source sends again is revived when
        # it is next loaded (its own _sdc_deleted_at is empty).
        deleted_at_type = self._table_types.get(DELETED_AT_COLUMN, "")
        self.connector.mark_other_versions_deleted(
            self.target_schema,
            self.target_table,
            new_version,
            deleted_at_is_timestamptz=_is_timestamptz(deleted_at_type),
        )


def stream_name_to_dict(stream_name, separator="-"):
    catalog_name = None
    schema_name = None
    table_name = stream_name

    # Schema and table name can be derived from stream if it's in <schema_nama>-<table_name> format
    s = stream_name.split(separator)
    if len(s) == 2:
        schema_name = s[0]
        table_name = s[1]
    if len(s) > 2:
        catalog_name = s[0]
        schema_name = s[1]
        table_name = "_".join(s[2:])

    return {
        "catalog_name": catalog_name,
        "schema_name": schema_name,
        "table_name": table_name,
    }
