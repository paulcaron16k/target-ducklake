"""DuckLake connector for managing database connections."""

from __future__ import annotations

import logging
import math
import time
import warnings
from typing import Any, TypeGuard

import duckdb
from singer_sdk.connectors import SQLConnector
from singer_sdk.connectors.sql import JSONSchemaToSQL
from sqlalchemy.types import (
    BIGINT,
    DECIMAL,
    INTEGER,
    JSON,
    VARCHAR,
    TypeEngine,
    UserDefinedType,
)

from target_ducklake.layout import PartitionKey, SortKey, TableLayout
from target_ducklake.parquet_utils import number_precision_and_scale
from target_ducklake.type_hints import (
    decimal_hint,
    declares_base64_binary,
    hint_fits_json_type,
    hinted_type,
    is_binary_hint,
    is_decimal_hint,
    is_default_type_hint,
)

logger = logging.getLogger(__name__)


# The SDK's record metadata columns: the record's table version, and the time
# a row was deleted at the source. Short enough never to be truncated.
VERSION_COLUMN = "_sdc_table_version"
DELETED_AT_COLUMN = "_sdc_deleted_at"

# Integer type widths for deciding safe in-place widening (e.g. INTEGER -> BIGINT).
_INT_TYPE_RANK = {
    "TINYINT": 1,
    "SMALLINT": 2,
    "INT": 3,
    "INTEGER": 3,
    "BIGINT": 4,
    "HUGEINT": 5,
}

# Postgres error messages that indicate transient connectivity issues.
# These surface as plain duckdb.Error (not IOException) when DuckLake's
# internal snapshot query hits a dropped Postgres connection.
_TRANSIENT_ERROR_PATTERNS = (
    "SSL connection has been closed unexpectedly",
    "Failed to get data file list from DuckLake"
)


def is_transient_error(error: duckdb.Error) -> bool:
    """Check if a duckdb.Error is a transient connection issue worth retrying."""
    msg = str(error)
    return any(pattern in msg for pattern in _TRANSIENT_ERROR_PATTERNS)


def _by_lower_name(column_types: dict[str, str]) -> dict[str, tuple[str, str]]:
    """Map each lowercased column name to the column's (name, type)."""
    return {name.lower(): (name, typ) for name, typ in column_types.items()}


def _same_keys(
    wanted: tuple[PartitionKey, ...] | tuple[SortKey, ...],
    current: tuple[PartitionKey, ...] | tuple[SortKey, ...] | None,
) -> bool:
    """Whether the wanted keys are the current ones, in order."""
    return [key.compare_key() for key in wanted] == [
        key.compare_key() for key in current or ()
    ]


def _sql_literal(value: str) -> str:
    """A SQL string literal for the value, with single quotes escaped."""
    return "'" + value.replace("'", "''") + "'"


def _unquoted(identifier: str) -> str:
    """An identifier as DuckLake stores it in a sort expression, without quotes.

    A name that needs quoting (``"Foo Bar"``) is stored quoted; a plain one
    (``Tenant``) isn't. Comparing the unquoted name keeps an unchanged sort
    order from being re-applied every run.
    """
    if len(identifier) > 1 and identifier[0] == identifier[-1] == '"':
        return identifier[1:-1].replace('""', '"')
    return identifier


class DuckLakeConnectorError(Exception):
    """Custom exception for DuckLake connector errors."""


class DuckDBType(UserDefinedType):
    """A column type rendered exactly as its DuckDB name, e.g. ``TIMESTAMPTZ``.

    SQLAlchemy's generic types lose detail when rendered: ``TIMESTAMP(timezone=True)``
    renders as plain ``TIMESTAMP``.
    """

    cache_ok = True

    def __init__(self, name: str) -> None:
        """Store the DuckDB type name."""
        self.name = name

    def get_col_spec(self, **_kw: Any) -> str:
        """Return the DuckDB type name."""
        return self.name


SINGER_DECIMAL_FORMAT = "x-singer.decimal"


def _is_count(value: object) -> TypeGuard[int]:
    """Whether a declared precision or scale is a plain integer (``True`` isn't)."""
    return isinstance(value, int) and not isinstance(value, bool)


def singer_decimal_size(schema: dict) -> tuple[int, int] | None:
    """The (precision, scale) of an ``x-singer.decimal`` string, if both are usable.

    Both must be integers, precision at least 1 and scale from 0 to precision,
    as target-iceberg requires both: a source that declares a precision but no
    scale is declaring an arbitrary scale (``scale: 0`` is a real scale).
    """
    precision, scale = schema.get("precision"), schema.get("scale")
    if (
        schema.get("format") == SINGER_DECIMAL_FORMAT
        and _is_count(precision)
        and _is_count(scale)
        and 0 <= scale <= precision
        and precision >= 1
    ):
        return precision, scale
    return None


def is_unsized_singer_decimal(schema: dict) -> bool:
    """Whether a property is an ``x-singer.decimal`` string loaded as VARCHAR.

    It is, without a usable precision and scale (``singer_decimal_size``),
    unless an ``x-sql-datatype`` decides the column: a sized ``decimal(p,s)``
    hint, or a supported non-decimal one such as ``double``. A bare
    ``decimal`` or ``numeric`` hint doesn't size it, so it doesn't decide.
    """
    if schema.get("format") != SINGER_DECIMAL_FORMAT or singer_decimal_size(schema):
        return False
    if decimal_hint(schema) is not None:
        return False
    return (
        is_decimal_hint(schema)
        or is_default_type_hint(schema)
        or hinted_type(schema) is None
    )


class JSONSchemaToDuckLake(JSONSchemaToSQL):
    """Convert JSON Schema types to DuckLake compatible types.

    ``date_time_type`` is the column type of a ``date-time`` property without
    an ``x-sql-datatype`` hint: ``timestamptz`` (TIMESTAMPTZ, the default) or
    ``timestamp`` (TIMESTAMP, wall-clock time).
    """

    def to_sql_type(self, schema: dict) -> TypeEngine:
        """Convert a property schema, honouring its ``x-sql-datatype`` hint first.

        A supported hint (see ``type_hints``) gives the source column's own type;
        an unsupported one falls back to the JSON Schema type and format, with
        the SDK's warning. A supported hint that the property's JSON type can't
        be loaded into (``uuid`` on an integer, see ``hint_fits_json_type``) is
        ignored with a warning, and the JSON type decides the column.

        A string sent as base64 (``contentEncoding: base64``) is BLOB, with or
        without a binary hint; a binary hint without that encoding is ignored
        (``json_to_ducklake_schema`` warns, naming the property).
        """
        unhinted = {k: v for k, v in schema.items() if k != "x-sql-datatype"}
        if (declared := self._declared_text_type(schema)) is not None:
            return declared
        if is_default_type_hint(schema) or is_binary_hint(schema):
            return super().to_sql_type(unhinted)
        hinted = "DECIMAL" if is_decimal_hint(schema) else hinted_type(schema)
        if hinted is None:
            return super().to_sql_type(schema)
        if hint_fits_json_type(schema, hinted):
            if hinted == "DECIMAL":
                return DECIMAL(*number_precision_and_scale(schema))
            return DuckDBType(hinted)
        warnings.warn(
            f"Ignoring x-sql-datatype '{schema['x-sql-datatype']}': values of "
            f"JSON type {schema.get('type')} can't be loaded into {hinted}.",
            UserWarning,
            stacklevel=2,
        )
        return super().to_sql_type(unhinted)

    @staticmethod
    def _declared_text_type(schema: dict) -> TypeEngine | None:
        """The column of a string whose text the schema says how to read, if any.

        An unsized ``x-singer.decimal`` is VARCHAR, keeping every digit; a
        string sent as base64 is BLOB.
        """
        if is_unsized_singer_decimal(schema):
            return VARCHAR()
        if declares_base64_binary(schema):
            return DuckDBType("BLOB")
        return None

    def __init__(self, *args, date_time_type: str = "timestamptz", **kwargs):
        super().__init__(*args, **kwargs)
        if date_time_type == "timestamptz":
            self.register_format_handler(
                "date-time", lambda _: DuckDBType("TIMESTAMPTZ")
            )
        # Override the default type handler for object and array types
        self.register_type_handler("object", JSON)
        self.register_type_handler("array", JSON)
        # Override the default type handler for integer type
        self.register_type_handler("integer", self._handle_integer_type)
        # Override number handling to use high-precision decimals by default
        self.register_type_handler("number", self._handle_number_type)
        self.register_format_handler(
            SINGER_DECIMAL_FORMAT, self._handle_singer_decimal_format
        )

    def _handle_singer_decimal_format(self, jsonschema: dict) -> TypeEngine:
        """Handle a string with format ``x-singer.decimal``.

        With a usable precision and scale, ``DECIMAL(precision, scale)``, spelt
        out so the values are staged at that type and any rounding is reported;
        a precision above 38 is capped, as for a JSON number. Otherwise the
        decimal is unsized, which no DuckDB DECIMAL holds: the SDK's bare
        ``DECIMAL`` is DECIMAL(18, 3) in DuckDB, which rounds to 3 places and
        fails on more than 15 integer digits, and its ``DECIMAL(precision)`` is
        scale 0, rounding every fraction. So the column is VARCHAR, keeping the
        text digit for digit, as target-iceberg keeps it as a string.
        """
        size = singer_decimal_size(jsonschema)
        if size is None:
            return VARCHAR()
        precision, scale = size
        return DECIMAL(
            *number_precision_and_scale({"precision": precision, "scale": scale})
        )

    def _handle_integer_type(self, jsonschema: dict) -> INTEGER | BIGINT:
        """Handle integer type."""
        minimum = jsonschema.get("minimum", -math.inf)
        maximum = jsonschema.get("maximum", math.inf)
        if minimum >= -(2**31) and maximum < 2**31:
            return INTEGER()
        return BIGINT()

    def _handle_number_type(self, jsonschema: dict) -> DECIMAL:
        """Handle number type with safe default precision.

        Uses the same (precision, scale) as the parquet staging schema, so the
        staged decimal values fit the column exactly. DuckDB supports precision
        up to 38.
        """
        return DECIMAL(*number_precision_and_scale(jsonschema))


class DuckLakeConnector(SQLConnector):
    """Handles DuckLake database connections and setup."""

    def __init__(self, config: dict[str, Any]) -> None:
        """Initialize the DuckLake connector with configuration.

        Args:
            config: Configuration dictionary containing connection parameters

        Raises:
            DuckLakeConnectorError: If required configuration is missing
        """
        super().__init__(config=config)
        self._validate_config()

        self.catalog_url = config.get("catalog_url")
        self.data_path = config.get("data_path")
        self.catalog_type = config.get("catalog_type", "postgres")
        self.storage_type = config.get("storage_type")
        self.public_key = config.get("public_key")
        self.secret_key = config.get("secret_key")
        self.region = config.get("region")

        self._connection: duckdb.DuckDBPyConnection | None = None
        self.catalog_name = "ducklake_catalog"
        self.meta_schema = config.get("meta_schema")

    def _validate_config(self) -> None:
        """Validate required configuration parameters."""
        required_params = ["catalog_url", "data_path"]
        missing_params = [
            param for param in required_params if not self.config.get(param)
        ]

        if missing_params:
            raise DuckLakeConnectorError(
                f"Missing required configuration parameters: {', '.join(missing_params)}"
            )

        # Validate cloud storage configuration
        storage_type = self.config.get("storage_type")
        if storage_type in ["GCS", "S3"]:
            has_public_key = bool(self.config.get("public_key"))
            has_secret_key = bool(self.config.get("secret_key"))

            if has_public_key != has_secret_key:
                # One is set but not the other
                missing_key = "secret_key" if has_public_key else "public_key"
                raise DuckLakeConnectorError(
                    f"Storage type is {storage_type} and {('public_key' if has_public_key else 'secret_key')} "
                    f"is provided, but {missing_key} is missing. Both must be provided together."
                )

            if not has_public_key and not has_secret_key:
                logger.info(
                    f"Storage type is {storage_type} but no authentication credentials provided. "
                    f"This will work for public storage or if using instance/environment credentials "
                    f"(e.g., IAM roles, service accounts)."
                )

            # For S3 with explicit credentials, region is required
            if (
                storage_type == "S3"
                and has_public_key
                and not self.config.get("region")
            ):
                raise DuckLakeConnectorError(
                    "Storage type is S3 with explicit credentials, but region is missing. "
                    "Region is required for S3 authentication."
                )

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create the DuckDB connection."""
        if self._connection is None:
            self._connection = self._create_connection()
            logger.info("DuckDB and DuckLake catalog connection created.")
        return self._connection

    def _create_connection(self) -> duckdb.DuckDBPyConnection:
        """Create and configure a new DuckDB connection."""
        try:
            logger.info("Creating DuckDB connection")
            conn = duckdb.connect(database=":memory:")

            # Execute startup script to configure DuckLake
            startup_script = self._build_startup_script()
            conn.execute(startup_script)

            return conn
        except Exception as e:
            raise DuckLakeConnectorError(
                f"Failed to create DuckDB connection: {e}"
            ) from e

    def _build_attach_statement(self) -> str:
        """Build the ATTACH statement for the DuckLake catalog."""
        if self.catalog_type == "duckdb":
            logger.info(
                f"Detected DuckDB catalog type: {self.catalog_type}. Local DuckDB catalog will be used."
            )
            return f"ATTACH 'ducklake:metadata.ducklake' AS {self.catalog_name};"

        attach_params = {
            "DATA_PATH": f"'{self.data_path}'",
            "DATA_INLINING_ROW_LIMIT": 0,
        }
        if self.meta_schema:
            attach_params["META_SCHEMA"] = f"'{self.meta_schema}'"

        params_str = ", ".join(
            f"{key} {value}" for key, value in attach_params.items()
        )
        return f"ATTACH 'ducklake:{self.catalog_type}:{self.catalog_url}' AS {self.catalog_name} ({params_str});"

    def _build_startup_script(self) -> str:
        """Build the DuckDB startup script with extensions and attachments."""
        script_parts = [
            "INSTALL ducklake;",
            "INSTALL postgres;",
            "LOAD postgres;",
            "SET ducklake_max_retry_count=100;",
            # SET GLOBAL is required on duckdb-postgres < f81dd35 (2026-04-20):
            # before that fix, pg_pool_* options register with default (SESSION)
            # scope, so plain SET only writes to the user's session — DuckLake's
            # internal child Connection that runs the postgres ATTACH won't see
            # them and the pool is built with defaults (no reaper, max=10).
            "SET GLOBAL pg_pool_max_connections=64;",
            "SET GLOBAL pg_pool_idle_timeout_millis=60000;",
            "SET GLOBAL pg_pool_enable_reaper_thread=true;",
            "SET GLOBAL pg_pool_enable_thread_local_cache=false;",
        ]

        script_parts.append(self._build_attach_statement())

        # Add secrets for cloud storage if configured
        if self.public_key and self.secret_key:
            if self.storage_type == "GCS":
                script_parts.append(
                    f"CREATE SECRET ("
                    f"TYPE gcs, "
                    f"KEY_ID '{self.public_key}', "
                    f"SECRET '{self.secret_key}'"
                    ");"
                )
            elif self.storage_type == "S3":
                # For S3, region is required when using explicit credentials
                secret_parts = [
                    f"TYPE s3, KEY_ID '{self.public_key}'",
                    f"SECRET '{self.secret_key}'",
                    f"REGION '{self.region}'",  # Always include since we validate it's required
                ]

                script_parts.append("CREATE SECRET (" + ", ".join(secret_parts) + ");")
        elif self.storage_type in ["GCS", "S3"]:
            # Log info when cloud storage is configured but no auth is provided
            logger.info(
                f"Storage type is {self.storage_type} but no authentication credentials provided. "
                f"Will attempt to access storage without explicit authentication. "
                f"This will only work for public buckets or if using instance/environment credentials."
            )

        return "\n".join(script_parts)

    def execute(self, query: str, parameters: Any = None, max_retries: int = 3) -> Any:
        """Execute a query on the DuckDB connection with retry on transient errors.

        Retries on duckdb.IOException (catalog connectivity, S3/GCS errors) with
        exponential backoff. All other exceptions are raised immediately.

        Args:
            query: SQL query to execute
            parameters: Optional parameters for the query
            max_retries: Maximum number of attempts (default 3)

        Returns:
            Query result

        Raises:
            DuckLakeConnectorError: If query execution fails after all retries
        """
        for attempt in range(1, max_retries + 1):
            try:
                logger.debug(f"Executing query: {query}")
                return self.connection.cursor().execute(query, parameters)
            except duckdb.IOException as e:
                sleep_time = 2 ** attempt
                logger.warning(
                    f"Transient IO error executing query (attempt {attempt}/{max_retries}): {e}. "
                    f"Retrying in {sleep_time}s..."
                )
                if attempt == max_retries:
                    raise DuckLakeConnectorError(
                        f"Query {query} failed after {max_retries} attempts with error: {e}"
                    ) from e
                time.sleep(sleep_time)
            except duckdb.Error as e:
                if not is_transient_error(e):
                    raise DuckLakeConnectorError(
                        f"Query {query} failed with error: {e}"
                    ) from e
                sleep_time = 2 ** attempt
                logger.warning(
                    f"Transient connection error executing query (attempt {attempt}/{max_retries}): {e}. "
                    f"Retrying in {sleep_time}s..."
                )
                if attempt == max_retries:
                    raise DuckLakeConnectorError(
                        f"Query {query} failed after {max_retries} attempts with error: {e}"
                    ) from e
                time.sleep(sleep_time)
            except Exception as e:
                raise DuckLakeConnectorError(
                    f"Query {query} failed with error: {e}"
                ) from e
        return None  # unreachable: last attempt either returns or raises

    def _check_if_schema_exists(self, target_schema_name: str) -> bool:
        """Check if the schema exists in the catalog."""
        check_schema_query = f"""
        SELECT
            COUNT(*) AS cnt
        FROM information_schema.schemata
            WHERE catalog_name = '{self.catalog_name}'
            AND schema_name = '{target_schema_name}';
        """
        logger.info(f"Checking if schema {target_schema_name} exists")
        result = self.execute(check_schema_query)
        return result.fetchone()[0] > 0

    def prepare_target_schema(self, target_schema_name: str) -> None:
        """Prepare the schema for the target table."""
        if self._check_if_schema_exists(target_schema_name):
            logger.info(
                f"Schema {target_schema_name} already exists in catalog {self.catalog_name}. Skipping creation."
            )
            return
        logger.info(f"Schema {target_schema_name} does not exist in catalog {self.catalog_name}. Creating schema.")
        create_schema_query = (
            f"CREATE SCHEMA IF NOT EXISTS {self.catalog_name}.{target_schema_name};"
        )
        logger.info(
            f"Preparing schema {target_schema_name} in catalog {self.catalog_name} with query {create_schema_query}"
        )
        self.execute(create_schema_query)

    def _check_if_table_exists(self, target_schema_name: str, table_name: str) -> bool:
        """Check if the table exists in the target schema."""
        check_table_query = f"""
        SELECT
            COUNT(*) AS cnt
        FROM information_schema.tables
            WHERE table_schema = '{target_schema_name}'
            AND table_name   = '{table_name}';
        """
        logger.info(f"Checking if table {table_name} exists")
        result = self.execute(check_table_query)
        return result.fetchone()[0] > 0

    def get_table_columns(  # type: ignore[override]
        self, target_schema_name: str, table_name: str
    ) -> list[str]:
        """Get the columns of the table."""
        get_columns_query = f"PRAGMA TABLE_INFO('{self.catalog_name}.{target_schema_name}.{table_name}');"
        logger.info(f"Getting columns for table {table_name}")
        result = self.execute(get_columns_query).fetchall()
        return [col[1] for col in result]

    def get_table_column_types(
        self, target_schema_name: str, table_name: str
    ) -> dict[str, str]:
        """Get the columns of the table mapped to their DuckDB types."""
        get_columns_query = f"PRAGMA TABLE_INFO('{self.catalog_name}.{target_schema_name}.{table_name}');"
        logger.info(f"Getting column types for table {table_name}")
        result = self.execute(get_columns_query).fetchall()
        return {col[1]: col[2] for col in result}

    def _add_columns(
        self,
        target_schema_name: str,
        table_name: str,
        columns: list[dict[str, str]],
    ) -> None:
        """Add columns to the table one by one."""
        for col in columns:
            add_column_query = f'ALTER TABLE {self.catalog_name}.{target_schema_name}.{table_name} ADD COLUMN "{col["name"]}" {col["type"]};'
            logger.info(
                f"Adding column {col['name']} ({col['type']}) to table {table_name}"
            )
            self.execute(add_column_query)

    @staticmethod
    def _safe_widen_target(current_type: str, desired_type: str) -> str | None:
        """Return desired_type if it safely widens current_type in place, else None.

        Only same-family promotions DuckLake applies as metadata-only changes are
        allowed: integer-family widenings (e.g. INTEGER -> BIGINT) and FLOAT/REAL ->
        DOUBLE. Equal types, narrowings, and cross-family changes (e.g. DOUBLE ->
        BIGINT, VARCHAR -> JSON) return None.
        """
        current = current_type.strip().upper()
        desired = desired_type.strip().upper()
        if current == desired:
            return None
        if current in _INT_TYPE_RANK and desired in _INT_TYPE_RANK:
            if _INT_TYPE_RANK[desired] > _INT_TYPE_RANK[current]:
                return desired_type
            return None
        if current in {"FLOAT", "REAL"} and desired == "DOUBLE":
            return desired_type
        return None

    def _alter_column_type(
        self,
        target_schema_name: str,
        table_name: str,
        column: str,
        new_type: str,
    ) -> None:
        """Widen an existing column's type in place."""
        alter_query = f'ALTER TABLE {self.catalog_name}.{target_schema_name}.{table_name} ALTER COLUMN "{column}" SET DATA TYPE {new_type};'
        logger.info(f"Widening column {column} to {new_type} in table {table_name}")
        self.execute(alter_query)

    def _reconcile_column_types(
        self,
        target_schema_name: str,
        table_name: str,
        columns: list[dict[str, str]],
        current_types: dict[str, str],
    ) -> None:
        """Widen existing columns whose desired type is wider than the current type.

        current_types is a name->type snapshot taken before columns were added, so
        newly added columns are absent and skipped (already created as desired).
        Names match case-insensitively, as DuckDB identifiers do.
        """
        current_by_name = _by_lower_name(current_types)
        for col in columns:
            current = current_by_name.get(col["name"].lower())
            if current is None:
                continue
            table_column, current_type = current
            new_type = self._safe_widen_target(current_type, col["type"])
            if new_type is not None:
                self._alter_column_type(
                    target_schema_name, table_name, table_column, new_type
                )

    def _create_empty_table(
        self,
        target_schema_name: str,
        table_name: str,
        columns: list[dict[str, str]],
    ) -> None:
        """Create an empty table in the target schema."""
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.catalog_name}.{target_schema_name}.{table_name} (
            {", ".join([f'"{col["name"]}" {col["type"]}' for col in columns])}
        );
        """
        logger.info(
            f"Creating table {table_name} in schema {target_schema_name} with query {create_table_query}"
        )
        self.execute(create_table_query)

    def prepare_table( # type: ignore[override]
        self,
        target_schema_name: str,
        table_name: str,
        columns: list[dict[str, str]],
        layout: TableLayout | None = None,
    ) -> dict[str, str]:
        """Prepare the table for the target schema.
        If the table doesn't exist, create it.
        Add missing columns to existing table if necessary.
        Returns every column of the table, in table order, mapped to its type.
        """
        if not self._check_if_table_exists(target_schema_name, table_name):
            logger.info(f"Table {table_name} does not exist in schema {target_schema_name}. Creating empty table.")
            self._create_empty_table(target_schema_name, table_name, columns)
        else:
            logger.info(
                f"Table {table_name} already exists in schema {target_schema_name}. Checking if columns match..."
            )
            existing_column_types = self.get_table_column_types(
                target_schema_name, table_name
            )
            # DuckDB identifiers are case-insensitive: a stream's "id" is the
            # table's existing "ID", and adding it again fails.
            existing_names = _by_lower_name(existing_column_types)
            new_columns = [
                col for col in columns if col["name"].lower() not in existing_names
            ]
            if new_columns:
                logger.info(f"Adding new columns to table {table_name}: {new_columns}")
                self._add_columns(target_schema_name, table_name, new_columns)
            else:
                logger.info(f"Columns match for table {table_name}. No new columns to add.")
            # widen any existing column whose desired type is wider (uses pre-add snapshot)
            self._reconcile_column_types(
                target_schema_name, table_name, columns, existing_column_types
            )

        # Apply the configured partition keys and sort order, if they changed
        if layout is not None:
            self.apply_table_layout(target_schema_name, table_name, layout)

        return self.get_table_column_types(target_schema_name, table_name)

    def get_table_layout(self, target_schema_name: str, table_name: str) -> TableLayout:
        """Read the table's current partition keys and sort order from the catalog.

        DuckDB has no function that reports them, so they are read from
        DuckLake's metadata tables. The metadata schema is looked up rather
        than assumed, since it depends on the catalog type and META_SCHEMA.
        """
        metadata = f"__ducklake_metadata_{self.catalog_name}"
        found = self.execute(
            "SELECT schema_name FROM duckdb_tables() "  # noqa: S608 (identifiers only)
            f"WHERE database_name = {_sql_literal(metadata)} "
            "AND table_name = 'ducklake_partition_column'"
        ).fetchone()
        if found is None:
            msg = f"DuckLake metadata tables not found in {metadata}"
            raise DuckLakeConnectorError(msg)
        m = f'"{metadata}"."{found[0]}"'
        current_table = (
            f"JOIN {m}.ducklake_table tb ON tb.table_id = info.table_id "
            "AND tb.end_snapshot IS NULL "
            f"JOIN {m}.ducklake_schema sc ON sc.schema_id = tb.schema_id "
            "AND sc.end_snapshot IS NULL "
            "WHERE info.end_snapshot IS NULL "
            f"AND sc.schema_name = {_sql_literal(target_schema_name)} "
            f"AND tb.table_name = {_sql_literal(table_name)}"
        )
        partition_rows = self.execute(
            "SELECT col.column_name, pc.transform "  # noqa: S608 (identifiers only)
            f"FROM {m}.ducklake_partition_info info "
            f"JOIN {m}.ducklake_partition_column pc USING (partition_id, table_id) "
            f"JOIN {m}.ducklake_column col ON col.table_id = pc.table_id "
            "AND col.column_id = pc.column_id AND col.end_snapshot IS NULL "
            f"{current_table} ORDER BY pc.partition_key_index"
        ).fetchall()
        sort_rows = self.execute(
            "SELECT se.expression, se.sort_direction, se.null_order "  # noqa: S608
            f"FROM {m}.ducklake_sort_info info "
            f"JOIN {m}.ducklake_sort_expression se USING (sort_id, table_id) "
            f"{current_table} ORDER BY se.sort_key_index"
        ).fetchall()
        return TableLayout(
            partition=tuple(PartitionKey(*row) for row in partition_rows),
            sort=tuple(
                SortKey(_unquoted(expression), direction, nulls)
                for expression, direction, nulls in sort_rows
            ),
        )

    def apply_table_layout(
        self, target_schema_name: str, table_name: str, layout: TableLayout
    ) -> None:
        """Set the configured partition keys and sort order, if they differ.

        Only a configured part is applied (``None`` leaves the table's own
        layout alone; an empty list removes its keys), and only when it
        differs from the table's current layout: an unchanged
        ``SET PARTITIONED BY`` still adds a snapshot, and an unchanged
        ``SET SORTED BY`` a new sort version too, every run. DuckLake applies
        a new layout to files written after it.
        """
        if layout.partition is None and layout.sort is None:
            return
        current = self.get_table_layout(target_schema_name, table_name)
        table_ref = f"{self.catalog_name}.{target_schema_name}.{table_name}"
        for kind, wanted, have in (
            ("PARTITIONED", layout.partition, current.partition),
            ("SORTED", layout.sort, current.sort),
        ):
            if wanted is None or _same_keys(wanted, have):
                continue
            if not wanted:  # an empty list removes the keys
                logger.info("Resetting the %s keys of table %s", kind, table_name)
                self.execute(f"ALTER TABLE {table_ref} RESET {kind} BY;")
                continue
            keys = ", ".join(key.sql() for key in wanted)
            logger.info("Setting table %s %s BY %s", table_name, kind, keys)
            self.execute(f"ALTER TABLE {table_ref} SET {kind} BY ({keys});")

    def _build_columns_sql(
        self,
        file_columns: list[str],
        target_table_columns: list[str],
    ) -> str:
        """Build SQL column list for SELECT statement used for INSERT.
        Orders columns by which they appear in the target table.
        If a column is not found in the file, it is replaced with NULL.
        Names match case-insensitively, as DuckDB identifiers do.
        """
        file_names = {col.lower() for col in file_columns}
        columns_sql = ""
        for col in target_table_columns:
            if col.lower() not in file_names:
                logger.warning(f"Column {col} not found in source, using NULL")
                columns_sql += "NULL, "
            else:
                columns_sql += f' "{col}", '
        # Remove trailing comma and space
        return columns_sql[:-2]

    def insert_into_table(
        self,
        file_location: str,
        target_schema_name: str,
        table_name: str,
        file_columns: list[str],
        target_table_columns: list[str],
    ):
        """Insert data from a parquet file into the table.
        Orders columns by which they appear in the target table.
        If a column is not found in the file, it is inserted as NULL.
        """
        columns_sql = self._build_columns_sql(file_columns, target_table_columns)
        table_ref = f"{self.catalog_name}.{target_schema_name}.{table_name}"
        insert_sql = f"""
        BEGIN TRANSACTION;
        INSERT INTO {table_ref} SELECT {columns_sql} FROM '{file_location}';
        COMMIT;
        """
        logger.info(f"Inserting into table {table_name} with SQL {insert_sql}")
        self.execute(insert_sql)

    def replace_table_contents(
        self,
        file_locations: list[str],
        target_schema_name: str,
        table_name: str,
        file_columns: list[str],
        target_table_columns: list[str],
    ) -> None:
        """Replace every row of the table with the rows of the parquet files.

        The DELETE and the INSERT run in one transaction, so readers see the
        old rows until the new ones are committed, and a failure leaves the
        old rows in place. With no files, the table is emptied.
        """
        table_ref = f"{self.catalog_name}.{target_schema_name}.{table_name}"
        statements = [f"DELETE FROM {table_ref};"]  # noqa: S608 (identifiers only)
        if file_locations:
            columns_sql = self._build_columns_sql(file_columns, target_table_columns)
            files_sql = ", ".join(f"'{location}'" for location in file_locations)
            statements.append(
                # Identifiers from the config and local staging paths; no data.
                f"INSERT INTO {table_ref} "  # noqa: S608
                f"SELECT {columns_sql} FROM read_parquet([{files_sql}]);"
            )
        replace_sql = "\n".join(["BEGIN TRANSACTION;", *statements, "COMMIT;"])
        logger.info(
            "Replacing the contents of table %s with %d staged file(s)",
            table_name,
            len(file_locations),
        )
        self.execute(replace_sql)

    def delete_other_versions(
        self, target_schema_name: str, table_name: str, version: int
    ) -> None:
        """Delete the rows whose table version is not the given one, in one transaction.

        Rows without a version (loaded before versions were recorded) are
        deleted too: after a full refresh, the table holds that version only.
        """
        table_ref = f"{self.catalog_name}.{target_schema_name}.{table_name}"
        delete_sql = (
            "BEGIN TRANSACTION;\n"
            f'DELETE FROM {table_ref} WHERE "{VERSION_COLUMN}" '
            f"IS DISTINCT FROM {int(version)};\n"
            "COMMIT;"
        )
        logger.info("Deleting rows of %s not in version %s", table_name, version)
        self.execute(delete_sql)

    def mark_other_versions_deleted(
        self,
        target_schema_name: str,
        table_name: str,
        version: int,
        *,
        deleted_at_is_timestamptz: bool = False,
    ) -> None:
        """Soft-delete the rows whose table version is not the given one.

        Sets their ``_sdc_deleted_at`` to the current time in one transaction,
        keeping the time of rows already marked deleted. Rows without a
        version are marked too, as in ``delete_other_versions``. A TIMESTAMP
        column gets the UTC wall-clock time; a TIMESTAMPTZ column gets the
        instant itself, since the UTC wall-clock time would be read in the
        session time zone.
        """
        table_ref = f"{self.catalog_name}.{target_schema_name}.{table_name}"
        now = "now()" if deleted_at_is_timestamptz else "now() AT TIME ZONE 'UTC'"
        update_sql = (
            "BEGIN TRANSACTION;\n"
            f'UPDATE {table_ref} SET "{DELETED_AT_COLUMN}" = {now} '
            f'WHERE "{VERSION_COLUMN}" IS DISTINCT FROM {int(version)} '
            f'AND "{DELETED_AT_COLUMN}" IS NULL;\n'
            "COMMIT;"
        )
        logger.info(
            "Marking rows of %s not in version %s as deleted", table_name, version
        )
        self.execute(update_sql)

    @staticmethod
    def _build_key_match_sql(
        key_properties: list[str], key_column_types: dict[str, str]
    ) -> tuple[str, str]:
        """Build the SQL that matches merge keys between the table and the batch.

        Returns the staged key columns to select (each cast to the table's
        type) and the join condition, used as::

            DELETE FROM t AS "target"
            USING (SELECT CAST("id" AS BIGINT) AS "id" FROM 'batch.parquet') AS "staged"
            WHERE "target"."id" IS NOT DISTINCT FROM "staged"."id"

        Each key column is compared on its own, so composite keys can't collide.
        Concatenating them (``tenant || id``) made ('1', '23') match ('12', '3').

        Keys compare with IS NOT DISTINCT FROM, so a NULL key value matches
        NULL. The in-batch dedup already treats NULL keys as equal; with ``=``
        or ``IN`` a NULL-key row was never deleted and every re-merge appended
        another copy.

        Each staged key is cast to the table column's actual type, the same cast
        INSERT applies, so a staged value compares equal to the row it produced:
        - Staged strings can't be compared with a TIMESTAMP, DATE, TIME or UUID
          column in an IN clause at all (binder error).
        - A REAL column holds 0.1 as 0.10000000149; uncast, the staged float64
          0.1 never matches it.
        - Casting to the stream's declared type instead fails for existing
          tables whose column type differs, e.g. a TIMESTAMP column for a
          stream that now declares timestamptz.
        A key with no known type is compared uncast.
        """
        staged_columns, conditions = [], []
        for key_name in key_properties:
            column = f'"{key_name}"'
            key_type = key_column_types.get(key_name)
            staged = f"CAST({column} AS {key_type})" if key_type else column
            staged_columns.append(f"{staged} AS {column}")
            conditions.append(
                f'"target".{column} IS NOT DISTINCT FROM "staged".{column}'
            )
        return ", ".join(staged_columns), " AND ".join(conditions)

    def merge_into_table(
        self,
        file_location: str,
        target_schema_name: str,
        table_name: str,
        file_columns: list[str],
        target_table_columns: list[str],
        key_properties: list[str],
        key_column_types: dict[str, str] | None = None,
    ):
        """Merge data from a parquet file into the table using DELETE+INSERT.

        We first delete rows in the target table that match the source (based on key_properties),
        then insert all rows from the source file.

        ``key_column_types`` maps each key column to the table's actual column
        type (as read from the table), so staged key values can be cast to it
        before comparison.

        Note: MERGE INTO is not used due to issues with partitioned tables.
        See: https://github.com/duckdb/ducklake/issues/643
        """
        columns_sql = self._build_columns_sql(file_columns, target_table_columns)
        staged_keys_sql, key_match_sql = self._build_key_match_sql(
            key_properties, key_column_types or {}
        )
        if len(key_properties) > 1:
            logger.info(
                f"Multiple key properties detected: {key_properties}, "
                "merging on composite key"
            )

        # Build the atomic merge operation
        table_ref = f"{self.catalog_name}.{target_schema_name}.{table_name}"
        combined_sql = f"""
        BEGIN TRANSACTION;
        DELETE FROM {table_ref} AS "target"
        USING (SELECT {staged_keys_sql} FROM '{file_location}') AS "staged"
        WHERE {key_match_sql};
        INSERT INTO {table_ref} 
        SELECT {columns_sql} FROM '{file_location}';
        COMMIT;
        """

        logger.info(f"Executing atomic merge operation on table {table_name}")
        logger.info(f"Merge SQL: {combined_sql}")

        self.execute(combined_sql)

    def json_to_ducklake_schema(
        self, schema: dict, date_time_type: str = "timestamptz"
    ) -> list[dict[str, str]]:
        """Convert a JSON schema to an array of dictionaries with column name and type.
        Example Output: [{'name': 'id', 'type': 'INTEGER'}, {'name': 'name', 'type': 'STRING'}]

        ``date_time_type`` is the type of unhinted date-time properties (see
        ``JSONSchemaToDuckLake``).
        """
        ducklake_schema = []
        converter = JSONSchemaToDuckLake(date_time_type=date_time_type)
        for prop_name, prop_schema in schema.items():
            if is_unsized_singer_decimal(prop_schema):
                logger.warning(
                    "Property %r is an x-singer.decimal string without a usable "
                    "precision and scale (precision=%r, scale=%r); loading it as "
                    "VARCHAR so every digit is kept. Declare both as integers to "
                    "load it as DECIMAL.",
                    prop_name,
                    prop_schema.get("precision"),
                    prop_schema.get("scale"),
                )
            if is_binary_hint(prop_schema) and not declares_base64_binary(prop_schema):
                logger.warning(
                    "Property %r declares x-sql-datatype %r but no contentEncoding, "
                    "so how its text stands for bytes is unknown; keeping its text "
                    "as sent. Add contentEncoding: base64 to load it as BLOB.",
                    prop_name,
                    prop_schema.get("x-sql-datatype"),
                )
            ducklake_schema.append(
                {"name": prop_name, "type": str(converter.to_sql_type(prop_schema))}
            )
        return ducklake_schema

    def close(self) -> None:
        """Close the database connection."""
        if self._connection is not None:
            logger.info("Closing DuckDB connection")
            self._connection.close()
            self._connection = None

    def __enter__(self) -> DuckLakeConnector:
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()
