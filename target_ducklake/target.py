"""ducklake target class."""

from __future__ import annotations

import ast
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

from singer_sdk import typing as th
from singer_sdk.target_base import SQLTarget

from target_ducklake.sinks import (
    ducklakeSink,
)

if TYPE_CHECKING:
    from collections.abc import Iterable

    from singer_sdk.sinks import Sink


# One stream's table layout (target-iceberg's shape). Values are checked by
# layout.resolve_layout, which names the fix; the schema stays permissive so a
# truncate transform or an unknown key gets that message, not a schema error.
_LAYOUT_SCHEMA: dict = {
    "type": "object",
    "properties": {
        "partition": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "column": {"type": "string"},
                    "transform": {"type": "string"},
                    "num_buckets": {"type": "integer"},
                    "width": {"type": "integer"},
                    "name": {"type": "string"},
                },
                "required": ["column"],
            },
        },
        "sort_order": {"type": "array", "items": {"type": "string"}},
    },
}


def _parse_layout_settings(config: dict) -> None:
    """Parse table layout settings given as JSON strings (env vars), in place."""
    for key in ("stream_defaults", "stream_overrides"):
        if isinstance(config.get(key), str):
            config[key] = json.loads(config[key])


class Targetducklake(SQLTarget):
    name = "target-ducklake"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.max_parallelism = 10 if self._drains_in_parallel() else 1
        self._MAX_RECORD_AGE_IN_MINUTES = float(
            self.config.get("max_record_age_minutes", 10.0)
        )

    def _drains_in_parallel(self) -> bool:
        """Whether sinks are drained in parallel: parallel_draining, except on SQLite.

        A SQLite catalog allows one writer at a time, so two sinks committing
        at once fail with 'database is locked'. Its sinks are always drained
        one at a time.
        """
        parallel = bool(self.config.get("parallel_draining", True))
        if parallel and self.config.get("catalog_type") == "sqlite":
            self.logger.info(
                "catalog_type is sqlite, which allows one writer at a time: "
                "sinks are drained one at a time (parallel_draining is ignored)."
            )
            return False
        return parallel

    def _drain_all(self, sink_list: Iterable[Sink], parallelism: int) -> None:
        """Drain sinks in parallel threads, and wait for all of them before raising.

        The SDK's parallel drain raises the first failure without waiting for
        the other drains, which keep running in background threads. The
        interpreter then shuts down under them: a thread that returns from
        native code (here Polars' ``from_arrow``) during shutdown is ended by
        CPython with ``pthread_exit``, which can't unwind through Rust or C++
        frames, so the process aborted (exit 134) instead of failing with the
        error. Here every drain finishes first; each failure is logged and the
        first one is raised, so the run exits with it. Sinks that drain
        successfully commit their batch, as on the serial path. Nothing
        changes when drains succeed.
        """
        if parallelism == 1:
            super()._drain_all(sink_list, parallelism)
            return
        with ThreadPoolExecutor(max_workers=parallelism) as pool:
            futures = [pool.submit(self.drain_one, sink) for sink in sink_list]
        # Leaving the block waited for every drain.
        errors = [error for future in futures if (error := future.exception())]
        for error in errors[1:]:
            self.logger.error("Another sink also failed to drain: %s", error)
        if errors:
            raise errors[0]

    def _write_state_message(self, state: dict) -> None:
        """Emit STATE, unless an overwrite hasn't committed its replace yet.

        A mid-stream drain (a full batch, or max_record_age_minutes) only
        stages an overwrite's batches; its rows reach the table when the
        stream ends. STATE emitted before that would let a resumable tap skip
        rows that were never committed if the run then fails. So STATE is held
        until every overwrite has committed. The SDK writes the latest STATE
        again after the final drain and clean-up.
        """
        sinks = [*self._sinks_to_clear, *self._sinks_active.values()]
        pending = [
            sink.stream_name
            for sink in sinks
            if getattr(sink, "has_uncommitted_overwrite", False)
        ]
        if pending:
            self.logger.info(
                "Holding STATE until the overwrite of %s commits.", ", ".join(pending)
            )
            return
        super()._write_state_message(state)

    def _process_activate_version_message(self, message_dict: dict) -> None:
        """Drain the stream's sink, then let it activate the version.

        Draining first writes any buffered records of the version, so rows
        the sink is about to keep are in the table before rows of other
        versions are deleted. The SDK neither drains nor checks whether the
        version can be honoured; the sink decides (``activate_version``).
        """
        stream_name = message_dict["stream"]
        for stream_map in self.mapper.stream_maps[stream_name]:
            sink = self._sinks_active.get(stream_map.stream_alias)
            if sink is None:
                self.logger.info(
                    "ACTIVATE_VERSION for %s before its SCHEMA; ignored.", stream_name
                )
                continue
            if not sink.process_activate_version_messages:
                self.logger.info(
                    "process_activate_version_messages is off; ignoring "
                    "ACTIVATE_VERSION for %s.",
                    stream_name,
                )
                continue
            self.drain_one(sink)
            sink.activate_version(message_dict["version"])

    @property
    def config(self):
        """Get config dict with JSON string parsing for complex types."""
        config = dict(super().config)

        # Convert certain config values from strings to their respective types
        # This is required for certain config values that are passed in via env vars into Kubernetes pods
        # Kubernetes pods only support string values for env vars (at least in GCP)
        if "partition_fields" in config and isinstance(config["partition_fields"], str):
            partition_fields_str = config["partition_fields"]
            parsed_partition_fields = ast.literal_eval(partition_fields_str)
            config["partition_fields"] = parsed_partition_fields
            logging.info(
                f"Successfully parsed partition_fields from string to object: {parsed_partition_fields}"
            )
        _parse_layout_settings(config)
        if "max_batch_size" in config:
            logging.warning(f"max_batch_size: {config.get('max_batch_size')}")
            config["max_batch_size"] = int(config.get("max_batch_size", 10000))
        if "max_record_age_minutes" in config:
            config["max_record_age_minutes"] = float(config["max_record_age_minutes"])
        if "flatten_max_level" in config:
            config["flatten_max_level"] = int(config.get("flatten_max_level", 0))
        if "max_column_length" in config:
            config["max_column_length"] = int(config.get("max_column_length", 63))
        if "validate_records" in config and isinstance(config["validate_records"], str):
            config["validate_records"] = config["validate_records"].lower() == "true"
        if "overwrite_if_no_pk" in config and isinstance(
            config["overwrite_if_no_pk"], str
        ):
            config["overwrite_if_no_pk"] = (
                config["overwrite_if_no_pk"].lower() == "true"
            )
        if "parallel_draining" in config and isinstance(
            config["parallel_draining"], str
        ):
            config["parallel_draining"] = (
                config["parallel_draining"].lower() == "true"
            )
        if "auto_cast_timestamps" in config and isinstance(
            config["auto_cast_timestamps"], str
        ):
            config["auto_cast_timestamps"] = (
                config["auto_cast_timestamps"].lower() == "true"
            )
        return config

    config_jsonschema = th.PropertiesList(
        th.Property(
            "catalog_url",
            th.StringType(nullable=False),
            secret=True,  # Flag config as protected.
            title="Catalog URL",
            description="URL connection string to your catalog database",
        ),
        th.Property(
            "catalog_type",
            th.StringType(
                allowed_values=["postgres", "sqlite", "mysql", "duckdb"],
                nullable=False,
            ),
            default="postgres",
            title="Catalog Type",
            description="Type of catalog database: postgres, sqlite, mysql, or duckdb",
        ),
            th.Property(
                "meta_schema",
                th.StringType(nullable=True),
                title="Meta Schema",
                description="Schema name in the catalog database to use for Ducklakemetadata tables",
            ),
        th.Property(
            "data_path",
            th.StringType(nullable=False),
            title="Data Path",
            description="GCS, S3, or local folder path for data storage",
        ),
        th.Property(
            "storage_type",
            th.StringType(
                allowed_values=["GCS", "S3", "local"],
                nullable=False,
            ),
            default="local",
            title="Storage Type",
            description="Type of storage: GCS, S3, or local",
        ),
        th.Property(
            "public_key",
            th.StringType(nullable=True),
            title="Public Key",
            description="Public key for private GCS and S3 storage authentication (optional)",
        ),
        th.Property(
            "secret_key",
            th.StringType(nullable=True),
            secret=True,  # Flag config as protected.
            title="Secret Key",
            description="Secret key for private GCS and S3 storage authentication (optional)",
        ),
        th.Property(
            "region",
            th.StringType(nullable=True),
            title="Region",
            description="AWS region for S3 storage type (required when using S3 with explicit credentials)",
        ),
        th.Property(
            "default_target_schema",
            th.StringType(nullable=True),
            title="Default Target Schema Name",
            description=(
                "Default database schema where data should be written. "
                "If not provided schema will attempt to be derived from the stream name "
                "(e.g. database taps provide schema name in the stream name)."
            ),
        ),
        th.Property(
            "target_schema_prefix",
            th.StringType(nullable=True),
            title="Target Schema Prefix",
            description=(
                "Prefix to add to the target schema name. "
                "If not provided, no prefix will be added."
                "May be useful if target schema name is inferred from the stream name "
                "and you want to add a prefix to the schema name."
            ),
        ),
        th.Property(
            "add_record_metadata",
            th.BooleanType(),
            default=False,
            title="Add Singer Record Metadata",
            description=(
                "When True, automatically adds Singer Data Capture (SDC) metadata columns to target tables: "
            ),
        ),
        th.Property(
            "load_method",
            th.StringType(allowed_values=["append", "merge", "overwrite"]),
            title="Load Method",
            description="Method to use for loading data into the target table: append, merge, or overwrite. An overwrite stages every batch and replaces the table's rows in one transaction at the end of the stream.",
        ),
        th.Property(
            "flatten_max_level",
            th.CustomType(
                {"oneOf": [{"type": "string"}, {"type": "integer", "minimum": 0}]}
            ),
            default=0,
            title="Flattening Max Level",
            description="Maximum depth for flattening nested fields. Set to 0 to disable flattening.",
        ),
        th.Property(
            "temp_file_dir",
            th.StringType(),
            default="temp_files/",
            title="Temporary File Directory",
            description="Directory path for storing temporary parquet files",
        ),
        th.Property(
            "max_batch_size",
            th.CustomType(
                {
                    "oneOf": [
                        {
                            "type": "string",
                            "description": "String representation of max batch size",
                        },
                        {"type": "integer", "minimum": 1},
                    ]
                }
            ),
            default=10000,
            title="Max Batch Size",
            description="Maximum number of records to process in a single batch",
        ),
        th.Property(
            "partition_fields",
            th.CustomType(
                {
                    "oneOf": [
                        {
                            "type": "string",
                            "description": "JSON string representation of partition fields object",
                        },
                        {
                            "type": "object",
                            "additionalProperties": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "column_name": {"type": "string"},
                                        "type": {
                                            "type": "string",
                                            "enum": ["timestamp", "identifier"],
                                        },
                                        "granularity": {
                                            "type": "array",
                                            "items": {
                                                "type": "string",
                                                "enum": [
                                                    "year",
                                                    "month",
                                                    "day",
                                                    "hour",
                                                ],
                                            },
                                        },
                                    },
                                    "required": ["column_name", "type"],
                                },
                            },
                        },
                        {"type": "null"},
                    ]
                }
            ),
            nullable=True,
            title="Partition Fields",
            description=(
                "Object mapping stream names to arrays of partition column definitions. "
                "Each stream key maps directly to an array of column definitions. "
                "Can be provided as a JSON string or object."
            ),
        ),
        th.Property(
            "stream_defaults",
            th.CustomType(
                {"oneOf": [{"type": "string"}, _LAYOUT_SCHEMA, {"type": "null"}]}
            ),
            nullable=True,
            title="Stream Defaults",
            description=(
                "Table layout for every stream, as in target-iceberg: partition "
                "([{column, transform, num_buckets}], transforms identity, year, "
                "month, day, hour, bucket) and sort_order (column names, optionally "
                "with ASC/DESC and NULLS FIRST/LAST). Can be a JSON string."
            ),
        ),
        th.Property(
            "stream_overrides",
            th.CustomType(
                {
                    "oneOf": [
                        {"type": "string"},
                        {"type": "object", "additionalProperties": _LAYOUT_SCHEMA},
                        {"type": "null"},
                    ]
                }
            ),
            nullable=True,
            title="Stream Overrides",
            description=(
                "Per-stream table layout, keyed by stream name, merged over "
                "stream_defaults key by key; same shape. partition replaces "
                "partition_fields for that stream. Can be a JSON string."
            ),
        ),
        th.Property(
            "max_column_length",
            th.CustomType(
                {"oneOf": [{"type": "string"}, {"type": "integer", "minimum": 0}]}
            ),
            default=63,
            title="Max Column Name Length",
            description=(
                "Maximum length for column names. Only applied when catalog_type is 'postgres'. "
                "Names exceeding this limit are truncated to avoid PostgreSQL's 63-character "
                "identifier limit (NAMEDATALEN). Colliding truncated names get a _{i} suffix. "
                "Set to 0 to disable truncation."
            ),
        ),
        th.Property(
            "auto_cast_timestamps",
            th.CustomType(
                {
                    "oneOf": [
                        {
                            "type": "string",
                            "description": "String representation of auto cast timestamps flag",
                        },
                        {"type": "boolean"},
                    ]
                }
            ),
            default=False,
            title="Auto Cast Timestamps",
            description=(
                "When True, automatically attempts to cast timestamp-like fields to timestamp types in ducklake."
            ),
        ),
        th.Property(
            "convert_tz_to_utc",
            th.BooleanType(),
            default=False,
            title="Convert TZ to UTC",
            description=(
                "When True, converts timezone-aware date-time values to UTC before "
                "loading, so a timestamp column without time zone stores the UTC "
                "time. When False, such a column stores each value's local "
                "wall-clock time (its offset is dropped). A timestamptz column "
                "stores the correct instant either way."
            ),
        ),
        th.Property(
            "validate_records",
            th.CustomType(
                {
                    "oneOf": [
                        {
                            "type": "string",
                            "description": "String representation of validate records",
                        },
                        {"type": "boolean"},
                    ]
                }
            ),
            default=False,
            title="Validate Records",
            description="Whether to validate the schema of the incoming streams.",
        ),
        th.Property(
            "overwrite_if_no_pk",
            th.CustomType(
                {
                    "oneOf": [
                        {
                            "type": "string",
                            "description": "String representation of overwrite if no primary key, overrides load_method.",
                        },
                        {"type": "boolean"},
                    ]
                }
            ),
            default=False,
            title="Overwrite If No Primary Key",
            description="When True, a stream without primary keys replaces the target table's rows (as load_method overwrite does, in one transaction at the end of the stream). Overrides load_method.",
        ),
        th.Property(
            "parallel_draining",
            th.CustomType(
                {
                    "oneOf": [
                        {
                            "type": "string",
                            "description": "String representation of parallel draining flag",
                        },
                        {"type": "boolean"},
                    ]
                }
            ),
            default=True,
            title="Parallel Draining",
            description=(
                "When True, enables parallel draining with max_parallelism=10 "
                "(Meltano default). When False, sets max_parallelism=1 to "
                "disable parallel draining. Ignored for a sqlite catalog, "
                "which allows one writer at a time: its sinks are always "
                "drained one at a time."
            ),
        ),
        th.Property(
            "max_record_age_minutes",
            th.CustomType(
                {
                    "oneOf": [
                        {
                            "type": "string",
                            "description": "String representation of max record age in minutes",
                        },
                        {"type": "number", "exclusiveMinimum": 0},
                    ]
                }
            ),
            default=10.0,
            title="Max Record Age (Minutes)",
            description=(
                "Maximum age in minutes that records can sit in a sink before a drain "
                "is forced. Larger values batch more aggressively; smaller values flush "
                "more often."
            ),
        ),
    ).to_dict()

    default_sink_class = ducklakeSink


if __name__ == "__main__":
    Targetducklake.cli()
