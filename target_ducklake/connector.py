"""DuckLake connector for managing database connections."""

from __future__ import annotations

import logging
import math
import time
from typing import Any

import duckdb
from singer_sdk.connectors import SQLConnector
from singer_sdk.connectors.sql import JSONSchemaToSQL
from sqlalchemy.types import BIGINT, DECIMAL, INTEGER, JSON, TypeEngine, UserDefinedType

from target_ducklake.parquet_utils import number_precision_and_scale
from target_ducklake.type_hints import decimal_hint, hinted_type

logger = logging.getLogger(__name__)

PARTITION_GRANULARITIES = ["year", "month", "day", "hour"]

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


class JSONSchemaToDuckLake(JSONSchemaToSQL):
    """Convert JSON Schema types to DuckLake compatible types."""

    def to_sql_type(self, schema: dict) -> TypeEngine:
        """Convert a property schema, honouring its ``x-sql-datatype`` hint first.

        A supported hint (see ``type_hints``) gives the source column's own type;
        an unsupported one falls back to the JSON Schema type and format, with
        the SDK's warning.
        """
        if decimal_hint(schema) is not None:
            return DECIMAL(*number_precision_and_scale(schema))
        if (type_name := hinted_type(schema)) is not None:
            return DuckDBType(type_name)
        return super().to_sql_type(schema)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Override the default type handler for object and array types
        self.register_type_handler("object", JSON)
        self.register_type_handler("array", JSON)
        # Override the default type handler for integer type
        self.register_type_handler("integer", self._handle_integer_type)
        # Override number handling to use high-precision decimals by default
        self.register_type_handler("number", self._handle_number_type)

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
        partition_fields: list[dict[str, str]] | None = None,
        overwrite_table: bool = False,
    ) -> list[str]:
        """Prepare the table for the target schema.
        If the table doesn't exist, create it.
        Add missing columns to existing table if necessary.
        Returns all columns in the table and order in which they appear.
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

        # truncate the table if necessary
        if overwrite_table:
            logger.info(f"Truncating table {table_name} in schema {target_schema_name}")
            self.execute(
                f"TRUNCATE TABLE {self.catalog_name}.{target_schema_name}.{table_name};"
            )

        # Add partition fields if any
        if partition_fields:
            logger.info(
                f"Adding partition fields to table {table_name}: {partition_fields}"
            )
            self._add_partition_fields(target_schema_name, table_name, partition_fields)

        return self.get_table_columns(target_schema_name, table_name)

    def _add_partition_fields(
        self,
        target_schema_name: str,
        table_name: str,
        partition_fields: list[dict[str, str]],
    ) -> None:
        """Add partition fields to the table."""
        partitions_sql = ""
        for partition_field in partition_fields:
            column_type = partition_field["type"]
            if column_type == "identifier":
                partitions_sql += f"{partition_field['column_name']}, "
            elif column_type == "timestamp":
                for granularity in partition_field["granularity"]:
                    if granularity not in PARTITION_GRANULARITIES:
                        raise DuckLakeConnectorError(
                            f"Unsupported granularity: {granularity}"
                        )
                    partitions_sql += (
                        f"{granularity}({partition_field['column_name']}), "
                    )
            else:
                raise DuckLakeConnectorError(
                    f"Unsupported partition type: {column_type}"
                )
        partitions_sql = partitions_sql[:-2]
        add_partition_query = f"ALTER TABLE {self.catalog_name}.{target_schema_name}.{table_name} SET PARTITIONED BY ({partitions_sql});"
        logger.info(
            f"Adding partition fields to table {table_name} with query {add_partition_query}"
        )
        self.execute(add_partition_query)

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

    def json_to_ducklake_schema(self, schema: dict) -> list[dict[str, str]]:
        """Convert a JSON schema to an array of dictionaries with column name and type.
        Example Output: [{'name': 'id', 'type': 'INTEGER'}, {'name': 'name', 'type': 'STRING'}]
        """
        ducklake_schema = []
        converter = JSONSchemaToDuckLake()
        for prop_name, prop_schema in schema.items():
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
