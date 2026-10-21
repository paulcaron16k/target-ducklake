# target-ducklake

`target-ducklake` is a Singer target for ducklake. This target is in development and may not be stable. I built this with GCS in mind, but it should work with S3 and local storage, though this has not been tested extensively.

Supports append, merge, and overwrite load methods (default is merge). The load method can be configured using the `load_method` setting. If no key properties are provided and `overwrite_if_no_pk` is true, data will be overwritten. Otherwise, the configured load method determines the behavior.

## TODOs

- support deleting data from the target or setting _sdc_deleted_at field (example use-case: syncing from Postgres using log-based change data capture)

## Configuration

### Accepted Config Options

| Setting | Type | Required | Default | Description |
|---------|------|----------|---------|-------------|
| `catalog_url` | string | ✅ | - | URL connection string to your catalog database |
| `data_path` | string | ✅ | - | GCS, S3, or local folder path for data storage |
| `storage_type` | string | ✅ | `"local"` | Type of storage: GCS, S3, or local |
| `catalog_type` | string | ✅ | `"postgres"` | Type of catalog database: postgres, sqlite, mysql, or duckdb. See <https://ducklake.select/docs/stable/duckdb/usage/choosing_a_catalog_database> |
| `meta_schema` | string | ❌ | - | Schema name in the catalog database to use for Ducklake metadata tables. If not provided will use the default schema for the catalog database (eg `public` for Postgres) |
| `public_key` | string | ❌ | - | Public key for private GCS and S3 storage authentication (optional) |
| `secret_key` | string | ❌ | - | Secret key for private GCS and S3 storage authentication (optional) |
| `region` | string | ❌ | - | AWS region for S3 storage type (required when using S3 with explicit credentials) |
| `default_target_schema` | string | ❌ | - | Default database schema where data should be written. If not provided schema will attempt to be inferred from the stream name (inferring schema only works for database extractors ) |
| `target_schema_prefix` | string | ❌ | - | Prefix to add to the target schema name. If not provided, no prefix will be added |
| `add_record_metadata` | boolean | ❌ | `false` | When True, automatically adds Singer Data Capture (SDC) metadata columns to target tables |
| `load_method` | string | ❌ | `"merge"` | Method to use for loading data into the target table: append, merge, or overwrite. See [Overwrite and ACTIVATE_VERSION](#overwrite-and-activate_version). |
| `flatten_max_level` | integer | ❌ | `0` | Maximum depth for flattening nested fields. Set to 0 to disable flattening |
| `temp_file_dir` | string | ❌ | `"temp_files/"` | Directory path for storing temporary parquet files |
| `max_batch_size` | integer | ❌ | `10000` | Maximum number of records to process in a single batch |
| `partition_fields` | object | ❌ | - | Object mapping stream names to arrays of partition column definitions. Each stream key maps directly to an array of column definitions |
| `auto_cast_timestamps` | boolean | ❌ | `false` | When True, automatically attempts to cast timestamp-like fields to timestamp types in ducklake |
| `convert_tz_to_utc` | boolean | ❌ | `false` | When True, converts timezone-aware date-time values to UTC before loading, so a timestamp column without time zone stores the UTC time. When False, such a column stores each value's local wall-clock time (its offset is dropped). With the default, a timestamp column fed values in different offsets holds local times that aren't directly comparable; turn this on, or declare the column `timestamptz` (via `x-sql-datatype`), to compare instants. Either way, a `timestamptz` column stores the correct instant, and values with any offset, such as `+05:30`, load. Values without a time zone are loaded as given; a `timestamptz` column reads them in the DuckDB session's time zone. |
| `validate_records` | boolean | ❌ | `false` | Whether to validate the schema of the incoming streams |
| `overwrite_if_no_pk` | boolean | ❌ | `false` | When True, a stream without primary keys replaces the target table's rows, as `load_method: overwrite` does. Overrides load_method. |
| `max_column_length` | integer | ❌ | `63` | Maximum length for column names. Only applied when `catalog_type` is `postgres`. Names exceeding this limit are truncated. Colliding truncated names get a `_{i}` suffix. Set to `0` to disable. See [ducklake#619](https://github.com/duckdb/ducklake/issues/619). |
| `parallel_draining` | boolean | ❌ | `true` | When True, enables parallel draining with `max_parallelism=10` (Meltano default). When False, sets `max_parallelism=1` to disable parallel draining. Ignored when `catalog_type` is `sqlite`: SQLite allows one writer at a time, so its sinks are always drained one at a time (a log line says so). |
| `max_record_age_minutes` | number | ❌ | `10.0` | Maximum age in minutes that records can sit in a sink before a drain is forced. Tune lower for more frequent flushes, higher for larger batches. |

### Example Meltano YAML Configuration

```yaml
plugins:
    # ... other plugins ...
    loaders:
    - name: target-ducklake
      namespace: target_ducklake
      pip_url: https://github.com/definite-app/target-ducklake.git
      config:
        catalog_url: postgres://test:test@localhost:5432/test_db
        data_path: gs://test-bucket/test-path
        storage_type: GCS # Optional (default local)
        public_key: GOOG1234567890 # Optional (required for private GCS and S3 storage)
        secret_key: GOOG1234567890 # Optional (required for private GCS and S3 storage)
        default_target_schema: my_schema # Optional (default None)
        target_schema_prefix: my_prefix # Optional
        max_batch_size: 10000 # Optional (default 10000)
        add_record_metadata: true # Optional
        load_method: merge # Optional (default merge, options: append, merge, overwrite)
        auto_cast_timestamps: true # Optional
        overwrite_if_no_pk: true # Optional (default false)
        partition_fields: {"my_stream": [{"column_name": "created_at", "type": "timestamp", "granularity": ["year", "month"]}]} # Optional
```

### Partitioning

Partitioning is supported by providing a `partition_fields` configuration object. Each stream name maps to an array of column definitions.

Example:

```json
{
    "stream_name": [    
        {
            "column_name": "created_at", # The column name to partition on
            "type": "timestamp", # The partition type (timestamp or identifier)
            "granularity": ["year", "month"] # The granularity of the partition (only for timestamp type)
        }
    ],
    "stream_name_2": [
        {
            "column_name": "status", # The column name to partition on
            "type": "identifier", # The partition type (timestamp or identifier)
        }
    ]
}
```

A full list of supported settings and capabilities for this
target is available by running:

```bash
target-ducklake --about
```

### Column types

Each property's JSON Schema type and format give its column type: `integer` is BIGINT (INTEGER with 32-bit bounds), `number` is DECIMAL(38, 9) (or its `precision`/`scale`), `boolean` is BOOLEAN, `object` and `array` are JSON, and `string` is VARCHAR, or DATE, TIME, TIMESTAMP or UUID by its `format`. A property declaring several non-null types, such as `["integer", "string"]`, is VARCHAR, and each value is stored as its text (`5`, `1.50`, `true`). Values are staged at their column's own type, so any value a column can hold loads, and one it can't hold fails the batch instead of being changed.

An `x-sql-datatype` annotation (as database taps emit, e.g. `smallint`, `numeric(12,4)`, `timestamptz`, `uuid`, `jsonb`) sets the column type instead, when the property's values can be loaded into it:

- a `string` property takes any supported hint, so `"12.5"` loads into a `numeric(12,4)` column;
- an `integer`, `number` or `boolean` property takes numeric hints and `json`/`jsonb`;
- an `object` or `array` property takes only `json`/`jsonb`.

Any other hint, such as `uuid` on an integer, is ignored with a warning, and the JSON type decides the column. Unsupported hints are ignored with the SDK's warning.

Supported hints (case and spacing ignored):

| Hint | Column type |
| --- | --- |
| `smallint`, `int2` / `integer`, `int`, `int4`, `mediumint` / `bigint`, `int8` | SMALLINT / INTEGER / BIGINT |
| MySQL `int(11)`, `bigint(20)` (display width ignored) | as without the width |
| `smallint unsigned`, `mediumint unsigned` / `int unsigned` / `bigint unsigned` | INTEGER / BIGINT / DECIMAL(20, 0) |
| `real`, `float4`, `float(p≤24)` / `float`, `float8`, `double`, `double precision`, `float(p>24)` (MySQL's `unsigned` ignored) | REAL / DOUBLE |
| `decimal(p,s)`, `numeric(p,s)`; `decimal(p)` (MySQL's `unsigned` ignored) | DECIMAL(p, s), capped at 38 digits; DECIMAL(p, 0) |
| `decimal`, `numeric` (no size) | DECIMAL(38, 9), or the property's `precision`/`scale` (so at most 29 integer digits) |
| `timestamp`, `timestamp without time zone`, `timestamp_ntz`, `datetime2`, `smalldatetime` | TIMESTAMP |
| `timestamptz`, `timestamp with time zone`, `timestamp_tz`, `timestamp_ltz`, `datetimeoffset` | TIMESTAMPTZ |
| `date` / `time`, `time without time zone` | DATE / TIME |
| any of the timestamp and time hints with a precision, e.g. `timestamp(3) with time zone` | as without it: microseconds are kept, so `datetime2(7)`'s 100 ns digit is dropped |
| `uuid`, `uniqueidentifier` / `json`, `jsonb` | UUID / JSON |

Hints naming the type a property gets anyway are accepted without a warning: a text type (`text`, `varchar(n)`, `varchar(max)`, `char(n)`, `nvarchar(n)`, `citext`) on a `string` property, and `boolean`/`bool` on a `boolean` property. On another JSON type they warn as unsupported. Deliberately unsupported, so they keep the JSON type's column: `tinyint` (MySQL uses `tinyint(1)` for booleans), `datetime` (MySQL's meaning is the opposite of ISO's), `timetz` and `time with time zone` (DuckLake's Parquet files drop the offset without converting it), `bit` and binary types.

Existing tables keep their column types, apart from safe integer and float widenings. Values are staged at an existing column's own type where the stream's type would change them: a number column into an existing DECIMAL, REAL or DOUBLE column, a whole-number decimal (such as `bigint unsigned`) into an existing integer column, and a string property into an existing text column, whose text is kept as sent whatever its hint. So a `double` hint on a column that already exists as DECIMAL(38, 9) still stores DECIMAL(38, 9) values: `1.5e-12` is stored as 0, and a warning reports how many values were rounded. Recreate the column to store doubles.

### Overwrite and ACTIVATE_VERSION

**Overwrite** (`load_method: overwrite`, or `overwrite_if_no_pk` for a stream without primary keys) replaces the table's rows atomically. Each batch is staged as a Parquet file in `temp_file_dir`, and at the end of the stream one transaction deletes the old rows and inserts every staged batch. Readers see the old table until that commit, and a run that fails or is interrupted leaves the old rows in place. STATE is emitted only after the commit. Two consequences:
- `temp_file_dir` needs room for the whole stream, not one batch.
- A stream with no records still empties the table, as before.

**ACTIVATE_VERSION**, as full-table taps send it, removes rows that are gone at the source. The tap tags each record with a table version, then sends `ACTIVATE_VERSION` for that version. The target first loads any buffered records, then deletes every row whose `_sdc_table_version` is not the activated version, including rows loaded without a version. Rows are hard-deleted. Some cases remove nothing:
- **Without `add_record_metadata`:** records carry no version, so the message is ignored with a warning.
- **A version announced before any of its records** (as taps do at the start of a sync) is a no-op, so the table never empties mid-sync.
- **An overwrite stream** ignores versions, since it replaces the whole table anyway.

Between a sync's first batch and its `ACTIVATE_VERSION`, readers see the new rows next to rows of the old version. Set `process_activate_version_messages: false` to ignore the message entirely.

### Ducklake Schema Migration From 0.1 to 0.2
Solution to this issue: https://github.com/duckdb/ducklake/issues/240
The SQL to manually migrate your catalog schema (assuming Postgres) can be found in the `schema_migration.sql` file.

### Configure using environment variables

This Singer target will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Authentication and Authorization

<!--
Developer TODO: If your target requires special access on the destination system, or any special authentication requirements, provide those here.
-->

## Usage

You can easily run `target-ducklake` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Target Directly

```bash
target-ducklake --version
target-ducklake --help
# Test using the "Smoke Test" tap:
tap-smoke-test | target-ducklake --config /path/to/target-ducklake-config.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

Prerequisites:

- Python 3.9+
- [uv](https://docs.astral.sh/uv/)

```bash
uv sync
```

### Create and Run Tests

Create tests within the `tests` subfolder and
then run:

```bash
# Run all unit tests (no database required)
uv run pytest tests/ -v
```

You can also test the `target-ducklake` CLI interface directly using `uv run`:

```bash
uv run target-ducklake --help
```

### Testing with [Meltano](https://meltano.com/)

_**Note:** This target will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd target-ducklake
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke target-ducklake --version

# OR run a test ELT pipeline with the Smoke Test sample tap:
meltano run tap-smoke-test target-ducklake
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the Meltano Singer SDK to
develop your own Singer taps and targets.
