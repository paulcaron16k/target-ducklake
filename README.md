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
| `hard_delete` | boolean | ❌ | `false` | When True, `ACTIVATE_VERSION` deletes rows absent from the activated version; when False, it sets their `_sdc_deleted_at`. See [Overwrite and ACTIVATE_VERSION](#overwrite-and-activate_version). |
| `process_activate_version_messages` | boolean | ❌ | `true` | When False, `ACTIVATE_VERSION` messages are ignored. |
| `load_method` | string | ❌ | `"merge"` | Method to use for loading data into the target table: append, merge, or overwrite. See [Overwrite and ACTIVATE_VERSION](#overwrite-and-activate_version). |
| `flatten_max_level` | integer | ❌ | `0` | Maximum depth for flattening nested fields. Set to 0 to disable flattening |
| `temp_file_dir` | string | ❌ | `"temp_files/"` | Directory path for storing temporary parquet files |
| `max_batch_size` | integer | ❌ | `10000` | Maximum number of records to process in a single batch |
| `partition_fields` | object | ❌ | - | Object mapping stream names to arrays of partition column definitions. Each stream key maps directly to an array of column definitions |
| `auto_cast_timestamps` | boolean | ❌ | `false` | When True, automatically attempts to cast timestamp-like fields to timestamp types in ducklake |
| `convert_tz_to_utc` | boolean | ❌ | `false` | When True, converts timezone-aware date-time values to UTC before loading, so a timestamp column without time zone stores the UTC time. When False, such a column stores each value's local wall-clock time (its offset is dropped). With the default, a timestamp column fed values in different offsets holds local times that aren't directly comparable; turn this on, or declare the column `timestamptz` (via `x-sql-datatype`), to compare instants. Either way, a `timestamptz` column stores the correct instant, and values with any offset, such as `+05:30`, load. Values without a time zone are loaded as given; a `timestamptz` column reads them in the DuckDB session's time zone. |
| `validate_records` | boolean | ❌ | `false` | Whether to validate the schema of the incoming streams |
| `value_type_checking` | string | ❌ | `permissive` | What to do with a number its column would round: `permissive` rounds it as DuckDB's cast of a DECIMAL does, with a warning; `strict` fails the batch naming the column and the value. See [Values a column would round](#values-a-column-would-round) |
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

### Partitioning and sort order

Each stream's table layout is set with `stream_defaults` (every stream) and `stream_overrides` (keyed by stream name, merged over the defaults key by key). This is the same shape as target-iceberg, so both targets can be configured alike:

```yaml
stream_defaults:
  sort_order: [updated_at]
stream_overrides:
  orders:
    partition:
      - {column: occurred_at, transform: day}
      - {column: tenant_id, transform: bucket, num_buckets: 16}
    sort_order: [customer_id, "occurred_at DESC NULLS LAST"]
```

- **`partition`:** a list of `{column, transform}` entries. Transforms are `identity` (the default), `year`, `month`, `day`, `hour` and `bucket`, in lowercase as in target-iceberg and Zamboni (`Day` is refused); `bucket` requires `num_buckets`.
- **`sort_order`:** column names, each optionally followed by `ASC`/`DESC` and `NULLS FIRST`/`NULLS LAST`. DuckLake writes each new file's rows in this order.
- **Column names** are the table's (flattened names use `__`), matched case-insensitively. An unknown key, transform or column fails the stream with a message naming it, rather than being ignored.
- **When applied:** a new table gets the layout at creation. On an existing table, a changed layout is applied as metadata, and files written afterwards follow it (unlike target-iceberg, where partitioning is fixed at creation). An unchanged layout is left alone. A stream with no `partition` or `sort_order` keeps whatever layout its table already has; an empty list (`partition: []`, `sort_order: []`) removes the table's keys.
- **`table_config`:** the path to a Zamboni `table-config.json`, the file target-iceberg reads. Tables are matched case-insensitively, then by target-iceberg's conformed name (`Orders Daily` → `orders_daily`); a table with no block takes `defaults` (logged), so a `defaults.partition` must name columns every stream has, as in target-iceberg. The file must be version 2 (`namespaces.<ns>.tables.<table>`), the format Zamboni reads; the namespace is the target schema. The version 1 shape (top-level `tables` keyed `namespace.table`) and any other `version` are refused, since Zamboni refuses them too. A table's `partition` sets its partition keys, and an `ordering` with `mode: sort` (`[{column, direction, nulls}]`) sets its sort order (`mode: none`, the default, removes it); an ordering Zamboni would refuse is refused too: a `sort` list or `zorder` block without the matching `mode`, both at once, an empty `sort`, an unknown key in a sort entry, or a direction or nulls value other than lowercase `asc`/`desc` and `first`/`last`. A missing `warehouse` is not checked, since target-ducklake doesn't use it. Maintenance-only keys (`retention`, `partition_evolution`, …) are ignored, and keys target-ducklake doesn't know are logged, not rejected. The format is pinned to Zamboni's published spec: `tests/contracts/` holds a copy from a Zamboni tag, refreshed by `bin/refresh-table-config-contract.sh`, and `tests/test_table_config_contract.py` fails if the reader and the spec disagree on a key, transform, ordering mode or version. A key in the table's block wins over `defaults`, and lists replace whole, as in Zamboni. A stream's own `partition` and `sort_order` win over the file, and the file wins over `partition_fields`.

**Configs shared with target-iceberg.** A config written for target-iceberg works here. What only configures Iceberg is accepted and ignored with a log line at startup:
- `delete_strategy` and `merge_on_read_maintenance_acknowledged` (DuckLake deletes always write delete files, i.e. merge-on-read; compaction is maintenance);
- `on_error` without `type_source`, which it alone applies to (target-iceberg injects `on_error: raise` into `stream_defaults`);
- a partition `name`;
- the `truncate` and `void` transforms (that entry is skipped);
- `ordering` with `mode: zorder`;
- the table config's maintenance sections (`partition_evolution`, `retention`, sizing, …), `warehouse` and `version`.

If skipping `truncate`/`void` leaves no partition key, the partition counts as not configured, so an existing table's partitioning is not reset by accident.

Settings that would change what data is loaded are **rejected** until implemented, never ignored:
- `write_mode` and `key_properties` (DUCKLKD-41);
- `type_source`, with any `on_error` (DUCKLKD-43).

target-iceberg ignores `write_mode`, and silently appends where upsert was meant. Unknown keys, unknown transforms and columns the stream lacks are rejected too. Column names may be given in full where a Postgres catalog truncates them to 63 characters.

- **Checked at startup:** the `stream_defaults`/`stream_overrides` settings of every stream, and every table's layout sections in `table_config`, are checked when the target starts (a missing or invalid file fails here). Column names, and the older `partition_fields`, are checked when each stream's schema arrives.
- **Both settings can be given as JSON strings** (for environment variables).

The older `partition_fields` setting still works for a stream without a `partition`:

```json
{
    "stream_name": [
        {"column_name": "created_at", "type": "timestamp", "granularity": ["year", "month"]},
        {"column_name": "status", "type": "identifier"}
    ]
}
```

A full list of supported settings and capabilities for this
target is available by running:

```bash
target-ducklake --about
```

### Column types

Each property's JSON Schema type and format give its column type: `integer` is BIGINT (INTEGER with 32-bit bounds), `number` is DECIMAL(38, 9) (or its `precision`/`scale`), `boolean` is BOOLEAN, `object` and `array` are JSON, and `string` is VARCHAR, or DATE, TIME, TIMESTAMP or UUID by its `format`. A string with format `x-singer.decimal` (the singer-sdk's `use_singer_decimal`) is DECIMAL(precision, scale) when both `precision` and `scale` are integers (precision at least 1, scale from 0 to precision), with a precision above 38 capped as for a number. Otherwise, including a precision without a scale, it is VARCHAR, keeping the decimal's text digit for digit (with a warning naming the property), as target-iceberg keeps it as a string: no DuckDB DECIMAL holds an unsized decimal, a bare DECIMAL would be DECIMAL(18, 3), rounding to 3 places, and DECIMAL(p) is scale 0, rounding every fraction. A bare `decimal`/`numeric` `x-sql-datatype` doesn't size it; a sized `decimal(p,s)` or a non-decimal hint such as `double` decides the column. A property declaring several non-null types, such as `["integer", "string"]`, is VARCHAR, and each value is stored as its text (`5`, `1.50`, `true`). Values are staged at their column's own type, so any value a column can hold loads, and one it can't hold fails the batch instead of being changed. One it would round is rounded with a warning, or fails under strict `value_type_checking` (see [Values a column would round](#values-a-column-would-round)).

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

Existing tables keep their column types, apart from safe integer and float widenings. Values are staged at an existing column's own type where the stream's type would change them: a number column into an existing DECIMAL, REAL or DOUBLE column, a number into an existing integer column, a string property into an existing text column, whose text is kept as sent whatever its hint, and an `x-singer.decimal` string into an existing DECIMAL column. A table created before unbounded `x-singer.decimal` strings became VARCHAR has them as DECIMAL(18, 3), which DuckLake can't change to VARCHAR: extra fraction digits are rounded with a warning, and a value with more than 15 integer digits fails naming the column. Recreate the column to keep every digit. So a `double` hint on a column that already exists as DECIMAL(38, 9) still stores DECIMAL(38, 9) values: `1.5e-12` is stored as 0, and a warning reports how many values were rounded. Recreate the column to store doubles.

### Values a column would round

A number with more fraction digits than its DECIMAL column holds (1.231 in DECIMAL(10, 2), or 10 fraction digits in a new table's DECIMAL(38, 9)), or with a fraction for an integer column (1.5 in BIGINT), can only be stored rounded. `value_type_checking` decides what happens:

- **`permissive`** (the default) rounds it as DuckDB's cast of a DECIMAL would, half away from zero (1.225 to 1.23, 2.5 to 3, -2.5 to -3), and logs a warning counting the rounded values in each column of each batch. For a key column the warning says so: records whose keys round to the same value are merged into one row, by the merge and by the in-batch dedup every load method runs: the later record is kept and the earlier one is lost. If a source's keys may carry more fraction digits than the key column, keep it on `strict`. Values are rounded as the exact decimals the JSON held, whatever the property's hint: a `double`-hinted 2.5 for an integer column is 3, where DuckDB's cast of a DOUBLE, which rounds half to even, gave 2.
- **`strict`** fails the batch on the first such value, naming the column, the value and what it would become, and, for a key column, that records would merge. A float key with binary noise (`0.30000000000000004` for a DECIMAL(10, 2) column) fails too: send keys exact at the column's scale.

A number a numeric column can't hold at all fails the batch in both modes, naming the column: one with more integer digits than the DECIMAL holds, one outside BIGINT's range, NaN or infinity for a DECIMAL or integer column, or text that isn't a number. DuckDB's cast fails on these too; no rounding stores them, and the target won't invent a value such as NULL. Other values (booleans, dates and times) are left to pyarrow and DuckDB, whose errors on a bad one are their own. Narrowing DuckDB does silently, such as cutting a timestamp to microseconds or a DOUBLE to a REAL's precision, isn't inspected and isn't covered by the setting. `validate_records` (the singer-sdk's JSON Schema validation) is a separate setting and is off by default.

One way to use it: run `strict` while onboarding a source, until syncs run clean, then switch to `permissive` and watch the warnings. To load such values exactly instead, give the column enough fraction digits (recreate an existing one, or declare the property's `precision` and `scale` for a new table), or a DECIMAL type instead of an integer.

Why the target looks at values at all, rather than leaving them to DuckDB: values are staged in a Parquet file at the column's own type (staging at the stream's type and casting afterwards rounded twice: a DECIMAL(38, 30) column staged at the default (38, 9) lost 21 fraction digits). pyarrow, which writes that file, refuses a decimal with too many fraction digits ("Rescaling Decimal128 value would cause data loss") and truncates a fraction for an integer (1.5 to 1). So the target rounds those values itself, the way DuckDB's cast of a DECIMAL would, to let them load, and says how many it rounded, which DuckDB's cast doesn't. For a value DuckDB would refuse anyway, it fails with the column's name, which DuckDB's error omits. Strict is the only place it fails a value DuckDB would have loaded.

### Overwrite and ACTIVATE_VERSION

**Overwrite** (`load_method: overwrite`, or `overwrite_if_no_pk` for a stream without primary keys) replaces the table's rows atomically. Each batch is staged as a Parquet file in `temp_file_dir`, and at the end of the stream one transaction deletes the old rows and inserts every staged batch. Readers see the old table until that commit. A run that fails or is interrupted in the target leaves the old rows in place. Some limits:
- **STATE is held** while any overwrite stream hasn't committed: mid-run drains don't emit it, so a resumable tap can't skip rows that were never loaded. It is emitted after the commit. STATE is one message for all streams, so other streams' checkpoints wait too. After a crash during a long overwrite, they resume from their last emitted STATE.
- **The swap happens at the end of the input.** If the tap dies but its output ends cleanly, the target swaps in what it received.
- **`temp_file_dir` needs room for the whole stream,** not one batch.
- **A stream with no records still empties the table,** as before.
- **A schema change mid-stream** (a new SCHEMA for the stream) fails the run at the end, with the old rows intact.

**ACTIVATE_VERSION**, as full-table taps send it, retires rows that are gone at the source. The tap tags each record with a table version, then sends `ACTIVATE_VERSION` for that version. The target first loads any buffered records, then handles every row whose `_sdc_table_version` is not the activated version, including rows loaded without a version:
- **By default,** as in the Singer SDK, it sets their `_sdc_deleted_at` to the current UTC time and keeps them. A row the source sends again is revived when it is next loaded.
- **With `hard_delete: true`,** it deletes them.

Some cases retire nothing:
- **Without `add_record_metadata`:** records carry no version, so the message is ignored with a warning.
- **A version announced before any of its records** (as taps do at the start of a sync) is a no-op, so the table never empties mid-sync. A second `ACTIVATE_VERSION` for the same version, even with no records between, ends the sync: the source is empty, and every row is retired.
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
