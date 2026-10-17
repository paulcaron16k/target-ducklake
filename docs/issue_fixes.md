# target-ducklake issue fixes

Fixes developed in this fork ahead of any upstream issue or pull request, in the order they were made. The order matters for conflict resolution: apply them in the order listed, and follow each issue's **Depends on** line.

Each entry mirrors its issue on the [Target DuckLake (Definite fork)](https://github.com/users/paulcaron16k/projects/34) board, where the issue is the source of truth. Since DUCKLKD-19, each story or bug branches from `xflow_dev` as `story/` or `bug/DUCKLKD-<n>_<slug>` and is squash-merged back through a pull request, so it lands as one commit (see [fork-development-model.md](fork-development-model.md)). DUCKLKD-16 to 18 predate this: their `general/` branches start from upstream `main` and were merged with merge commits. A fix is Done when merged into `xflow_dev`; an upstream pull request is optional and is made by cherry-picking the squash commit onto `origin/main`.

| Key | Issue | Branch | Base | Depends on | Status |
| --- | --- | --- | --- | --- | --- |
| [DUCKLKD-16](https://github.com/paulcaron16k/target-ducklake-definite/issues/16) | Numbers lose precision on the way into DECIMAL columns | `general/DUCKLKD-16_decimal-precision` | `origin/main` `e9dc3f3` | — | Done (in `xflow_dev`); no upstream PR |
| [DUCKLKD-17](https://github.com/paulcaron16k/target-ducklake-definite/issues/17) | In-batch dedup can keep an older version of a row | `general/DUCKLKD-17_dedup-keep-last` | `origin/main` `e9dc3f3` | — | Done (in `xflow_dev`); no upstream PR |
| [DUCKLKD-18](https://github.com/paulcaron16k/target-ducklake-definite/issues/18) | Composite merge keys collide and delete unrelated rows | `general/DUCKLKD-18_composite-key-merge` | `origin/main` `e9dc3f3` | — | Done (in `xflow_dev`); no upstream PR |
| [DUCKLKD-19](https://github.com/paulcaron16k/target-ducklake-definite/issues/19) | Merges keyed on a timestamp never match, so every run duplicates rows | `bug/DUCKLKD-19_timestamp-merge-keys` (deleted after merge) | `xflow_dev` | DUCKLKD-18 | Done (squash `4466be0` in `xflow_dev`); no upstream PR |
| [DUCKLKD-21](https://github.com/paulcaron16k/target-ducklake-definite/issues/21) | Honour x-sql-datatype hints in type mapping | `story/DUCKLKD-21_x-sql-datatype-hints` (deleted after merge) | `xflow_dev` | DUCKLKD-16, DUCKLKD-19 | Done (squash `683bd73` in `xflow_dev`); no upstream PR |
| [DUCKLKD-48](https://github.com/paulcaron16k/target-ducklake-definite/issues/48) | Merge keys are compared using the stream's declared type, not the table's column type | `bug/DUCKLKD-48_merge-key-table-types` (deleted after merge) | `xflow_dev` | DUCKLKD-18, DUCKLKD-19, DUCKLKD-21 | Done (squash `09a6e91` in `xflow_dev`); no upstream PR |

**Conflict notes.** DUCKLKD-16 to DUCKLKD-18 each branch from upstream `main` and change different code. They overlap only in the import block at the top of `tests/test_comprehensive.py`; resolve by keeping the union of the imports. DUCKLKD-19 edits `_build_key_columns_sql`, which DUCKLKD-18 introduces. It branched from `xflow_dev`, which already contained DUCKLKD-18, and landed as squash commit `4466be0`. Offered upstream, cherry-pick `4466be0` onto `origin/main` after DUCKLKD-18 and reword the message (drop the DUCKLKD key, `(#45)` and `Fixes #19`). DUCKLKD-21 (`683bd73`) extends `number_precision_and_scale` from DUCKLKD-16 and the temporal key set from DUCKLKD-19, so upstream it must follow both. DUCKLKD-48 (`09a6e91`) replaces DUCKLKD-19's temporal-only cast with a cast of every key to the table's actual type, so upstream it follows DUCKLKD-18, 19 and 21; offering DUCKLKD-19 without DUCKLKD-48 would reintroduce the UUID and REAL key failures.

**Verification baseline.** Upstream `main` already has 556 ruff findings, 4 files the formatter would change, and 2 mypy errors in `sinks.py`. "No new findings" below means none added by the fix, compared against that baseline. Tests run with `uv run --python 3.12 pytest tests/` (the default Python 3.14 has no wheel for the pinned pyarrow 20; tracked as [DUCKLKD-32](https://github.com/paulcaron16k/target-ducklake-definite/issues/32)).

**Branch renames.** The three fix branches were first pushed as `general/fix-*` and renamed on 2026-10-17. The `xflow_dev` merge commits `c3186b4`, `df3afdd` and `f931924` still name the old branches.

---

# DUCKLKD-16 Numbers lose precision on the way into DECIMAL columns

## Intent

Found reviewing the load path on 2026-10-16. JSON Schema `number` columns are created as `DECIMAL(38, 9)`, but `preprocess_record` converted every `Decimal` to `float`, and `parquet_utils` staged numbers in a `float64` Parquet column. DuckDB then cast that double into the DECIMAL column. Values beyond about 15–16 significant digits are silently changed: `12345678901234567890123456789.123456789` was stored as `12345678901234567525491324606.797053952`. Smaller values such as `1234567.123456789` survive, which is why the bug went unnoticed. Money and high-precision identifiers are the main risk.

Reading the type mapping exposed a second, latent bug: a schema with `"precision": 5` and no `"scale"` produced `DECIMAL(5, 9)`, which DuckDB rejects because the scale exceeds the precision.

## Acceptance

- Number columns are staged as `decimal128(p, s)` with exactly the precision and scale of the DuckLake column, taken from one shared helper used by both the Parquet schema and the DDL.
- A 38-digit value written through the staging path into a `DECIMAL(38, 9)` column reads back unchanged.
- Values with more decimal places than the column scale are rounded half-even, matching DuckDB's own cast; ints, floats and `None` are accepted.
- A schema precision below 9 with no scale yields `DECIMAL(p, p)`, never scale > precision.
- Decimals arriving in non-number columns behave as before (converted to float).
- The new tests fail on the old code; no new ruff or mypy findings.

## Evidence

Commit `c9ba9ca` on `general/DUCKLKD-16_decimal-precision`, merged into `xflow_dev` in `c3186b4`. New `number_precision_and_scale()` in `parquet_utils.py` is used by `JSONSchemaToDuckLake._handle_number_type` and `_field_type_to_pyarrow_field`. Values are quantized under a 38-digit decimal context, because Python's default context keeps only 28 digits. The `Decimal`→`float` step in `preprocess_record` is removed.

Reproduced the loss on the old path with DuckDB 1.5.2 (float64 Parquet into `DECIMAL(38, 9)`). The new `TestDecimalPrecision` class has 10 tests, including a DuckDB round trip of the 38-digit value. Branch suite: 75 passed. `xflow_dev` with DUCKLKD-16 to DUCKLKD-18: 83 passed. Also checked that decimal columns keep their type and digits through DUCKLKD-17's Polars dedup. Upstream PR: not opened.

---

# DUCKLKD-17 In-batch dedup can keep an older version of a row

## Intent

Found reviewing the merge path on 2026-10-16. Before writing a batch, `_remove_temp_table_duplicates` drops duplicate keys with Polars `unique(subset=key_properties)`. Polars' default is `keep="any"`, which keeps an arbitrary row per key. When a batch holds several versions of the same key, which is common for CDC and incremental taps, an older version can be kept and then merged over the newer row already in the table. target-iceberg keeps the last record for the same reason.

## Acceptance

- Within a batch, the last occurrence of each key wins, in stream order, for single and composite keys.
- It holds for batches large enough for Polars to work in parallel (tested at 100,000 rows).
- Output row order is deterministic.
- The new tests fail on the old code; no new ruff or mypy findings.

## Evidence

Commit `a75308d` on `general/DUCKLKD-17_dedup-keep-last`, merged into `xflow_dev` in `df3afdd`. One change in `sinks.py`: `unique(subset=..., keep="last", maintain_order=True)`. The new `TestInBatchDeduplication` class has 3 tests: single key, composite key, and a 100k-row batch with 1,000 distinct keys. All 3 fail on the old `keep="any"`. Branch suite: 68 passed. Upstream PR: not opened.

---

# DUCKLKD-18 Composite merge keys collide and delete unrelated rows

## Intent

Found reviewing the merge SQL on 2026-10-16. `merge_into_table` deleted matching rows with `WHERE (k1||k2) IN (SELECT (k1||k2) FROM 'file.parquet')`. Concatenation makes different keys equal: merging the key (`'12'`,`'3'`) also deleted the unrelated row (`'1'`,`'23'`). This silently loses data in any table with a composite key whose parts can run together. The code also rewrote the caller's `key_properties` list in place.

## Acceptance

- The merge DELETE compares each key column separately: `("k1", "k2") IN (SELECT "k1", "k2" FROM 'file.parquet')`.
- Merging (`'12'`,`'3'`) leaves (`'1'`,`'23'`) in place, on unpartitioned and partitioned DuckLake tables.
- Single-key merges still produce a simple `IN`; datetime keys keep their existing VARCHAR cast (changing that is DUCKLKD-19).
- Key names are quoted, and the caller's key list is not changed.
- The new tests fail on the old code; no new ruff or mypy findings.

## Evidence

Commit `48b523d` on `general/DUCKLKD-18_composite-key-merge`, merged into `xflow_dev` in `f931924`, where the test-import conflict with DUCKLKD-17 was resolved by keeping both sets of imports. The new `DuckLakeConnector._build_key_columns_sql()` builds the key column list.

Verified on a local DuckLake (DuckDB 1.5.2, DuckDB-file catalog, local data path), partitioned and unpartitioned. The old SQL deleted the unrelated row. Row-value `IN`, `EXISTS` and `DELETE … USING` all kept it; row-value `IN` was chosen as the smallest change. The new `TestMergeKeyMatching` class has 5 tests, including the collision scenario run through the real merge SQL in DuckDB. All 5 fail on the old code. Branch suite: 70 passed. Upstream PR: not opened.

---

# DUCKLKD-19 Merges keyed on a timestamp never match, so every run duplicates rows

## Intent

Found while testing DUCKLKD-18 on 2026-10-16. For datetime key columns, `merge_into_table` compares `ts::VARCHAR` on both sides, because DuckDB refuses to compare a TIMESTAMP column with a VARCHAR column in an `IN` clause (binder error, confirmed). The two sides render differently:
- The singer-sdk always parses date-time values into `datetime`, and staging writes them as `2024-01-01 10:00:00.000000`, plus `Z` when timezone-aware.
- The table's TIMESTAMP column renders as `2024-01-01 10:00:00`.

On a local DuckLake, the current comparison matched **zero** rows, for both whole-second and fractional timestamps. So for any stream with a timestamp key, the DELETE removes nothing and every merge appends another copy of each row. Casting the staged column to the table type, `CAST(s.ts AS TIMESTAMP)`, matched correctly in the same test.

## Acceptance

- Datetime keys are compared as TIMESTAMP: the staged column is cast to the table column's type, not both sides to VARCHAR.
- Re-merging a batch with whole-second, fractional, naive and UTC-aware timestamp keys leaves exactly one row per key.
- Composite keys mixing datetime and other columns work (row-value comparison from DUCKLKD-18).
- Added during the fix: DATE and TIME keys merge too. They had no cast at all and failed the merge with a binder error (same root cause).
- A regression test reproduces the duplicate on the old code.
- A note on cleaning up duplicates already present in existing tables, since this fix stops new ones but doesn't remove old ones.

## Evidence

> **Superseded in part by [DUCKLKD-48](https://github.com/paulcaron16k/target-ducklake-definite/issues/48):** `temporal_key_types` and the temporal-only cast described below were replaced by `key_column_types`, which casts every key to the table's actual column type.

Squash-merged into `xflow_dev` as `4466be0` through fork PR #45 (branch `bug/DUCKLKD-19_timestamp-merge-keys`, deleted after merge). For each DATETIME, DATE or TIME key, the staged side is cast to the column's type, which is the cast `INSERT` applies; the table side is compared natively. `merge_into_table` takes `temporal_key_types` (column → type) instead of `date_type_keys`, built by the sink from the DuckLake schema.

Old behaviour reproduced: DATETIME keys duplicated on every re-merge (naive whole-second, fractional and UTC-aware values); DATE and TIME keys failed with `Binder Error: Cannot compare values of type DATE and VARCHAR in IN/ANY/ALL clause`. New SQL checked on a local DuckLake (DuckDB 1.5.2): three re-merges with a composite temporal + id key leave one row per key with the latest values, unpartitioned and partitioned by `year(k)`, `month(k)` and `k`. New `TestTemporalMergeKeys` (5 re-merge cases plus a sink test); `xflow_dev` suite: 89 passed. No new ruff or mypy findings.

Cleanup for duplicates already in existing tables, verified on a DuckLake copy partitioned by `year(k)`; keeps the latest row per key by `rowid` (insertion order). Run it on a copy first:

```sql
BEGIN;
DELETE FROM lake.schema.tbl
WHERE rowid NOT IN (SELECT max(rowid) FROM lake.schema.tbl GROUP BY <key columns>);
COMMIT;
```

Upstream PR: not opened.

---

# DUCKLKD-21 Honour x-sql-datatype hints in type mapping

## Intent

Database taps declare each source column's own type in `x-sql-datatype` (smallint vs bigint, real vs double, numeric(p,s), timestamp vs timestamptz, uuid, jsonb). The target ignored them, so lake types drifted from the source: `smallint` became BIGINT, `numeric(12,4)` became DECIMAL(38,9), `timestamptz` became a naive TIMESTAMP. target-iceberg's mapping.py honours them.

## Acceptance

- Supported hints map to the matching DuckDB type; unknown hints fall back to today's mapping.
- Tests cover each supported hint.

## Evidence

> **Superseded in part by [DUCKLKD-48](https://github.com/paulcaron16k/target-ducklake-definite/issues/48):** the shared temporal key set (`TEMPORAL_TYPES`) mentioned below was removed; merge keys are now cast to the table's actual column types. The independent review of this PR found the UUID-key regression fixed there.

Squash-merged into `xflow_dev` as `683bd73` through fork PR #46 (branch `story/DUCKLKD-21_x-sql-datatype-hints`, deleted after merge). A new `type_hints.py` holds the vocabulary and is used by both the DDL (`JSONSchemaToDuckLake.to_sql_type`) and the parquet staging, so they can't disagree:

| Hint (case and spacing ignored) | DuckDB type |
| --- | --- |
| `smallint`, `int2` / `integer`, `int`, `int4` / `bigint`, `int8` | SMALLINT / INTEGER / BIGINT |
| `real`, `float4`, `float(p<=24)` | REAL |
| `float`, `float8`, `double`, `double precision`, `float(p>24)` | DOUBLE |
| `decimal(p,s)`, `numeric(p,s)` | DECIMAL(p,s), capped at 38 |
| `timestamp`, `timestamp without time zone`, `timestamp_ntz` | TIMESTAMP |
| `timestamptz`, `timestamp with time zone`, `timestamp_tz`, `timestamp_ltz` | TIMESTAMPTZ |
| `date` / `time`, `time without time zone` | DATE / TIME |
| `uuid`, `uniqueidentifier` / `json`, `jsonb` | UUID / JSON |

Decimal hints stage at the hinted precision; float hints stage as float64 (decimal staging would round `1.5e-12` to 0). Hinted types render through `DuckDBType`, because SQLAlchemy renders `TIMESTAMP(timezone=True)` as plain `TIMESTAMP`. A `timestamptz` key gets DUCKLKD-19's merge cast.

Deliberately unsupported: `timetz` (on a local DuckLake, `10:00+02` was stored as `10:00+00`: the Parquet write drops the offset without converting), `datetime` (MySQL's meaning is the opposite of ISO's), `tinyint` (MySQL booleans) and binary hints (base64 needs a decode on insert). An unqualified `float` maps to DOUBLE, unlike target-iceberg. Existing tables keep their column types except through the existing safe widenings.

`TestSqlDatatypeHints` has 43 cases; 36 fail on the old code (the 7 fallback and no-hint cases pass by design). `xflow_dev` suite: 132 passed. No new ruff or mypy findings; `type_hints.py` fully clean. Verified on a local DuckLake (DuckDB 1.5.2): every hinted type created, partitioned by `year("at")`, and a composite (`id`, `timestamptz`) re-merge left one row with exact values. Found, not fixed: non-UTC offsets fail staging unless `convert_tz_to_utc` is set, filed as [DUCKLKD-47](https://github.com/paulcaron16k/target-ducklake-definite/issues/47).

Upstream PR: not opened.

---

# DUCKLKD-48 Merge keys are compared using the stream's declared type, not the table's column type

## Intent

Found by the independent Sonnet review of PR #46 (DUCKLKD-21) on 2026-10-17 and reproduced on `xflow_dev`. The merge DELETE cast staged keys only for temporal types, and to the type the stream declares rather than the column type the table actually has. UUID key columns (from an `x-sql-datatype: uuid` hint, a DUCKLKD-21 regression, or the SDK's own `format: uuid`, pre-existing) failed every merge with `Cannot compare values of type UUID and VARCHAR in IN/ANY/ALL clause`. REAL keys never matched (table `0.10000000149` vs staged float64 `0.1`), so every run duplicated the row. An existing TIMESTAMP column whose stream now hints `timestamptz` was cast to TIMESTAMPTZ; with timezone-aware values and a non-UTC session TimeZone each run duplicated the row.

## Acceptance

- Every staged merge key is cast to the target table's actual column type, read from the table.
- Re-merging leaves one row per key for VARCHAR, BIGINT, INTEGER, SMALLINT, DECIMAL, REAL, DOUBLE, UUID, DATE, TIME, TIMESTAMP and TIMESTAMPTZ keys, alone and composite.
- Existing tables whose column type differs from the stream's hint merge correctly, including under a non-UTC session TimeZone.
- Regression tests reproduce the crash and the duplicates on the old code by assertion, not by TypeError.
- No new ruff or mypy findings.

## Evidence

Squash-merged into `xflow_dev` as `09a6e91` through fork PR #52 (branch `bug/DUCKLKD-48_merge-key-table-types`, deleted after merge). The sink reads the table's column types once, on the first merge (`_key_column_types()`), matches key names case-insensitively (the singer-sdk lowercases `key_properties` while columns keep the property's case), and passes `key_column_types` to `merge_into_table`, which casts every staged key to its column's type: the same cast `INSERT` applies. `type_hints.TEMPORAL_TYPES` was removed.

`TestMergeKeyTableTypes` has 31 cases through the real sink lifecycle (`setup()` plus two `process_batch()` calls on an in-memory catalog, session TimeZone `America/New_York`): 13 key types single and composite, an upper-case key, an existing TIMESTAMP table with a `timestamptz` hint, and the append query count. 8 regression cases fail on the pre-PR code: REAL x2 and the existing table by duplicate-row assertion; `uuid` format x2, `uuid` hint x2 and the upper-case key by the binder error. `xflow_dev` suite: 162 passed; no new ruff, format or mypy findings. On a local DuckLake (non-UTC session, composite keys), REAL, UUID, an upper-case UUID key, TIMESTAMPTZ partitioned by `year(k)`, DATE by `month(k)` and DECIMAL with identity partitioning each left one row per key after three merges.

Independent Sonnet review: no blocking findings; two should-fix findings reproduced and fixed in their own commits (`3f9e8f5` case-insensitive key matching, `f6171d7` key types read only when merging); three nits (mock test removed, matrix run under a non-UTC zone with a BOOLEAN key, docs/format note needing no PR change). Follow-up review of those commits: no blocking or should-fix findings; one nit declined with reasoning on the PR. Found and filed: [DUCKLKD-53](https://github.com/paulcaron16k/target-ducklake-definite/issues/53), `prepare_table` matching column names case-sensitively.

Upstream PR: not opened.
