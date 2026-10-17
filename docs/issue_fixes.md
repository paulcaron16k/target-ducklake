# target-ducklake issue fixes

Fixes developed in this fork ahead of any upstream issue or pull request, in the order they were made. The order matters for conflict resolution: apply them in the order listed, and follow each issue's **Depends on** line.

Each entry mirrors its issue on the [Target DuckLake (Definite fork)](https://github.com/users/paulcaron16k/projects/34) board, where the issue is the source of truth. Since DUCKLKD-19, each story or bug branches from `xflow_dev` as `story/` or `bug/DUCKLKD-<n>_<slug>` and is squash-merged back through a pull request, so it lands as one commit (see [fork-development-model.md](fork-development-model.md)). DUCKLKD-16 to 18 predate this: their `general/` branches start from upstream `main` and were merged with merge commits. A fix is Done when merged into `xflow_dev`; an upstream pull request is optional and is made by cherry-picking the squash commit onto `origin/main`.

| Key | Issue | Branch | Base | Depends on | Status |
| --- | --- | --- | --- | --- | --- |
| [DUCKLKD-16](https://github.com/paulcaron16k/target-ducklake-definite/issues/16) | Numbers lose precision on the way into DECIMAL columns | `general/DUCKLKD-16_decimal-precision` | `origin/main` `e9dc3f3` | — | Done (in `xflow_dev`); no upstream PR |
| [DUCKLKD-17](https://github.com/paulcaron16k/target-ducklake-definite/issues/17) | In-batch dedup can keep an older version of a row | `general/DUCKLKD-17_dedup-keep-last` | `origin/main` `e9dc3f3` | — | Done (in `xflow_dev`); no upstream PR |
| [DUCKLKD-18](https://github.com/paulcaron16k/target-ducklake-definite/issues/18) | Composite merge keys collide and delete unrelated rows | `general/DUCKLKD-18_composite-key-merge` | `origin/main` `e9dc3f3` | — | Done (in `xflow_dev`); no upstream PR |
| [DUCKLKD-19](https://github.com/paulcaron16k/target-ducklake-definite/issues/19) | Merges keyed on a timestamp never match, so every run duplicates rows | `bug/DUCKLKD-19_timestamp-merge-keys` | `xflow_dev` | DUCKLKD-18 (already in `xflow_dev`) | In Progress |

**Conflict notes.** DUCKLKD-16 to DUCKLKD-18 each branch from upstream `main` and change different code. They overlap only in the import block at the top of `tests/test_comprehensive.py`; resolve by keeping the union of the imports. DUCKLKD-19 edits `_build_key_columns_sql`, which DUCKLKD-18 introduces. It branches from `xflow_dev`, which already contains DUCKLKD-18; offered upstream, it must follow DUCKLKD-18.

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
- A regression test reproduces the duplicate on the old code.
- A note on cleaning up duplicates already present in existing tables, since this fix stops new ones but doesn't remove old ones.

## Evidence

Reproduced on a local DuckLake (DuckDB 1.5.2) while testing DUCKLKD-18: the `::VARCHAR` comparison matched 0 rows for naive and UTC-aware, whole-second and fractional timestamps; native `IN` raises a binder error (TIMESTAMP vs VARCHAR); `IN (SELECT CAST(ts AS TIMESTAMP) ...)` matched both rows. Fix branches from `xflow_dev`, which already contains DUCKLKD-18.
