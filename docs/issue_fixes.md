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
| [DUCKLKD-49](https://github.com/paulcaron16k/target-ducklake-definite/issues/49) | NULL key values never match, so re-merging duplicates those rows | `bug/DUCKLKD-49_null-merge-keys` (deleted after merge) | `xflow_dev` | DUCKLKD-48 | Done (squash `8f83caf` in `xflow_dev`); no upstream PR |
| [DUCKLKD-47](https://github.com/paulcaron16k/target-ducklake-definite/issues/47) | Timestamps with a non-UTC offset fail staging unless convert_tz_to_utc is set | `bug/DUCKLKD-47_non-utc-offsets` (deleted after merge) | `xflow_dev` | DUCKLKD-21 (timestamptz tests), DUCKLKD-48 | Done (squash `f06c733` in `xflow_dev`); no upstream PR |
| [DUCKLKD-50](https://github.com/paulcaron16k/target-ducklake-definite/issues/50) | x-sql-datatype hints on anyOf properties are lost; invalid decimal hints produce invalid DDL | `bug/DUCKLKD-50_anyof-and-invalid-decimal-hints` (deleted after merge) | `xflow_dev` | DUCKLKD-21 | Done (squash `0bb2fe7` in `xflow_dev`); no upstream PR |
| [DUCKLKD-59](https://github.com/paulcaron16k/target-ducklake-definite/issues/59) | A decimal hint whose precision exceeds 38 caps the scale with it, leaving no integer digits | `bug/DUCKLKD-59_decimal-precision-cap` (deleted after merge) | `xflow_dev` | DUCKLKD-16, DUCKLKD-21 | Done (squash `ac91dc2` in `xflow_dev`); no upstream PR |
| [DUCKLKD-60](https://github.com/paulcaron16k/target-ducklake-definite/issues/60) | flatten_schema crashes on an all-null or untyped anyOf branch and mutates the caller's schema | `bug/DUCKLKD-60_flatten-anyof-crash` (deleted after merge) | `xflow_dev` | DUCKLKD-50 | Done (squash `5e8461c` in `xflow_dev`); no upstream PR |
| [DUCKLKD-65](https://github.com/paulcaron16k/target-ducklake-definite/issues/65) | Properties declared with oneOf, allOf or {} are silently left out of the table, and their values are lost | `bug/DUCKLKD-65_oneof-allof-columns` (deleted after merge) | `xflow_dev` | DUCKLKD-60 | Done (squash `6ca7032` in `xflow_dev`, with DUCKLKD-66); no upstream PR |
| [DUCKLKD-66](https://github.com/paulcaron16k/target-ducklake-definite/issues/66) | An anyOf branch with several types gets a staging type that changes from run to run | `bug/DUCKLKD-65_oneof-allof-columns` (deleted after merge) | `xflow_dev` | DUCKLKD-60 | Done (squash `6ca7032` in `xflow_dev`, with DUCKLKD-65); no upstream PR |
| [DUCKLKD-70](https://github.com/paulcaron16k/target-ducklake-definite/issues/70) | A null in a JSON (object or array) column is stored as the JSON text null, not SQL NULL | `bug/DUCKLKD-70_json-null` (deleted after merge) | `xflow_dev` | DUCKLKD-60 | Done (squash `49d221e` in `xflow_dev`); no upstream PR |
| [DUCKLKD-69](https://github.com/paulcaron16k/target-ducklake-definite/issues/69) | auto_cast_timestamps ignores properties declared with anyOf, oneOf or allOf | `bug/DUCKLKD-69_autocast-choices` (deleted after merge) | `xflow_dev` | DUCKLKD-65 | Done (squash `e5445b9` in `xflow_dev`); no upstream PR |
| [DUCKLKD-53](https://github.com/paulcaron16k/target-ducklake-definite/issues/53) | Existing columns whose name differs only in case are re-added, failing setup | `bug/DUCKLKD-53_case-insensitive-columns` (deleted after merge) | `xflow_dev` | DUCKLKD-48 | Done (squash `4b3880a` in `xflow_dev`); no upstream PR |
| [DUCKLKD-55](https://github.com/paulcaron16k/target-ducklake-definite/issues/55) | convert_tz_to_utc doesn't convert unparsed date-time strings | `bug/DUCKLKD-55_convert-tz-strings` (deleted after merge) | `xflow_dev` | DUCKLKD-47 | Done (squash `472e2bc` in `xflow_dev`); no upstream PR |
| [DUCKLKD-57](https://github.com/paulcaron16k/target-ducklake-definite/issues/57) | Without convert_tz_to_utc, a timestamp column's wall-clock time depends on batch order | `bug/DUCKLKD-57_mixed-zone-wall-clock` (deleted after merge) | `xflow_dev` | DUCKLKD-47 | Done (squash `3798b52` in `xflow_dev`); no upstream PR |
| [DUCKLKD-68](https://github.com/paulcaron16k/target-ducklake-definite/issues/68) | A column with several JSON types is staged at its first type while the table column is VARCHAR | `bug/DUCKLKD-68_staging-follows-ddl` (deleted after merge) | `xflow_dev` | DUCKLKD-21, DUCKLKD-65 | Done (squash `2c668e9`, with DUCKLKD-61 in `xflow_dev`); no upstream PR |
| [DUCKLKD-61](https://github.com/paulcaron16k/target-ducklake-definite/issues/61) | A hint that conflicts with the JSON type gives different DDL and staging types | `bug/DUCKLKD-68_staging-follows-ddl` (deleted after merge) | `xflow_dev` | DUCKLKD-21, DUCKLKD-50 | Done (squash `2c668e9`, with DUCKLKD-68 in `xflow_dev`); no upstream PR |
| [DUCKLKD-63](https://github.com/paulcaron16k/target-ducklake-definite/issues/63) | Decimals are staged at the stream's declared scale, not an existing column's, so they are rounded silently | `bug/DUCKLKD-63_existing-decimal-scale` (deleted after merge) | `xflow_dev` | DUCKLKD-68 | Done (squash `aac6968` in `xflow_dev`); no upstream PR |
| [DUCKLKD-51](https://github.com/paulcaron16k/target-ducklake-definite/issues/51) | Support more x-sql-datatype spellings and stop warning on hints that match the default | `story/DUCKLKD-51_more-hint-spellings` (deleted after merge) | `xflow_dev` | DUCKLKD-61, DUCKLKD-63 | Done (squash `ab371be` in `xflow_dev`); no upstream PR |
| [DUCKLKD-79](https://github.com/paulcaron16k/target-ducklake-definite/issues/79) | Parallel draining with a SQLite catalog fails the run with 'database is locked' and aborts the process | `bug/DUCKLKD-79_sqlite-parallel-drain` (deleted after merge) | `xflow_dev` | — | Done (squash `ebb2e28` in `xflow_dev`); no upstream PR |
| [DUCKLKD-20](https://github.com/paulcaron16k/target-ducklake-definite/issues/20) | Make overwrite atomic and honour ACTIVATE_VERSION | `story/DUCKLKD-20_atomic-overwrite` (deleted after merge) | `xflow_dev` | DUCKLKD-63 (`prepare_table` returns types), DUCKLKD-79 (`Targetducklake` overrides) | Done (squash `87f8e9f` in `xflow_dev`); no upstream PR |
| [DUCKLKD-38](https://github.com/paulcaron16k/target-ducklake-definite/issues/38) | Sort keys and bucket partitions | `story/DUCKLKD-38_sort-and-bucket-partitions` (deleted after merge) | `xflow_dev` | DUCKLKD-63 (`prepare_table`), DUCKLKD-79 (`Targetducklake.__init__`) | Done (squash `72a97db` in `xflow_dev`); no upstream PR |
| [DUCKLKD-94](https://github.com/paulcaron16k/target-ducklake-definite/issues/94) | Accept target-iceberg's layout config and Zamboni table-config.json, ignoring Iceberg-only settings | `story/DUCKLKD-94_shared-iceberg-config` (deleted after merge) | `xflow_dev` | DUCKLKD-38 (`layout.py`) | Done (squash `80362a3` in `xflow_dev`); no upstream PR |
| [DUCKLKD-84](https://github.com/paulcaron16k/target-ducklake-definite/issues/84) | An x-singer.decimal string without a precision lands in DECIMAL(18,3), rounding to 3 places | `bug/DUCKLKD-84_unbounded-singer-decimal` (deleted after merge) | `xflow_dev` | DUCKLKD-63 (`_staged_columns`) | Done (squash `701991e` in `xflow_dev`); no upstream PR |
| [DUCKLKD-97](https://github.com/paulcaron16k/target-ducklake-definite/issues/97) | A merge key rounded to its DECIMAL key column's scale (or by the cast to an integer key column) merges distinct keys into one row | `bug/DUCKLKD-97_rounded-merge-key` | `xflow_dev` | DUCKLKD-63 (`_staged_columns`, `_decimal_column`), DUCKLKD-84 | Done (squash `77991dc` in `xflow_dev`); no upstream PR |
| [DUCKLKD-99](https://github.com/paulcaron16k/target-ducklake-definite/issues/99) | A number on an existing integer column is rounded silently by DuckDB's cast; adds `value_type_checking` (permissive/strict) over every rounding staging does | `bug/DUCKLKD-99_value-type-checking` | `xflow_dev` | DUCKLKD-97, DUCKLKD-63, DUCKLKD-84 | Done (squash `5ebe7f9` in `xflow_dev`); no upstream PR |
| [DUCKLKD-22](https://github.com/paulcaron16k/target-ducklake-definite/issues/22) | Decimals inside JSON columns are converted to float; now JSON numbers with every digit | `bug/DUCKLKD-22_json-decimals` (deleted after merge) | `xflow_dev` | — | Done (squash `516db8c` in `xflow_dev`); no upstream PR |
| [DUCKLKD-89](https://github.com/paulcaron16k/target-ducklake-definite/issues/89) | date-time properties load as TIMESTAMPTZ by default (`date_time_type`); zone-less values read as UTC; soft-delete time correct in TIMESTAMPTZ | `story/DUCKLKD-89_date-time-timestamptz` (deleted after merge) | `xflow_dev` | DUCKLKD-38/94 (`layout.py` stream settings), DUCKLKD-97/99 (`StagingRules`), DUCKLKD-20 (`mark_other_versions_deleted`) | Done (squash `87d37d5` in `xflow_dev`); no upstream PR |
| [DUCKLKD-103](https://github.com/paulcaron16k/target-ducklake-definite/issues/103) | meltano.yml was not valid YAML (an unquoted ': '), nor valid against Meltano's schema (`default:`, plain-string options) | `bug/DUCKLKD-103_meltano-yml` (deleted after merge) | `xflow_dev` | — | Done (squash `551d2be` in `xflow_dev`); no upstream PR |
| [DUCKLKD-104](https://github.com/paulcaron16k/target-ducklake-definite/issues/104) | With convert_tz_to_utc, a date-time whose UTC time leaves years 1-9999 failed the batch or kept its wall-clock time | `bug/DUCKLKD-104_utc-year-range` (deleted after merge) | `xflow_dev` | DUCKLKD-89 (`_as_utc_instant`) | Done (squash `131e348` in `xflow_dev`); no upstream PR |
| [DUCKLKD-91](https://github.com/paulcaron16k/target-ducklake-definite/issues/91) | Strings with contentEncoding: base64 load as BLOB, decoded strictly; undeclared text into BLOB and identity partitions on BLOB refused | `story/DUCKLKD-91_base64-binary` (deleted after merge) | `xflow_dev` | DUCKLKD-63 (`_staged_columns`), DUCKLKD-38/94 (layout) | Done (squash `584ffa9` in `xflow_dev`); no upstream PR |

**Conflict notes.** DUCKLKD-16 to DUCKLKD-18 each branch from upstream `main` and change different code. They overlap only in the import block at the top of `tests/test_comprehensive.py`; resolve by keeping the union of the imports. DUCKLKD-19 edits `_build_key_columns_sql`, which DUCKLKD-18 introduces. It branched from `xflow_dev`, which already contained DUCKLKD-18, and landed as squash commit `4466be0`. Offered upstream, cherry-pick `4466be0` onto `origin/main` after DUCKLKD-18 and reword the message (drop the DUCKLKD key, `(#45)` and `Fixes #19`). DUCKLKD-21 (`683bd73`) extends `number_precision_and_scale` from DUCKLKD-16 and the temporal key set from DUCKLKD-19, so upstream it must follow both. DUCKLKD-48 (`09a6e91`) replaces DUCKLKD-19's temporal-only cast with a cast of every key to the table's actual type, so upstream it follows DUCKLKD-18, 19 and 21; offering DUCKLKD-19 without DUCKLKD-48 would reintroduce the UUID and REAL key failures. DUCKLKD-49 (`8f83caf`) rewrites the same DELETE as a NULL-safe `USING` join and renames DUCKLKD-48's helper, so upstream it follows DUCKLKD-48. DUCKLKD-50 (`0bb2fe7`) changes `decimal_hint` in `type_hints.py`, which DUCKLKD-21 introduces, and makes `flatten.py` import that module, so upstream it follows DUCKLKD-21. DUCKLKD-59 (`ac91dc2`) rewrites `number_precision_and_scale` from DUCKLKD-16 and changes a DUCKLKD-21 test expectation, so upstream it follows both. DUCKLKD-60 (`5e8461c`) rewrites the `anyOf` path in `flatten_schema`, where DUCKLKD-50 added `_with_wrapper_hint`, so upstream it follows DUCKLKD-50; it also overrides `get_validator` in `sinks.py`. DUCKLKD-65 and DUCKLKD-66 (one squash, `6ca7032`) replace DUCKLKD-60's `_anyof_column` and `_first_choice_column` with `_choice_column`, so upstream they follow DUCKLKD-60 and travel together. DUCKLKD-70 (`49d221e`), DUCKLKD-69 (`e5445b9`), DUCKLKD-53 (`4b3880a`), DUCKLKD-55 (`472e2bc`) and DUCKLKD-57 (`3798b52`) change separate code and merged cleanly in that order; DUCKLKD-69 builds on DUCKLKD-65's `_choice_column`, DUCKLKD-55 and DUCKLKD-57 on DUCKLKD-47's text rendering. DUCKLKD-68 and DUCKLKD-61 (one squash, `2c668e9`) replace staging by JSON type (`_field_type_to_pyarrow_field`, `flatten_schema_to_pyarrow_schema`) with `staging_type` over the DDL, and rewrite `_coerce_column`, so upstream they follow DUCKLKD-21, 50, 57 and 65; the merge with DUCKLKD-57 kept both `_mixes_zones` and the per-type coercion. DUCKLKD-63 (`aac6968`) builds on `staging_type` and changes `prepare_table` to return column types, so it follows DUCKLKD-68. DUCKLKD-51 (`ab371be`) extends DUCKLKD-61's hint rule and relies on DUCKLKD-63's existing-column staging, so it follows both. DUCKLKD-79 (`ebb2e28`) changes only `target.py` (drain parallelism and the SDK's `_drain_all`), independent of the others. DUCKLKD-38 (`72a97db`) adds `layout.py`, replaces `prepare_table`'s `partition_fields` parameter with a `layout` and removes `_add_partition_fields`, so upstream it follows DUCKLKD-20's `prepare_table` change and DUCKLKD-63. DUCKLKD-20 (`87f8e9f`) removes `prepare_table`'s `overwrite_table` truncate, adds `replace_table_contents`, `delete_other_versions` and `mark_other_versions_deleted` to the connector, and overrides the SDK's `_write_state_message` and `_process_activate_version_message` beside DUCKLKD-79's overrides in `target.py`, so upstream it follows DUCKLKD-63 and DUCKLKD-79. DUCKLKD-94 (`80362a3`) extends DUCKLKD-38's `layout.py` (`table_config`, Iceberg-only keys ignored, exact-case transforms) and adds `bin/refresh-table-config-contract.sh`, `tests/contracts/` and `tests/test_table_config_contract.py`, so upstream it follows DUCKLKD-38; the vendored Zamboni spec is xflow-specific and would likely stay fork-only. DUCKLKD-84 (`701991e`) adds an `x-singer.decimal` format handler to `JSONSchemaToDuckLake` and a rule to DUCKLKD-63's `_staged_columns`, so upstream it follows DUCKLKD-63; it is a general fix, a candidate for upstream. DUCKLKD-97 (`77991dc`) adds a key rule to `_staged_columns` beside DUCKLKD-84's and an `exact` mode to DUCKLKD-63's `_decimal_column`, threaded through `create_pyarrow_table` and `concat_tables`, so upstream it follows DUCKLKD-63 and DUCKLKD-84; a general fix, a candidate for upstream. DUCKLKD-99 (`5ebe7f9`) replaces DUCKLKD-97's `key_columns` parameter and key staging rule with `StagingRules` and a general fractional-to-integer rule, adds `_integer_column`, changes staging's rounding to half away from zero and adds the `value_type_checking` setting, so upstream it follows DUCKLKD-97 and must travel with it; it makes DUCKLKD-97's key failure strict-only. DUCKLKD-22 (`516db8c`) replaces `flatten.py`'s `CustomJSONEncoder` with `dump_json_value` (simplejson, `use_decimal`) and makes `simplejson` a direct dependency; it depends on no other story, so it can be offered upstream alone. DUCKLKD-89 (`87d37d5`) adds `date_time_type` to DUCKLKD-38/94's `stream_defaults`/`stream_overrides` keys in `layout.py` (`_stream_settings`, `resolve_date_time_type`), `utc_columns` to DUCKLKD-99's `StagingRules`, and a column-type argument to DUCKLKD-20's `mark_other_versions_deleted`, so upstream it follows DUCKLKD-20, 38, 94 and 99; offered upstream, its default should be `timestamp` (opt-in), since `timestamptz` changes new tables for existing users. DUCKLKD-103 (`551d2be`) changes only `meltano.yml`, adds a test and declares `pyyaml` for tests; it depends on nothing. DUCKLKD-104 (`131e348`) adds `_utc_text` and `_mixes_text_and_datetimes` to `parquet_utils.py` and changes DUCKLKD-89's `_as_utc_instant`, so upstream it follows DUCKLKD-89; its `_normalize_datetime` and `_coerce_column` parts alone fix `convert_tz_to_utc` on upstream `main`. DUCKLKD-91 (`584ffa9`) adds binary rules to `type_hints.py` (`declares_base64_binary`, `is_binary_hint`), BLOB staging and strict decoding to `parquet_utils.py`, `_declared_text_type` to `JSONSchemaToDuckLake`, `existing_column_types` to the connector, and `_check_blob_columns`/`_check_partition_types` to the sink (which read DUCKLKD-38's `TableLayout`), so upstream it follows DUCKLKD-38, 63 and 84. DUCKLKD-63 and DUCKLKD-51 were developed stacked; after their parents' squashes they were applied onto `xflow_dev` in a merge commit, so their squashes contain only their own changes.

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

Commit `48b523d` on `general/DUCKLKD-18_composite-key-merge`, merged into `xflow_dev` in `f931924`, where the test-import conflict with DUCKLKD-17 was resolved by keeping both sets of imports. The new `DuckLakeConnector._build_key_columns_sql()` builds the key column list (since replaced by `_build_key_match_sql()` in DUCKLKD-49, which keeps the per-column comparison as a NULL-safe join).

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

Cleanup for duplicates already in existing tables, verified on a DuckLake copy partitioned by `year(k)`; keeps the latest row per key by `rowid` (insertion order). `GROUP BY` puts NULL key values in one group, which matches the rule adopted in [DUCKLKD-49](https://github.com/paulcaron16k/target-ducklake-definite/issues/49): NULL key values match NULL. Run it on a copy first:

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

---

# DUCKLKD-49 NULL key values never match, so re-merging duplicates those rows

## Intent

Found by the independent Sonnet review of PR #45 (DUCKLKD-19) on 2026-10-17 and reproduced on `xflow_dev`; pre-existing. The merge DELETE used `(k...) IN (SELECT k... FROM staged)`, and NULL never compares equal, so a row whose key had a NULL value was never deleted and every re-merge appended another copy. The DUCKLKD-19 cleanup SQL's handling of NULL keys also needed a stated rule.

## Acceptance

- Decide and record whether NULL key values match each other or are rejected.
- With the chosen rule, re-merging a batch with NULL key values never duplicates rows.
- The DUCKLKD-19 cleanup SQL is corrected for NULL keys (here and in the PR #45 description).
- A regression test reproduces the duplicate on the old code.

## Evidence

**Decision: NULL key values match NULL** (`IS NOT DISTINCT FROM`). The in-batch dedup (Polars `unique`) already treats NULL keys as equal, so the merge now agrees with it; rejecting NULL keys would fail loads that work today. `(NULL, a)`, `(NULL, b)` and `(x, a)` remain three different composite keys. Behaviour change for existing tables: duplicate NULL-key rows left by the old merge are all replaced by the next batch carrying that key. The DUCKLKD-19 cleanup SQL already follows this rule (`GROUP BY` treats NULLs as one group); noted in its entry above and on PR #45.

Squash-merged into `xflow_dev` as `8f83caf` through fork PR #54 (branch `bug/DUCKLKD-49_null-merge-keys`, deleted after merge). The DELETE is now `DELETE FROM t AS "target" USING (SELECT CAST("k" AS <table type>) AS "k", ... FROM 'file') AS "staged" WHERE "target"."k" IS NOT DISTINCT FROM "staged"."k" AND ...`, built by `_build_key_match_sql()`. Performance on a 2M-row DuckLake table with a 50k-row batch: no regression (median 0.22 s vs 0.29 s for the old `IN`; the reviewer measured them equal within noise). An `EXISTS` form was rejected at about 2x slower.

Four new tests through the real sink lifecycle (NULL single and composite keys, NULL keys distinct from other keys, existing NULL-key duplicates collapsing) all fail on the old merge SQL by duplicate-row assertion. `xflow_dev` suite: 166 passed; no new ruff, format or mypy findings. On a local DuckLake (non-UTC session, composite keys): NULL VARCHAR with identity partitioning, NULL UUID, REAL, an upper-case UUID key, TIMESTAMPTZ by `year(k)` and DATE by `month(k)` each left one row per key after three merges; the reviewer additionally broke nothing with keys named `target`/`staged`, NULLs in partition columns, 2M-row tables and failed casts.

Independent Sonnet review: no blocking findings; finding 1 (auto-close) not reproduced, its process point kept (issue closed only after this docs commit); finding 2 deferred to [DUCKLKD-31](https://github.com/paulcaron16k/target-ducklake-definite/issues/31); finding 3 accepted (`50944fe`, collapse pinned by a test); finding 4 declined; finding 5 accepted (performance claim corrected to no regression). Follow-up review: no blocking or should-fix findings; two description nits fixed.

Upstream PR: not opened.

---

# DUCKLKD-47 Timestamps with a non-UTC offset fail staging unless convert_tz_to_utc is set

## Intent

Found verifying DUCKLKD-21 on 2026-10-17. The singer-sdk parses date-time strings into timezone-aware datetimes; one with a fixed non-UTC offset such as `+02:00` made `create_pyarrow_table` fail the whole batch with `ArrowInvalid: Cannot locate timezone '+02:00'`, because pyarrow looks a fixed offset up as a zone name. Only `convert_tz_to_utc` avoided it, so with the default setting any tap emitting offsets other than `Z`/`+00:00` couldn't load.

## Acceptance

- A batch with timezone-aware values of any fixed offset (+02:00, -07:00, +05:30) stages and loads with convert_tz_to_utc false and true.
- TIMESTAMPTZ columns store the correct instant; behaviour for naive values and DATETIME columns is documented and unchanged in meaning.
- A regression test reproduces the ArrowInvalid on the old code.
- No new ruff or mypy findings.

## Evidence

Squash-merged into `xflow_dev` as `f06c733` through fork PR #56 (branch `bug/DUCKLKD-47_non-utc-offsets`, deleted after merge). A batch column that holds any datetime with a fixed non-UTC offset (`datetime.timezone`, offset != 0) has every datetime rendered as ISO text DuckDB reads exactly, each in its own offset (UTC `...000000Z`; others `...+02:00`, or `+02:00:30` with seconds; naive without zone; a named zone's DST gap normalised through UTC; fixed offsets skip that round trip so year-range edges don't overflow). Columns without fixed offsets are left to pyarrow, so batches that loaded before stage byte-identically (0 differences over 6,000 random batches here and 16,000 cases in the reviewer's run). `convert_tz_to_utc` conversion runs before the rendering.

Meaning, documented in the README and the config schema: without `convert_tz_to_utc`, a timestamp column without time zone stores each value's local wall-clock time (offset dropped); with it, the UTC time; a `timestamptz` column (via `x-sql-datatype`) stores the correct instant either way; naive values load as given and a `timestamptz` column reads them in the session time zone.

`TestTimezoneOffsets` has 21 cases through the real sink lifecycle: three offsets in both modes into plain and `timestamptz` columns, a re-merge on an offset key, a batch mixing fixed-offset/UTC/Europe-Paris/naive values, unchanged naive/UTC/named-zone loading under session `America/New_York`, a VARCHAR key written by earlier versions, and five edge values (years 9999 and 1, a seconds offset, a local-mean-time named zone, and a named-zone value whose UTC normalisation overflows). On the pre-PR code 9 fail (the fixed-offset cases with the setting off, with the reported `ArrowInvalid`, and the edge cases); the compatibility cases pass on both. `xflow_dev` suite: 187 passed; no new ruff, format or mypy findings. On a local DuckLake (non-UTC session, `year(at_tz)` partition, `(id, at_tz)` key), mixed and edge-value batches merged to one row per key with correct instants in both modes.

Independent Sonnet review, four rounds. Round 1: no blocking; should-fix findings reproduced and fixed (UTC text had changed to `+00:00`, breaking existing VARCHAR keys; unchanged behaviour wasn't tested); re-verification of that fix then caught a regression of mine (a batch mixing a fixed offset with other zones raised `ArrowTypeError`), fixed by rendering such columns as a whole. Round 2: no blocking; year-range overflow and seconds offsets fixed, wording corrected. Round 3: no blocking or should-fix; the overflow-fallback test added, a sub-second-offset nit declined. Round 4: no findings.

Found, not fixed (pre-existing, filed): [DUCKLKD-55](https://github.com/paulcaron16k/target-ducklake-definite/issues/55) `convert_tz_to_utc` ignores unparsed date-time strings; [DUCKLKD-57](https://github.com/paulcaron16k/target-ducklake-definite/issues/57) without the setting, a mixed-zone column's wall-clock time depends on batch order. Also pre-existing and unchanged: with the setting on, a value whose UTC time leaves Python's year range fails the conversion.

Upstream PR: not opened.

---

# DUCKLKD-50 x-sql-datatype hints on anyOf properties are lost; invalid decimal hints produce invalid DDL

## Intent

Found by the independent Sonnet review of PR #46 (DUCKLKD-21) on 2026-10-17, findings 4 and 7. A hint on the outer object of an anyOf property was dropped by `flatten_schema`, which keeps only the chosen non-null branch, so the column got its default type: VARCHAR instead of UUID, DECIMAL(38,9) instead of DECIMAL(12,4). `decimal_hint` accepted precisions and scales DuckDB's DECIMAL can't represent. `decimal(5,10)` was silently clamped to DECIMAL(5,5). `numeric(0,0)` on a number crashed sink creation (pyarrow precision 0); on a string it rendered the invalid DDL `DECIMAL(0, 0)`.

## Acceptance

- A hint on an anyOf property, outer or in the non-null branch, sets the column type and the staging type.
- decimal/numeric hints with precision < 1 or scale > precision fall back to the default mapping with a warning instead of producing invalid or silently changed DDL.
- Regression tests fail on the old code.

## Evidence

Squash-merged into `xflow_dev` as `0bb2fe7` through fork PR #58 (branch `bug/DUCKLKD-50_anyof-and-invalid-decimal-hints`, deleted after merge).
- **Wrapper hint.** `flatten._with_wrapper_hint` copies the wrapper's `x-sql-datatype` into the chosen branch, unless the branch has a non-empty hint of its own, which wins. Only the hint is copied, so a wrapper `format` still leaves the column VARCHAR, and the caller's schema is not changed by the copy.
- **Decimal validation.** `decimal_hint` returns None unless `precision >= 1 and scale <= precision`, so the column keeps its default mapping and the SDK warns. Precision above 38 is still capped later.

Tests and verification:
- `TestSqlDatatypeHints` has new cases:
  - outer, inner, inner-wins and empty-inner hints;
  - the wrapper `format` guard;
  - the caller's schema left unchanged;
  - an unsupported wrapper hint;
  - three impossible decimal hints, and the valid edges `decimal(5,5)`, `decimal(5,0)`, `decimal(1,0)`;
  - sink creation with `decimal(0,0)`.

  The original fix's tests give 6 failures on the pre-PR code; the inner-hint cases already passed and are pinned.
- `xflow_dev` suite: 202 passed. No new ruff, format or mypy findings; `flatten_schema` complexity is unchanged.
- On a local DuckLake through the sink:
  - an outer-hinted UUID key created a UUID column and re-merged to one row;
  - outer `numeric(12,4)` stored exact values;
  - `decimal(0,0)` fell back to DECIMAL(38,9).
- An existing table created before this fix (VARCHAR, DECIMAL(38,9)) keeps its types and keeps merging once the hints apply. Only new tables get the hinted types.

Independent Sonnet review, two rounds:
- **Round 1:** no blocking findings.
  - Fixed: an empty branch hint hid the wrapper's (`e076a69`, failing test first); missing tests (`82f598c`); docstring wording (`b78057f`).
  - Verified on real DuckLake, no change: existing tables keep loading.
  - Declined, out of scope or file convention: the hint/JSON-type mismatch, the `anyOf` crash and in-place mutation, and the test lint convention.
- **Round 2:** nothing blocking; two optional test nits declined.

Found, not fixed (pre-existing, filed):
- [DUCKLKD-59](https://github.com/paulcaron16k/target-ducklake-definite/issues/59): a decimal hint above precision 38 caps the scale with it, so `numeric(50,45)` becomes DECIMAL(38,38) and any value of 1 or more fails.
- [DUCKLKD-60](https://github.com/paulcaron16k/target-ducklake-definite/issues/60): `flatten_schema` crashes on an all-null or untyped anyOf branch and mutates the caller's schema.
- [DUCKLKD-61](https://github.com/paulcaron16k/target-ducklake-definite/issues/61): a hint that conflicts with the JSON type gives different DDL and staging types.

Upstream PR: not opened.

---

# DUCKLKD-59 A decimal hint whose precision exceeds 38 caps the scale with it, leaving no integer digits

## Intent

Found by the independent Sonnet review of PR #58 (DUCKLKD-50) on 2026-10-17; reproduced. `number_precision_and_scale` capped the precision at 38, then clamped the scale to it. So `numeric(50,45)`, with 5 integer digits, became DECIMAL(38,38), with none. Any value of 1 or more failed the whole batch: staging raised `decimal.InvalidOperation`, and DuckDB rejected the cast as out of range. The DDL and the staging type share the helper, so both were affected, for hints and for schema `precision`/`scale` keys.

## Acceptance

- When the hinted precision exceeds 38, the capped type keeps the hint's integer digits where it can, dropping scale before integer digits. If the integer digits alone exceed 38, the choice (and a warning) is documented.
- DDL and staging stay identical.
- A regression test loads 12.5 into a numeric(50,45) column and fails on the old code.
- No new ruff or mypy findings.

## Evidence

Squash-merged into `xflow_dev` as `ac91dc2` through fork PR #62 (branch `bug/DUCKLKD-59_decimal-precision-cap`, deleted after merge).

`parquet_utils._capped_to_38` fits any precision above 38 into 38 digits:
- **Fraction digits are dropped first,** so the integer digits still fit: `numeric(50,45)` gives DECIMAL(38,33).
- **The scale never drops below `min(scale, 9)`,** the default scale, so a capped type never keeps fewer fraction digits than the DECIMAL(38,9) default. Examples: `numeric(40,2)` keeps DECIMAL(38,2); MySQL's maximum `decimal(65,30)` and `numeric(60,10)` give DECIMAL(38,9). Values with more integer digits than are left fail at staging.
- **A `UserWarning`** names the declared and the capped type.

This narrows the acceptance's "keep integer digits" on purpose. Applied strictly, it would turn `numeric(40,2)` money into DECIMAL(38,0) and lose the cents. Both reviews judged the floor sound.

A DUCKLKD-21 test had pinned the bug (`decimal(50,40)` → DECIMAL(38,38)); it now expects DECIMAL(38,28).

Tests and verification:
- New `TestDecimalPrecision` cases:
  - nine cap cases, for hints and keys, including `numeric(39,0)`, `numeric(39,39)` and keys with scale above precision;
  - a fitting hint emits no warning;
  - a DuckDB load test with `12.5` and a 5-integer-digit, 33-fraction-digit value, asserting the staging type equals the DDL type.
- On the old code, 8 fail, including the reported `InvalidOperation`.
- `xflow_dev` suite: 212 passed, with no warnings; no new ruff, format or mypy findings.
- On a local DuckLake, through the sink with a merge and a re-merge: columns DECIMAL(38,33), (38,2) and (38,9); values read back exactly; one row per key.

Independent Sonnet review, two rounds:
- **Round 1:** no blocking findings.
  - Fixed: missing edge tests and a staging/DDL assertion (`961a3f7`), and the docstring's failure point (`3cb648d`).
  - Pinned, no change: keys with scale above precision give (38,38), which is faithful to a declared type with no integer digits.
  - Declined: the warning naming the column (it would need a signature change), and negative scale from keys (pre-existing).
  - Held for the owner, then filed as a follow-up: existing tables.
- **Round 2:** nothing blocking; two test nits declined.

Found, not fixed (filed on the owner's decision): [DUCKLKD-63](https://github.com/paulcaron16k/target-ducklake-definite/issues/63). Staging uses the stream's declared scale, not an existing column's. A table created by upstream `main` as DECIMAL(38,30) from schema keys is now staged at (38,9), so small values lose fraction digits silently; upstream's float staging kept about 16 of them. Values of 1e8 or more, which failed upstream, now load.

Upstream PR: not opened.

---

# DUCKLKD-60 flatten_schema crashes on an all-null or untyped anyOf branch and mutates the caller's schema

## Intent

Found by the independent Sonnet review of PR #58 (DUCKLKD-50) on 2026-10-17; reproduced.
- **Crashes:** `flatten_schema` failed the stream at sink creation for any property without a usable type:
  - an all-null `anyOf` raised IndexError;
  - an `anyOf` whose first non-null branch had no `type` (a `$ref`, a nested `anyOf`, `{}`, a hint only) raised KeyError;
  - found while reproducing, from the same cause: a property with neither `type` nor `anyOf` (`format` or `$ref` alone) raised TypeError.
- **Mutation:** it also edited the caller's schema in place, deleting `description` and rewriting the chosen branch's `type` to include null.

## Acceptance

- An all-null anyOf and an untyped branch produce a column with a documented fallback type (or a clear error naming the property) instead of IndexError/KeyError.
- flatten_schema leaves its input schema unchanged.
- Regression tests fail on the old code; existing flattening output is unchanged for schemas that loaded before.
- No new ruff or mypy findings.

## Evidence

Squash-merged into `xflow_dev` as `5e8461c` through fork PR #64 (branch `bug/DUCKLKD-60_flatten-anyof-crash`, deleted after merge).

**Flattening.**
- A property with no usable type becomes `UNTYPED_COLUMN` (`["null", "object"]`), a JSON column that keeps any value as JSON text. That covers untyped `anyOf` branches, `format` or `$ref` alone, and boolean schemas.
- An `anyOf` takes its first typed, non-null branch.
- `flatten_schema` works on copies and returns a deep copy, so nothing reaches the caller's schema.
- Output is identical for every property that flattened before. The reviewer's differential fuzz over 60,000 random schemas found 0 differences wherever the old code returned; the old code crashed on about 47% of the calls.
- `flatten_schema` complexity dropped from 19 to 13.

**Validation.** The sink's `self.schema` is what the SDK validates records against when `validate_records` is on. It is off by default in this target, so this matters only to streams that enable it. The old mutation had changed validation:
- `anyOf`/`oneOf` properties without a null branch accepted null;
- `oneOf: [string, null]` rejected a valid null, because null then matched two branches.

`ducklakeSink.get_validator` now validates against `nullable_choices(self.schema)`. That is a copy in which every `anyOf`/`oneOf` property also accepts null, at any depth. It accepts everything the old validation accepted, plus the valid `oneOf` nulls; only null is added. `self.schema` stays as the tap sent it.

**Tests and verification.**
- `TestUntypedAndChoiceProperties` covers:
  - each untyped shape;
  - JSON column loads of every value kind;
  - an input left unchanged and sharing nothing;
  - compatibility pins at `max_level` 0 and 1 with auto-cast, whose expected output is the old code's;
  - null acceptance through `_validate_and_parse`;
  - invalid values still rejected;
  - `validate_records: false`.
- On the old code, 16 of the original 18 fail; the 2 compatibility pins pass on both.
- `xflow_dev` suite: 236 passed. No new ruff, format or mypy findings.
- On a local DuckLake, through `_validate_and_parse` and the sink with a merge and a re-merge: the `$ref` and all-null `anyOf` columns are JSON; the choice properties' nulls validate; there is one row per key. The old code failed with TypeError.

Independent Sonnet review, two rounds:
- **Round 1:** no blocking findings.
  - Fixed: columns shared nested values with the input and `UNTYPED_COLUMN` (`7436ede`, deep copy); boolean property schemas crashed (`d39763c`); test gaps (`ef31aef`).
  - Documented and pinned (`222800d`): null acceptance for any `anyOf`/`oneOf` property.
  - Declined, filed as a follow-up: properties still left out.
- **Round 2:** nothing blocking; three nits declined.

Noted, unchanged: a null in a JSON column is stored as the JSON text `null`, as for every object column.

Found, not fixed (filed): [DUCKLKD-65](https://github.com/paulcaron16k/target-ducklake-definite/issues/65). Properties declared with `oneOf`, `allOf` or `{}` are silently left out of the table, and their values are lost. Only `oneOf` whose first branch is string, array or object keeps a column.

Upstream PR: not opened.

---

# DUCKLKD-65 Properties declared with oneOf, allOf or {} are silently left out of the table, and their values are lost

## Intent

Found by the independent Sonnet review of PR #64 (DUCKLKD-60) on 2026-10-18, finding 5; reproduced and found wider. A property with neither `type` nor `anyOf` took `flatten_schema`'s third path. That path looked only at the first branch of its first key, and kept the column only when that branch's type was exactly `string`, `array` or `object`. Otherwise the property got no column, and its values were dropped at staging with no error. Affected: `{}`, `oneOf [integer, null]`, `oneOf` with the null branch first, `oneOf` with a type list, and `allOf [number]`.

## Acceptance

- oneOf and allOf properties are flattened like anyOf: the first typed, non-null branch gives the column, with type lists handled and null branches skipped.
- {} (any value) gives the JSON column (UNTYPED_COLUMN), as untyped properties do since DUCKLKD-60.
- Existing tables gain the new columns through the normal add-column path; nothing already loaded changes type. The PR states this table-shape change.
- Regression tests show each case's column and values on the old code missing.
- No new ruff or mypy findings.

## Evidence

Squash-merged into `xflow_dev` as `6ca7032` through fork PR #67 (branch `bug/DUCKLKD-65_oneof-allof-columns`, deleted after merge), together with [DUCKLKD-66](https://github.com/paulcaron16k/target-ducklake-definite/issues/66).

**The fix.** One `_choice_column(prop, keyword)` serves `anyOf`, `oneOf` and `allOf`:
- the first branch with a non-null type gives the column;
- null-only, untyped and non-dict branches are skipped;
- the wrapper's `x-sql-datatype` hint is carried.

A property with no type information at all (`{}`, a description only) is the JSON column. `allOf` uses its first typed branch; branches are not merged.

**Behaviour changes, stated in the PR:**
- Properties that were dropped get columns, and existing tables gain them through `prepare_table`'s add-column path.
- `oneOf`/`allOf` columns that DUCKLKD-60 (not deployed) made JSON now take their branch's type.
- `oneOf`/`allOf` wrapper hints are honoured.
- An `anyOf` branch whose type is the list `["null"]` is skipped like `{type: "null"}`. It used to give VARCHAR and hide a later typed branch.

**Tests and verification:**
- New tests:
  - seven column cases (column, DDL and staging);
  - staged values;
  - the type-order test of DUCKLKD-66.
- On the old `flatten.py`, 11 fail.
- `xflow_dev` suite: 247 passed. No new ruff, format or mypy findings; `flatten_schema` complexity is 12.
- A differential check against `xflow_dev` over 20,000 random schemas:
  - 0 lost columns and 9,078 new ones;
  - every other difference is intended: a type now taken from a oneOf/allOf branch, a carried wrapper hint, the `["null"]`-branch case, or type order.
- On a local DuckLake: the old code created the stream's table with only `id`. The new code added `amt DECIMAL(38,9)`, `any JSON`, `n BIGINT` and `s UUID`, and loaded their values.

Independent Sonnet review, two rounds:
- **Round 1:** no blocking findings. Fixed: the subprocess test's `cwd` (`7d22962`). Declined:
  - allOf null validation, which correctly rejects null;
  - a malformed `{type: 5}`, which is invalid JSON Schema and crashed before too.
  - With `validate_records` off, the default, it reaches `flatten_schema`; the first reply's reason was corrected on the PR.
- Three findings filed as follow-ups.
- **Round 2:** nothing blocking.

Found, not fixed (pre-existing, filed):
- [DUCKLKD-68](https://github.com/paulcaron16k/target-ducklake-definite/issues/68): a column with several JSON types is staged at its first type while the DDL is VARCHAR, so `'abc'` in an `[integer, string]` column fails the batch.
- [DUCKLKD-69](https://github.com/paulcaron16k/target-ducklake-definite/issues/69): `auto_cast_timestamps` ignores `anyOf`/`oneOf`/`allOf` properties.
- [DUCKLKD-70](https://github.com/paulcaron16k/target-ducklake-definite/issues/70): a null in a JSON column is stored as the JSON text `null`.

Upstream PR: not opened.

---

# DUCKLKD-66 An anyOf branch with several types gets a staging type that changes from run to run

## Intent

Found while starting DUCKLKD-65 on 2026-10-18; reproduced. `_anyof_column` built the column's type list with `list(set([...]))`, whose order depends on Python's per-process string hashing (PYTHONHASHSEED). Staging uses the first non-null type. So `anyOf [{type: [integer, string]}, {type: null}]` staged as int64 under seeds 1, 4 and 6, and as string under 2, 3 and 5, while the DDL was VARCHAR every time. A non-numeric value loaded in one run and failed in the next.

## Acceptance

- A choice branch's type list keeps its declared order (null first, then the branch's non-null types), as a plain type list does, so the column and staging types are the same in every process.
- A regression test runs the flattening under several PYTHONHASHSEED values and gets one result.
- No new ruff or mypy findings.

## Evidence

Fixed in the same squash as DUCKLKD-65 (`6ca7032`, fork PR #67), because the shared `_choice_column` would otherwise have carried the random order into `oneOf` and `allOf`. The column type is `["null", *branch types in declared order]`. `test_branch_type_order_is_the_same_in_every_process` runs the flattening for `anyOf`, `oneOf` and `allOf`, each in fresh interpreters under PYTHONHASHSEED 1–8. All three give one result, `["null", "integer", "string"]`. On the old code they fail; the `anyOf` case gave five different orders across the eight seeds.

What remains is filed as [DUCKLKD-68](https://github.com/paulcaron16k/target-ducklake-definite/issues/68): staging at the first type while the DDL is VARCHAR, which now fails consistently for integer-first lists.

Upstream PR: not opened.

---

# DUCKLKD-70 A null in a JSON (object or array) column is stored as the JSON text null, not SQL NULL

## Intent

Found by the independent reviews of PR #64 and PR #67 on 2026-10-18. `flatten_record` JSON-dumped every value of an object, array or untyped column, including None, so an explicit null was stored as the text `'null'` (`c IS NULL` false) while a missing key gave SQL NULL.

## Acceptance

- None in an object, array or untyped (JSON) column is stored as SQL NULL; a null inside a value, such as `{"a": null}`, is unchanged.
- The PR states the change for existing rows, with SQL to convert them.
- A regression test checks `c IS NULL` through staging and DuckDB and fails on the old code; no new ruff or mypy findings.

## Evidence

Squash-merged into `xflow_dev` as `49d221e` through fork PR #71. None is never JSON-dumped; values containing a null, and the string `"null"`, are dumped as before. Existing rows keep `'null'`; to convert them: `UPDATE lake.schema.tbl SET c = NULL WHERE c::VARCHAR = 'null';` (checked on a DuckLake table loaded by the old code).

`test_none_in_json_column_is_sql_null` (object, array, untyped, `{}`) and the updated untyped-values test: 5 fail on the old code. Real DuckLake: the old code stored `'null'` for explicit nulls and NULL for missing keys; the new code stores NULL for both. Independent Sonnet review: no blocking findings; nit 1 fixed (test cases name their inner value), nit 2 answered (NULL keys match NULL since DUCKLKD-49). Re-review: no findings.

Upstream PR: not opened.

---

# DUCKLKD-69 auto_cast_timestamps ignores properties declared with anyOf, oneOf or allOf

## Intent

Found by the independent review of PR #67 on 2026-10-18. The auto-cast check ran only in `flatten_schema`'s `type` branch, so `created_at: {anyOf: [string, null]}` stayed VARCHAR with `auto_cast_timestamps` on.

## Acceptance

- With the setting, a choice property resolving to a string without a format, named in `TIMESTAMP_COLUMN_NAMES`, gets format date-time; off, nothing changes.
- The PR states that new tables get TIMESTAMP where they got VARCHAR and existing tables keep VARCHAR.
- A regression test fails on the old code; no new ruff or mypy findings.

## Evidence

Squash-merged into `xflow_dev` as `e5445b9` through fork PR #72. `_leaf_column` resolves every non-flattened property, and the auto-cast check runs once on the resolved column; a `format` on a choice wrapper also stops it. `flatten_schema` lost its C901/PLR0912 findings. Mixed choices follow their first branch, as plain type lists do (pinned by a test, stated in the PR).

The 4 auto-cast cases fail on the old code; the skip and name cases pin unchanged behaviour. Real DuckLake: anyOf/oneOf `created_at`/`updated_at` became TIMESTAMP; an existing all-VARCHAR table kept VARCHAR and loaded. Independent Sonnet review: no blocking findings; nit 1 declined (the wrapper-format check is not redundant), nit 2 fixed (mixed-choice pin and description). Re-review: no findings.

Upstream PR: not opened.

---

# DUCKLKD-53 Existing columns whose name differs only in case are re-added, failing setup

## Intent

Found reproducing a review finding on PR #52 on 2026-10-17. `prepare_table` compared column names case-sensitively, so a table with `"ID"` receiving a stream property `id` ran `ADD COLUMN "id"` and failed setup with `Column with name id already exists!`.

## Acceptance

- `prepare_table` and the type widening match existing columns case-insensitively; loading and merging into such a table succeeds.
- A regression test reproduces the Catalog Error on the old code.

## Evidence

Squash-merged into `xflow_dev` as `4b3880a` through fork PR #73. `_by_lower_name` serves `prepare_table` and `_reconcile_column_types` (widening issued against the table's own name). The INSERT column list (`_build_columns_sql`) matched case-sensitively too and would have filled `"ID"` with NULL once setup succeeded; it is fixed in the same PR.

`TestCaseInsensitiveColumns` (merge and append lifecycles, widening, INSERT columns): all 4 fail on the old code, 2 with the reported Catalog Error. Real DuckLake against an upper-case table: setup succeeded, only the new column was added, one row per key. Independent Sonnet review: no blocking or should-fix findings; three nits needed no change.

Upstream PR: not opened.

---

# DUCKLKD-55 convert_tz_to_utc doesn't convert unparsed date-time strings

## Intent

Found fixing DUCKLKD-47 on 2026-10-17. The singer-sdk parses only top-level date-times it recognises, so nested values, top-level oneOf/allOf properties and auto-cast strings reached staging as text, and `convert_tz_to_utc` skipped them: a timestamp column stored the wall-clock time.

## Acceptance

- With the setting, date-time text with an offset is converted to UTC like a parsed value, top-level and flattened; without it, nothing changes.
- Unparseable strings keep today's behaviour; a regression test reproduces the wall-clock value on the old code.

## Evidence

Squash-merged into `xflow_dev` as `472e2bc` through fork PR #75. `_normalize_datetime` parses text with `datetime.fromisoformat` (the SDK's parser); text with an offset becomes UTC text (`...Z`). Naive, unparseable and out-of-range text is left as it is.

Tests through the real sink and the SDK's own parsing, for nested, oneOf and auto-cast shapes: the 9 offset cases fail on the old code; 11 others pin unchanged behaviour. Real DuckLake: before, only the top-level value was converted; after, all four. Independent Sonnet review: no blocking findings; nit 2 fixed (the `try` covers only parsing), nit 1 declined (UTC text matches DUCKLKD-47's). Re-review: no findings.

Upstream PR: not opened.

---

# DUCKLKD-57 Without convert_tz_to_utc, a timestamp column's wall-clock time depends on batch order

## Intent

Found fixing DUCKLKD-47 on 2026-10-17. pyarrow builds a datetime column in its first value's zone, so a batch mixing zones stored order-dependent wall-clock times. Found while fixing: a batch mixing naive and aware values (`...T10:00:00` and `...Z`, the case real taps produce) was order-dependent for TIMESTAMPTZ columns too, with the setting on or off.

## Acceptance

- With `convert_tz_to_utc` off, a plain timestamp column stores each value's own wall-clock time regardless of the batch.
- TIMESTAMPTZ columns and the setting on are unchanged; a regression test reproduces the order dependence on the old code.

## Evidence

Squash-merged into `xflow_dev` as `3798b52` through fork PR #76. A column whose datetimes have more than one `tzinfo` (naive counts as one) is rendered as text, each value in its own offset (`_mixes_zones`, alongside DUCKLKD-47's fixed-offset rule); one-zone columns are left to pyarrow. **Beyond the Acceptance as written:** naive+aware batches now store correct TIMESTAMPTZ instants, with the setting on and off, because they were wrong before; every other batch is unchanged.

7 cases fail on the old code (6 orders; naive+UTC with the setting on); 3 pin unchanged behaviour. Real DuckLake, values sent as JSON text in both orders: the old code stored the UTC value 5 hours off (naive first) or read the naive value as UTC (UTC first); the new code gives the same, correct result in both orders. Independent Sonnet review: no blocking findings; nit 1 fixed (TIMESTAMPTZ pinned with the setting on, Acceptance difference stated), nit 2 answered in the description. Re-review: no findings.

Found, not fixed (pre-existing, filed): [DUCKLKD-79](https://github.com/paulcaron16k/target-ducklake-definite/issues/79). With a sqlite catalog, two streams draining in parallel fail the run (`database is locked` at commit, often followed by an abort); old and new code behave the same, and `parallel_draining: false` avoids it.

Upstream PR: not opened.

---

# DUCKLKD-68 A column with several JSON types is staged at its first type while the table column is VARCHAR

## Intent

Found by the independent review of PR #67 on 2026-10-18. For `[integer, string]`, the DDL is VARCHAR but staging was int64, so `'abc'` failed the batch; `[number, string]` staged a decimal.

## Acceptance

- A column with more than one non-null type is staged at its DDL type, so every value it can hold stages; DDL and staging agree for every combination (parametrized), and single-type columns are unchanged.
- A regression test loads `'abc'` into `[integer, string]` and fails on the old code; no new ruff or mypy findings.

## Evidence

Squash-merged into `xflow_dev` as `2c668e9` through fork PR #74, together with [DUCKLKD-61](https://github.com/paulcaron16k/target-ducklake-definite/issues/61) (same root cause: staging did not follow the DDL). `staging_type(column_type)` maps each DDL type to its parquet type, and the sink builds `staging_schema(self.ducklake_schema)`; `_field_type_to_pyarrow_field`, `flatten_schema_to_pyarrow_schema` and `is_float_hint` were removed. `_coerce_column` coerces values to the staging type:
- text columns render scalars as the text they store (`true`, `1.50`);
- int64/float64 columns read number text as DuckDB's cast did (`' 7 '`, `'+5'`, `'1e2'`, `'12.0'`);
- int64 columns keep whole numbers exact and raise a `ValueError` naming the column for fractions, non-finite and out-of-range values (rejected on magnitude, so `'1e1000000'` doesn't stall).

A differential check over 20,000 random columns found differences only for multi-type columns and conflicting hints. New multi-type numbers in existing VARCHAR columns are stored as their exact text (`1.50`, not `1.500000000`).

`TestStagingFollowsColumnType` (55 cases at merge); 40 of the original 44 fail on the old code. Real DuckLake: the old code failed the batch on `'n/a'`; the new code loaded every multi-type and hinted column. Independent Sonnet review, five rounds: round 1 should-fix (integer-hinted text parsed more strictly than DuckDB) fixed, with three nits; rounds 2–4 one nit each (out-of-range errors, a huge-exponent stall, zero with a large exponent), all fixed; round 5: no findings.

Upstream PR: not opened.

---

# DUCKLKD-61 A hint that conflicts with the JSON type gives different DDL and staging types

## Intent

Found by the independent review of PR #58 on 2026-10-17. The DDL followed the hint and staging the JSON type: string+`numeric(12,4)` created DECIMAL(12,4) but staged text; integer+`uuid` created UUID but staged int64, which no value could be loaded into.

## Acceptance

- Decide and document the rule for a hint incompatible with the JSON type; DDL and staging agree under it for each pair.
- A test pins the rule for string+decimal and integer+uuid; no new ruff or mypy findings.

## Evidence

Fixed in the same squash as DUCKLKD-68 (`2c668e9`, fork PR #74). **Rule:** a hint applies when the property's values can be loaded into it, and staging then follows it; otherwise it is ignored with a warning and the JSON type decides (`type_hints.hint_fits_json_type`). A `string` takes any supported hint; `integer`, `number` and `boolean` take numeric hints and `json`/`jsonb`; `object` and `array` take only `json`/`jsonb`. Documented in the README's *Column types* section. Ignored hints change only new tables (integer+uuid now creates BIGINT).

Upstream PR: not opened.

---

# DUCKLKD-63 Decimals are staged at the stream's declared scale, not an existing column's, so they are rounded silently

## Intent

Found by the independent review of PR #62 on 2026-10-17; filed on the owner's decision. A table created by upstream `main` as DECIMAL(38,30) was staged at (38,9), so `1.123456789012345678901234567890` was stored as `1.123456789`, silently. Found while fixing: an existing DOUBLE column fed by a number stream stored `1.5e-12` as 0.

## Acceptance

- When the table exists, each decimal column is staged at the column's own (precision, scale), read from the table.
- A value that doesn't fit fails loudly or is reported, never rounded silently; new tables are unchanged.
- A regression test loads the 30-digit value into an existing DECIMAL(38,30) column through the sink and fails on the old code.

## Evidence

Squash-merged into `xflow_dev` as `aac6968` through fork PR #77. `prepare_table` returns the table's column types from the PRAGMA it already ran, and the sink stages a number column at an existing DECIMAL, REAL or DOUBLE column's type (`_staged_columns`). Merge-key types come from the same read, so merges issue one query fewer. `_decimal_column` rounds each value once:
- values rounded to the column's scale are reported with a warning (`1 value(s) in column 'd2' had more than 2 fraction digits and were rounded to fit DECIMAL(10, 2).`);
- a value with too many integer digits, a non-finite number or non-number text raises a `ValueError` naming the column and DECIMAL(p, s).

Tests through the real sink against existing tables; the DECIMAL(38,30), DOUBLE, REAL and rounding-report cases fail on the old code. Real DuckLake: before, `d30` lost 21 digits and `dbl` stored 0; after, both are exact, with the warning for `d2`. Independent Sonnet review, three rounds: round 1 should-fix (bare `InvalidOperation` for wide columns) and two nits fixed; round 2 should-fix (error message built on every value) and a nit fixed; round 3: no findings.

Upstream PR: not opened.

---

# DUCKLKD-51 Support more x-sql-datatype spellings and stop warning on hints that match the default

## Intent

Raised by the independent review of PR #46 on 2026-10-17 (findings 5, 6 and 8). Common source spellings fell back with an SDK warning; hints matching the default mapping warned on every sink creation; the DECIMAL-to-DOUBLE behaviour for existing tables was undecided.

## Acceptance

- The listed spellings map to the matching DuckDB type, or are documented as deliberately unsupported; hints equivalent to the default mapping don't warn.
- The DECIMAL-to-DOUBLE behaviour for existing tables is decided and documented; tests cover each new spelling.

## Evidence

Squash-merged into `xflow_dev` as `ab371be` through fork PR #78. New mappings:
- `decimal(p)` is DECIMAL(p,0), and unsized `numeric`/`decimal` take the default decimal mapping;
- temporal precisions are ignored (`timestamp(3) with time zone`);
- SQL Server `datetime2`/`smalldatetime`/`datetimeoffset`;
- MySQL display widths, `mediumint` and `unsigned` ranges, with `bigint unsigned` → DECIMAL(20,0); `unsigned` is ignored on decimals and floats.

Text-type hints (including `varchar(max)`) are silent on string properties, and `boolean`/`bool` on boolean properties; elsewhere they warn. Deliberately unsupported: `tinyint`, `datetime`, `timetz`/`time with time zone`, `bit`, binary types.

**Decision:** an existing DECIMAL column is never changed to DOUBLE; a float hint on it stores values at its scale, with DUCKLKD-63's rounding warning. Recreate the column to store doubles. Existing columns also keep their stored values under the new mappings: a string property into an existing text column is staged as text, and a whole-number decimal into an existing integer column as int64. All of this is documented in the README.

New tests: 35 of the original 41 fail on the code without the change; the 13 added for the review fail on its first commit. Real DuckLake: all new spellings created the expected columns and loaded exactly, with no hint warning. Independent Sonnet review, two rounds: round 1 three should-fix (existing VARCHAR columns, default hints on other JSON types, `varchar(max)`) and two nits, all fixed; round 2: no findings.

Upstream PR: not opened.

---

# DUCKLKD-79 Parallel draining with a SQLite catalog fails the run with 'database is locked' and aborts the process

## Intent

Found verifying DUCKLKD-57 on 2026-10-18. With `catalog_type: sqlite`, any run with two or more streams failed under the default config. All sinks share one DuckDB instance and the SDK drains them in parallel threads, so two DuckLake commits ran at once and SQLite, which allows one writer, failed the second with `database is locked`. The run failed with one table empty, often aborting with exit 134.

## Acceptance

- Decide and document how a SQLite catalog (and any other single-writer catalog) handles concurrent sinks; check the duckdb catalog type too.
- A run with several streams into a SQLite catalog loads every stream with the default config and exits cleanly.
- If a commit still fails, the process exits with a clear error, not an abort.
- A regression test with two parallel sinks on a SQLite catalog fails on the old code; the README documents the rule.

## Evidence

Squash-merged into `xflow_dev` as `ebb2e28` through fork PR #80.
- **Decision: SQLite catalogs drain one sink at a time.** `Targetducklake._drains_in_parallel()` ignores `parallel_draining` for `catalog_type: sqlite`, with an info line; the README and the config schema say so. The duckdb catalog type was checked and needs no change (its catalog lives in the same DuckDB instance; 3 of 3 two-stream runs loaded). Postgres and MySQL accept concurrent writers and were not tested.
- **No abort on any catalog.** Found by the review: the SDK's parallel drain raised the first failure without waiting for the other drains, and the process aborted (a duckdb catalog with one bad record: exit 134 in 4 of 10 runs). `Targetducklake._drain_all` now waits for every drain in a `ThreadPoolExecutor`, logs each failure and raises the first. After the fix: exit 1 in 10 of 10, with the error.
- A `database is locked` retry, in the first version, was dropped: no real lock reproduced the error, and serialising removes the cause.

**Root causes, verified after the merge (2026-10-20).**
- **Why SQLite failed at once.** A plain `sqlite3` script, with no DuckDB and a 5 s busy timeout on every connection (rollback-journal mode, as DuckLake's catalog uses), showed:
  - a writer behind a plain lock waited 1.05 s, then succeeded;
  - two transactions that both read and then wrote: the second failed with `database is locked` after 0.00 s;
  - with `BEGIN IMMEDIATE`, or a retry of the losing transaction, both committed.

  This is SQLite's documented deadlock avoidance, not a SQLite bug. Through DuckLake, two threads committing in parallel failed the same way after about 0.04 s, though DuckDB sets a ~5 s busy timeout, and a retry of the losing commit made every row load. So the contention is recoverable; #80 removes it by draining SQLite sinks one at a time.
- **A failed SQLite `COMMIT` can wedge the catalog attachment.** When the failure is the SQLite `COMMIT` itself (`Failed to execute query "COMMIT": database is locked`), later statements on that DuckDB connection's catalog fail with `database is locked` too. This happened in 5 of 12 runs of the two-thread experiment. Another process can still take the write lock, so no file lock is held; rolling back or closing the failed cursors doesn't help (`no transaction is active`). Only `DETACH` and re-`ATTACH`, or a new DuckDB connection, recover. This looks like DuckLake's SQLite connection keeping the transaction that SQLite leaves open after a busy `COMMIT`. It is unreachable in-process since #80; filed as [DUCKLKD-82](https://github.com/paulcaron16k/target-ducklake-definite/issues/82).
- **Why the parallel drain aborted.** A gdb capture of the abort, on the code before #80 (duckdb catalog, one bad record), shows the main thread in `Py_FinalizeEx` and the three aborting threads inside Polars' `from_arrow` (the in-batch dedup). They returned to Python during shutdown, and CPython 3.12 ended them with `pthread_exit`, which can't unwind through Rust frames, so `abort`. The SDK's joblib drain raises the first failure without waiting for the others; waiting for every drain (#80) avoids it. The stream failure itself was a genuinely bad record. The `_drain_all` docstring was corrected to match.

`TestSqliteCatalogDraining`: drain parallelism per catalog type, three streams through `listen()` into a real SQLite catalog, a failing sink waiting for a slower one, and every failure reported. The three-stream test fails 3 of 3 on the old code, and the drain-wait test fails under the SDK's drain. Real DuckLake through the CLI: the issue's sqlite repro failed 5 of 5 before and passed 8 of 8 after. Independent Sonnet review, three rounds: round 1 two should-fix (the abort outside sqlite, the unproven retry) and a nit, all addressed; round 2 three nits addressed; round 3: no findings.

Upstream PR: not opened.

---

# DUCKLKD-20 Make overwrite atomic and honour ACTIVATE_VERSION

## Intent

Overwrite truncated the table at setup and refilled it batch by batch, so readers saw an empty or partial table for the whole run, and a failed run lost the old rows. ACTIVATE_VERSION was only logged, so full-table taps never removed rows that disappeared at the source.

## Acceptance

- During an overwrite, readers see the old table until the new one is complete.
- ACTIVATE_VERSION full refreshes remove rows absent from the new version.
- The swap is one DuckLake transaction; a failed run leaves the old data intact.

## Evidence

Squash-merged into `xflow_dev` as `87f8e9f` through fork PR #83.

**Atomic overwrite** (`load_method: overwrite`, or `overwrite_if_no_pk` without keys). There is no truncate at setup. Each batch's Parquet file is staged in `temp_file_dir`, and `clean_up` runs `connector.replace_table_contents`: `DELETE` plus `INSERT … FROM read_parquet([files])` in one transaction. `Targetducklake._write_state_message` holds STATE while any overwrite sink hasn't committed, since mid-stream drains also write STATE. The SDK writes the latest STATE after the final clean-up.

**ACTIVATE_VERSION.** `Targetducklake._process_activate_version_message` drains the sink, then `activate_version` retires rows whose `_sdc_table_version` is not the version, unversioned rows included. **Decided with the owner: SDK semantics.** By default `_sdc_deleted_at` is set (current UTC time) and the rows are kept, revived when sent again; with `hard_delete: true` they are deleted. Nothing is retired:
- without `add_record_metadata`, with a warning;
- for a version announced before its records (a second `ACTIVATE_VERSION` with no records between ends the sync, so an emptied source retires every row);
- for overwrite streams.

**Design choice.** A single DuckLake transaction kept open across batches would write once and need no local disk. But on a SQLite catalog an open transaction blocked every other commit (3 of 3 runs), so batches are staged locally and swapped in one short transaction. `temp_file_dir` must hold the whole overwrite stream; worth reconsidering at the Postgres retest. Documented limits:
- the swap happens at the end of the input (a tap dying with a clean EOF swaps in what was received);
- a mid-stream schema change fails the run at the end with the old rows intact (as before, though the old code failed midway);
- a held STATE delays every stream's checkpoint.

`TestAtomicOverwrite` and `TestActivateVersion` (real sink lifecycle; `listen()` on a real SQLite catalog), 14 tests. On the old code, 8 of the original 11 fail. Real DuckLake through the CLI:
- a failing overwrite left the 1000 old rows (the old code left the failed run's 500);
- the full-table resync marked the vanished row deleted by default and removed it with `hard_delete` (the old code kept it unmarked).

Independent Sonnet review, two rounds. Round 1: one blocking finding (STATE before the overwrite committed) and two should-fix (an emptied source; hard delete by default), all fixed, the third by the owner's decision; two nits documented. Round 2: nothing to change; one README note added.

Upstream PR: not opened.

---

# DUCKLKD-38 Sort keys and bucket partitions

## Intent

DuckLake 1.5.2 supports `SET SORTED BY` and `bucket(N, col)` partitions; the target offered only identity and time partitions, through a top-level `partition_fields` that was also re-applied on every run.

## Acceptance

- Per-stream config sets sort keys and bucket partitions.
- Tests cover the generated DDL.

## Evidence

Squash-merged into `xflow_dev` as `72a97db` through fork PR #93. Preceded by the two-way parity evaluation ([target-parity-ducklake-iceberg.md](target-parity-ducklake-iceberg.md), `674c313`).

**Config.** Owner decision: target-iceberg's shape, `stream_defaults` plus `stream_overrides.<stream>`, merged key by key:
- `partition: [{column, transform, num_buckets}]`, with transforms identity, year, month, day, hour and bucket;
- `sort_order`: column names with optional `ASC`/`DESC` and `NULLS FIRST`/`LAST`;
- both accept JSON strings.

**Validation** (`layout.py`). Every rejection names the fix. Rejected:
- unknown keys and transforms, and `truncate`/`void` (not in DuckLake);
- a bucket without `num_buckets`, and misplaced `num_buckets`/`width`;
- malformed sort entries;
- columns the stream lacks;
- target-iceberg's `write_mode`/`key_properties` (pointing to DUCKLKD-41).

`check_layout_config` checks every stream's settings at startup; columns are checked per stream.

**Applied on change.** The current layout is read from DuckLake's metadata tables, whose schema is looked up through `duckdb_tables()`. A layout is altered only when it differs: an unchanged `SET PARTITIONED BY` adds a snapshot, and an unchanged `SET SORTED BY` adds a sort version too. An empty list resets the keys. The older `partition_fields` maps to the same keys. CLI: three runs with `partition_fields` gave +2 snapshots per run before and +1 after. DuckLake writes rows in the configured sort order: 200 ascending ids were stored descending.

`TestTableLayout` (30 tests): DDL, merging, legacy mapping, rejections, the startup check, and runs through `listen()` on a real SQLite catalog. Independent Sonnet review, three rounds:
- round 1: two should-fix (quoted sort names re-applied every run; an empty list emitted invalid SQL) and three nits, all fixed;
- round 2: three nits, fixed;
- round 3: no findings.

**Postgres retest:** that `duckdb_tables()` finds the metadata tables with `META_SCHEMA`, and that sort expressions come back as on SQLite (otherwise one extra snapshot per run).

Upstream PR: not opened.

