# target-ducklake and target-iceberg: Feature Parity

As of 2026-10-21 · Paul Caron

This is a two-way capability comparison of the two xflow Singer targets: what each one does, what the other lacks, and where they behave differently for the same tap. It replaces the one-way B list in [target-ducklake-gap-list.md](target-ducklake-gap-list.md) for anything comparing the two targets, and was done before DUCKLKD-38 so the comparison could shape its config design.

**Compared:**
- target-ducklake `xflow_dev` at `5a5a7c2`: DuckDB 1.5.2, singer-sdk 0.46.4.
- target-iceberg (`~/checkout/xflow.ai/target-iceberg`) at `ff0b9c2`: PyIceberg 0.12.0, singer-sdk 0.54.7.

Each side was inventoried from its code against the same checklist, with `file:line` evidence. A config key that is declared but never read counts as "declared only".

**Status key:** ✅ supported · ◐ partial · ⚠ declared only (accepted in config, not implemented) · ✗ not supported.

## Summary

- **target-ducklake gaps relative to target-iceberg.** 17 already had board issues. This evaluation filed 9 more, DUCKLKD-84 to 92. One is a bug: an `x-singer.decimal` string without precision is silently rounded to 3 places (DUCKLKD-84, fixed 2026-10-26: such a string is now VARCHAR, as in target-iceberg).
- **target-iceberg gaps relative to target-ducklake.** These are recorded here and not filed: issues go on our board only.
  - three per-stream settings that are accepted but never applied (`sort_order`, `write_mode`, `key_properties`);
  - no `anyOf`/`oneOf`/`allOf`;
  - no handling of nullable keys;
  - STATE emitted while a full refresh is still staged;
  - no transient-error retry.
- **Divergences that matter for xflow.** The same tap gives different tables in the two targets:
  - timestamps (TIMESTAMP vs timestamptz);
  - column names (raw vs conformed);
  - `hard_delete` semantics and default;
  - schema routing;
  - the batch-size setting's name.

## Two-way matrix

| Area | Capability | target-ducklake | target-iceberg | Gap / board |
| --- | --- | --- | --- | --- |
| **Load** | append | ✅ per-batch transaction | ✅ one snapshot per batch | — |
| | upsert / merge | ✅ DELETE+INSERT, keys cast to the column type | ✅ `table.upsert`, copy-on-write by default | — |
| | atomic overwrite / full refresh | ✅ staged batches, one replace transaction at the end; STATE held until commit | ✅ staging branch, one pointer swap; STATE can be emitted before the swap | Iceberg: STATE before swap |
| | per-stream load method / keys | ✗ | ⚠ `write_mode`, `key_properties` declared, never read | Both · DUCKLKD-41 |
| **Deletes** | ACTIVATE_VERSION | ✅ retires other versions (soft by default, `hard_delete` removes); needs `add_record_metadata` | ✅ branch staging with either message placement | — (different models) |
| | `_sdc_deleted_at` row deletes (CDC) | ✗ stored as a column | ◐ kept and warned, not applied | Both · DUCKLKD-23 |
| | delete strategies (copy-on-write, merge-on-read) | n/a: DELETE+INSERT | ✅ per stream, merge-on-read gated by an acknowledgement | — |
| **Keys** | in-batch dedup, last wins | ✅ append and merge | ✅ upsert only | — |
| | composite keys | ✅ per-column match | ✅ | — |
| | NULL key values | ✅ NULL matches NULL | ◐ a nullable key is dropped from identifiers, then upsert refuses | Iceberg |
| | key type handling | ✅ staged keys cast to the table type | ◐ dedup compares raw values | Iceberg |
| **Types** | decimals | ✅ exact decimal128, rounding reported; `x-singer.decimal` without integer precision and scale becomes exact VARCHAR, as in target-iceberg (DUCKLKD-84) | ✅ exact; an unbounded decimal becomes exact string | — |
| | decimals inside JSON | ◐ converted to float | ✅ exact string | DuckLake · DUCKLKD-22 |
| | `x-sql-datatype` hints | ✅ wider vocabulary (MySQL unsigned, `datetime2`, unsized numeric); hints that conflict with the JSON type are ignored with a warning | ✅ core vocabulary; `timetz` maps to time with a warning; binary | — (see binary) |
| | `date-time` default | TIMESTAMP (wall clock unless `convert_tz_to_utc`) | timestamptz (UTC) | Divergence · **DUCKLKD-89** |
| | objects / arrays | JSON text, or flattened | ✅ native struct / list when properties/items are declared; JSON text otherwise | DuckLake · **DUCKLKD-85** |
| | `anyOf` / `oneOf` / `allOf` | ✅ first typed branch, wrapper hint carried | ✗ string with a warning | Iceberg |
| | multi-type | ✅ VARCHAR, exact text | ◐ string with a warning | — |
| | a number its column would round (extra fraction digits, a fraction for an integer) | ✅ `value_type_checking`: `permissive` (default) rounds as DuckDB's DECIMAL cast, with a warning (a key warning says records merge); `strict` fails naming column and value (DUCKLKD-97, DUCKLKD-99) | ◐ strict only: Arrow refuses a decimal rescale; no setting | Divergence (no shared setting) |
| | `type_source` (all-string taps) | ✗ (`auto_cast_timestamps` only) | ✅ per stream, `on_error` policy | DuckLake · DUCKLKD-43 |
| | binary (`contentEncoding: base64`) | ✗ text | ✅ strict decode | DuckLake · **DUCKLKD-91** |
| **Schema** | add columns | ✅ | ✅ (refused on upsert tables with rows: a PyIceberg limit) | Iceberg (upstream limit) |
| | type widening | ◐ int family, float→double | ◐ accepts an already-wider table; never alters | — |
| | name conforming | ✗ raw names; truncation to 63 on Postgres | ✅ SDK `conform_name` parity, clash detection | Divergence · **DUCKLKD-87** |
| | case handling | ✅ case-insensitive matching | ✅ lowercased | — |
| **Layout** | partition transforms | ✅ identity, year–hour, bucket (DUCKLKD-38); no truncate/void in DuckLake 1.5.2 | ✅ identity, year–hour, bucket, truncate, void | — |
| | when applied | at creation, then only when changed (DUCKLKD-38) | at creation only | — (deliberate difference) |
| | sort order | ✅ `sort_order` (DUCKLKD-38) | ⚠ `sort_order` declared, never applied | Iceberg |
| | shared `table-config.json` | ✅ version 2 only, as Zamboni v0.7.0; `partition` and sort-mode `ordering`; pinned to Zamboni's spec by contract tests (DUCKLKD-94) | ◐ `partition` only, version 1 only, so a v2 file's partitions are silently not applied (being fixed there) | Iceberg |
| | small-file control | ✗ inlining forced off | ✗ (format v2, object-storage layout) | DuckLake · DUCKLKD-39 |
| **Catalog & storage** | catalog types | Postgres, SQLite, MySQL, DuckDB | REST, SQL | — |
| | catalog auth | password in URL | OAuth2, bearer token, TLS settings | DuckLake · DUCKLKD-26 |
| | storage | S3, GCS, local | S3 (endpoint, path-style), GCS, Azure, file, catalog-vended | DuckLake · DUCKLKD-24, **88** |
| | keyless identity | ✗ anonymous fallback | ◐ ambient credentials | DuckLake · DUCKLKD-25 |
| **Safety** | tenant guard | ✗ | ✅ warehouse and location checks | DuckLake · DUCKLKD-27 |
| | secret redaction | ◐ SQL in errors | ✅ | DuckLake · DUCKLKD-28 |
| | fail-closed startup checks | ◐ | ✅ | DuckLake · DUCKLKD-29 |
| | stream→table collisions | ✗ (3-part names drop their first part silently) | ✅ | DuckLake · DUCKLKD-44 |
| **Transactions** | STATE vs commits | ✅ held during an overwrite | ◐ SDK default; early during a full refresh | Iceberg |
| | commit-conflict retry | ✅ DuckLake-native (`ducklake_max_retry_count`) | ✅ 8 attempts with jitter | — |
| | transient-error retry | ✅ IO / connection errors in `execute()` | ✗ commit conflicts only | Iceberg |
| | parallel drains | ✅ waits for all before raising; SQLite serial | SDK default (raises first error early) | Iceberg (see DUCKLKD-79) |
| **Throughput** | Singer BATCH messages | ✗ (a BATCH message would bypass `process_record`) | ◐ jsonl/parquet; bypasses conforming and coercion | DuckLake · DUCKLKD-40 |
| | batch size setting | `max_batch_size` (SDK `batch_size_rows` ignored) | `batch_size_rows` | Divergence · **DUCKLKD-92** |
| **Observability** | write-completed events | ✗ | ✅ CloudEvents over NATS | DuckLake · **DUCKLKD-86** |
| | snapshot provenance | ✗ | ✅ snapshot summary keys | DuckLake · **DUCKLKD-90** |
| | health warnings | ◐ ad hoc | ✅ (merge-on-read delete ratio, unpartitioned create, …) | DuckLake · DUCKLKD-34 |
| **Maintenance** | compaction, expiry, cleanup | ✗ | ✗ (delegated to a separate maintainer) | DuckLake · DUCKLKD-33, 35 |
| **Integration** | spec.json, plugin declaration | ◐ `meltano.yml` stale (missing `hard_delete`, `process_activate_version_messages`, `max_column_length`) | ✅ generated, parity-tested | DuckLake · DUCKLKD-42 |
| | dev stack, CI integration tests | ✗ (SQLite catalog tests only) | ✅ Lakekeeper, Postgres, MinIO, NATS in CI | DuckLake · DUCKLKD-30, 31 |
| | singer-sdk | 0.46.4 | 0.54.7 | DuckLake (gap-list B14) |

## Gaps in target-iceberg

These are recorded here for target-iceberg's own tracker and are not filed on the DUCKLKD board.

1. **Per-stream settings accepted but never applied.** `sort_order`, `write_mode` and `key_properties` in `stream_defaults`/`stream_overrides` are declared (target.py:112-157) and advertised in the README and design docs, but nothing reads them. The test that claims to cover `write_mode` (tests/test_upsert.py:270-289) only checks that it is unset. `write_mode` is the worst of the three: a stream set to `write_mode: upsert` under `load_method: append-only` silently appends and duplicates every key (confirmed by the target-iceberg agent, 2026-10-21).
2. **STATE during a staged full refresh.** A mid-run drain commits to the staging branch and can emit STATE before the swap makes the data visible. target-ducklake holds STATE until its overwrite commits (DUCKLKD-20).
3. **`anyOf` / `oneOf` / `allOf`.** They become string columns with a warning; target-ducklake takes the first typed branch (DUCKLKD-60/65).
4. **Nullable keys.** A key the tap declares nullable is dropped from the identifier fields, and upsert then refuses. target-ducklake matches NULL keys (DUCKLKD-49).
5. **Transient errors.** Only commit conflicts are retried; network errors fail the run.
6. **Parallel-drain failures.** The SDK raises the first error while other drains run; target-ducklake found this can abort the process (exit 134, Python 3.12) and waits for every drain (DUCKLKD-79). target-iceberg uses the SDK drain on Python ≥ 3.12, so the same abort may apply; not checked there.
7. **Smaller items.** In-batch dedup compares raw values (`"1"` vs `1`); `py-io-impl` is read but not declared; stale comments on `delete_strategy`; BATCH chunks skip conforming and `type_source` coercion.

**Two different "write modes".** In xflow, "write mode" per table means Iceberg's `write.delete.mode`/`write.update.mode` table properties. target-iceberg sets them from the per-stream `delete_strategy` (`copy_on_write` or `merge_on_read`; sinks.py:273-279, 535-546), and that works. target-iceberg's per-stream `write_mode` is a different, Singer-level idea: a per-stream override of `load_method` (append, upsert, overwrite), and it is the dead key above. For target-ducklake:
- **`write_mode` is a cautionary tale.** target-ducklake rejects it at startup (DUCKLKD-38), pointing to DUCKLKD-41, so it can't silently fall back to the global `load_method`. DUCKLKD-41 must honour any per-stream load method it accepts, or reject it.
- **`delete_strategy` has no DuckLake counterpart.** A DuckLake DELETE (and so a merge) always writes delete files that readers apply, which is merge-on-read; rewriting them away is maintenance (DUCKLKD-33). Since DUCKLKD-94, target-ducklake accepts and ignores `delete_strategy` and `merge_on_read_maintenance_acknowledged` (logged), so a shared config works.

**Shared config (DUCKLKD-94).** A target-iceberg config now loads on target-ducklake: Iceberg-only settings (`delete_strategy`, `on_error` without `type_source`, partition `name`, `truncate`/`void` entries, `zorder`) are ignored with a log line, and settings that change what is loaded (`write_mode`, `key_properties`, `type_source`) are rejected naming DUCKLKD-41/43. `table_config` reads Zamboni's version 2 only; Zamboni v0.7.0 refuses version 1, so accepting it would make a "shared" file work in one place only. target-ducklake vendors Zamboni's published spec in `tests/contracts/` and asserts `TABLE_CONFIG_VERSION = 2`, target-iceberg's vendor-and-assert-version pattern; target-iceberg's own reader is v1-only and its agent is fixing that.

## Divergences for xflow

Each of these is a design choice in one target; xflow sees different tables for the same tap. Aligning them is a product decision, so the target-ducklake side is filed as opt-in settings.

| Divergence | target-ducklake | target-iceberg | Issue |
| --- | --- | --- | --- |
| date-time type | TIMESTAMP, wall clock unless `convert_tz_to_utc` | timestamptz, UTC | DUCKLKD-89 |
| column names | as sent; truncated to 63 on Postgres | conformed (lowercase, `_` separators) | DUCKLKD-87 |
| `hard_delete` | SDK default false; ACTIVATE_VERSION soft-deletes unless true | default true; `false` refused | — (document) |
| schema routing | `default_target_schema`, else derived from `schema-table` stream names | `default_target_schema`, default `elt_raw` | — (document) |
| batch size | `max_batch_size` | `batch_size_rows` | DUCKLKD-92 |
| per-stream config | `stream_defaults` / `stream_overrides` (DUCKLKD-38); `partition_fields` still accepted; target-iceberg's keys accepted (DUCKLKD-94) | `stream_defaults` / `stream_overrides` | aligned |

## For DUCKLKD-38

- **Config shape:** use target-iceberg's per-stream `stream_defaults` / `stream_overrides` with `partition: [{column, transform, num_buckets, name}]` and `sort_order` (decided 2026-10-21). Keep `partition_fields` working as the older form.
- **Transforms:**
  - identity, year, month, day, hour and bucket map directly;
  - reject `truncate` and `void` with a clear error, since DuckLake 1.5.2 has neither ("Unsupported partition function truncate");
  - unlike target-iceberg, do implement `sort_order`.
- **Apply on change only.** Re-applying an unchanged layout adds a snapshot every run (partitions), and for sort order a new sort-spec version too. So compare with the table's current layout and alter only on a difference.
- **When to apply:** target-iceberg sets the layout at creation only. DuckLake evolves partitioning and sort order cheaply (metadata only, applies to new files), so applying a changed layout to an existing table is a reasonable difference. Document it.
