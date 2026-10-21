# target-ducklake Gap List

As of 2026-10-16 · Paul Caron · Source doc: <https://claude.ai/code/artifact/8b044cac-c7c5-44ac-9cf8-8fdf7510ad02>

> **Update 2026-10-21.** For comparisons with target-iceberg, see the two-way [parity matrix](target-parity-ducklake-iceberg.md), which supersedes list B. Fixed on `xflow_dev` since this list was written: A4 (DUCKLKD-18), A5 (DUCKLKD-20), B1 (DUCKLKD-16), B2 (DUCKLKD-17), B6 (DUCKLKD-20) and B7 (DUCKLKD-21/51). A3 is partly done: ACTIVATE_VERSION deletes (DUCKLKD-20); `_sdc_deleted_at` deletes remain DUCKLKD-23.

For batch ingestion, current target-ducklake has 34 gaps: 9 High, 13 Medium and 12 Low. Streaming needs 16 more enhancements, 6 of them High.

Three of the High batch items are data-correctness bugs to fix now, whatever the backend: lost decimal precision (B1), arbitrary duplicate choice (B2) and composite-key collisions (A4). The rest of the High list is what blocks hyperscaler deployments: Azure storage, keyless cloud identity, IAM catalog auth, the tenant guard, hard deletes and table maintenance.

**Priority key.** High = required; blocks a production deployment on AWS, Azure or GCP, or corrupts data. Medium = needed for scale, operability or xflow integration. Low = nice to have.

| List | High | Medium | Low |
| --- | --- | --- | --- |
| A. Batch, vs dlt-backed target | 4 | 5 | 6 |
| B. Batch, vs Iceberg / target-iceberg | 5 | 8 | 6 |
| C. Streaming enhancements | 6 | 7 | 3 |

## A. Batch gaps vs the dlt-backed target

Four gaps are High: Azure, keyless cloud identity, hard deletes and the composite-key merge bug. dlt closes all four with little or no extra code.

| # | Gap in current target-ducklake | Priority | Why it matters | How the dlt-backed target closes it |
| --- | --- | --- | --- | --- |
| A1 | No Azure Blob / ADLS storage: `storage_type` accepts only `GCS`, `S3`, `local` | High | Blocks every Azure deployment | dlt's native `abfss://` / `az://` secret on duckdb 1.5.2 (account key, SAS, service principal, managed identity) |
| A2 | No keyless cloud identity: with no keys set, `main` creates no DuckDB secret, so storage is accessed anonymously. Only the `definite` branch adds a GCS credential chain | High | Blocks EKS IRSA, GKE Workload Identity and AKS managed identity, where static keys are usually forbidden | `credential_chain` secrets for S3 and Azure; GCS needs a small custom secret |
| A3 | No hard deletes: `_sdc_deleted_at` rows stay in the table as live rows | High | Replicas of OLTP sources drift; deleted customers and orders remain in reports | `hard_delete` column hint, or the `cdc` merge strategy |
| A4 | Composite-key merge concatenates keys with `\|\|`, so (`'1'`,`'23'`) matches (`'12'`,`'3'`) | High | Merge can delete the wrong rows: silent data corruption | dlt merge SQL compares each key column (inferred; confirm in the spike) |
| A5 | Overwrite is not atomic: the table is truncated at setup, then refilled batch by batch | Medium | Dashboards see an empty or partial table for the whole run | `replace` + `insert-from-staging` for single-run loads; a staging-table swap for multi-batch runs |
| A6 | One global `load_method` and no per-stream key override | Medium | Mixed taps (events + dimensions) need separate pipelines | Per-resource write disposition and key hints |
| A7 | No Singer BATCH messages | Medium | Per-record Python work dominates at tens of millions of rows | `with_file_import` loads Parquet files directly |
| A8 | Only mocked unit tests; no tests against a real catalog or bucket | Medium | Regressions in SQL, secrets or DuckLake upgrades reach production | dlt's shared load suite already runs against DuckLake |
| A9 | No schema-contract policy: an incompatible value fails at INSERT with a cast error | Medium | One bad record stops the whole load with no choice to drop it | `freeze` / `discard_row` / `discard_value` contracts |
| A10 | No upsert through `MERGE` (only DELETE+INSERT) | Low | Fewer statements per batch on unpartitioned tables | `merge` + `upsert`; keep delete-insert on partitioned tables (ducklake#643) |
| A11 | No SCD type 2 history | Low | History has to be rebuilt in dbt snapshots | `merge` + `scd2` |
| A12 | No insert-only merge for immutable keyed events | Low | Duplicate re-sends cost a delete per batch | `merge` + `insert-only` |
| A13 | No load audit table | Low | Hard to answer which run wrote which rows | `_dlt_loads` row per committed load |
| A14 | Data inlining hard-coded off (`DATA_INLINING_ROW_LIMIT 0`) | Low | Small incremental runs create many tiny files | Make it configurable through `set_option`; flush in maintenance |
| A15 | Arrays can only become JSON | Low | Some consumers prefer child tables | dlt child tables, opt-in per column |

## B. Batch gaps vs Iceberg and target-iceberg

Five gaps are High. Two are data-correctness bugs that target-iceberg handles and target-ducklake does not: decimal precision and which duplicate wins. A1–A4 apply here too and are not repeated.

| # | Gap in current target-ducklake | Priority | Why it matters | Path on DuckLake |
| --- | --- | --- | --- | --- |
| B1 | Decimals become `float` before Parquet (`preprocess_record`), then are cast to `DECIMAL(38,9)` | High | Values beyond about 15 significant digits are silently altered; unsafe for money | Build Arrow `decimal128(p,s)` from `x-singer.decimal` / schema precision, as target-iceberg does |
| B2 | In-batch dedup keeps an arbitrary row: Polars `unique(subset=keys)` defaults to `keep="any"` | High | An older version of a row can overwrite the newer one in the same batch | `keep="last"` (record order), matching target-iceberg's last-wins |
| B3 | Catalog login is a static password inside `catalog_url`; no IAM token auth | High | Managed Postgres set up for IAM-only auth (Cloud SQL IAM, RDS IAM, Entra ID) can't be used. target-iceberg supports OAuth2 and bearer tokens | Fetch a short-lived IAM token at startup and pass it as the libpq password |
| B4 | No tenant guard: nothing checks that the catalog's stored `data_path` and `meta_schema` match config | High | A wrong `catalog_url` writes tenant A's data into tenant B's lake | Read the catalog's stored settings at startup and fail closed on mismatch, like target-iceberg's warehouse and prefix check |
| B5 | No table maintenance: no compaction, snapshot expiry or file cleanup anywhere | High | Files, snapshots and storage cost grow without bound; queries slow down. Iceberg has engine-side maintenance; DuckLake needs our own job | `ducklake-ops` program: flush, expire, merge, rewrite, cleanup on a daily cron |
| B6 | `ACTIVATE_VERSION` is only logged; no full-refresh semantics | Medium | Full-table taps never remove rows that disappeared at the source | Staging table plus a swap in one DuckLake transaction (target-iceberg uses a staging branch) |
| B7 | `x-sql-datatype` hints ignored (int vs bigint, timestamptz, uuid, binary) | Medium | Types drift from the source database's real types | Port target-iceberg's `mapping.py` |
| B8 | No secret redaction: `DuckLakeConnectorError` embeds the full SQL text | Medium | Credentials or PII in SQL can reach logs and alerts | Redact before raising; target-iceberg's `errors.py` is the model |
| B9 | Startup validation is partial (`_validate_config` checks keys and region only) | Medium | Misconfigurations surface mid-run instead of at start | Fail-closed checks for catalog, storage, keys and merge settings |
| B10 | No `type_source` coercion for all-string taps (CSV, files) | Medium | File taps land everything as VARCHAR | Port target-iceberg's `coerce.py` with its `on_error` policy |
| B11 | No xflow integration artifacts: `spec.json` for the admin UI, generated plugin declaration, Docker dev stack, CI integration tests | Medium | The target can't be offered through the xflow admin UI | Copy target-iceberg's pattern; dev stack = Postgres + MinIO + Azurite |
| B12 | Narrower engine reach than Iceberg (Snowflake, BigQuery, Athena can't read DuckLake) | Medium | Teams on those platforms need a copy | trino-ducklake and pg_ducklake for reads; `export-iceberg` in the ops program for the rest |
| B13 | No catalog backup or catalog-version guard | Medium | A newer DuckDB can upgrade the catalog to spec 1.1, which 1.5.2 can't read | Ops `backup-catalog` and `version-guard` |
| B14 | singer-sdk `~=0.46` vs target-iceberg's `~=0.54` | Low | Shared code between the targets drifts | Upgrade with the dlt work |
| B15 | No native nested structs or lists; nested data is flattened or JSON | Low | Consumers parse JSON instead of using typed fields | DuckLake supports structs; needs our own DDL (dlt has no struct type) |
| B16 | No `bucket[N]` partitioning or sort order | Low | Fewer pruning options on large tables | `bucket(N, col)` and `SET SORTED BY`, both in DuckLake 1.5.2 |
| B17 | No load provenance on snapshots | Low | Snapshot history can't say which stream or run wrote it | `CALL set_commit_message(...)` inside each load transaction |
| B18 | No detection of two streams writing one table | Low | Silent overwrites between streams | Port target-iceberg's collision check |
| B19 | No delete-file or small-file health warnings | Low | Degradation goes unnoticed until queries slow | Ops `health` report |

Not possible on DuckLake, so out of scope: catalog-vended storage credentials, a REST catalog, and the `truncate` / `void` partition transforms.

## C. Streaming ingestion enhancements

target-ducklake is a batch process: it runs, drains stdin and exits. Streaming needs a long-running consumer that commits small batches to DuckLake every few seconds, plus the maintenance to absorb that commit rate. Priorities here are for streaming ingestion; High means streaming can't go to production on a hyperscaler without it.

| # | Enhancement | Priority | Why it's needed | Approach |
| --- | --- | --- | --- | --- |
| C1 | Long-running consumer mode: flush on size or time (for example every 5–60 s), graceful shutdown, health and readiness endpoints | High | Singer's run-to-completion model doesn't fit an unbounded stream | A separate daemon entry point that shares the target's mapping, typing and load code |
| C2 | Kafka-protocol consumer | High | One client covers AWS MSK, Azure Event Hubs (Kafka endpoint), Confluent Cloud and Google Managed Kafka | Consumer groups; scale out by partition; one writer per table to limit commit conflicts |
| C3 | Exactly-once delivery: commit data and consumer offsets in the same DuckLake transaction | High | Broker offsets committed separately give duplicates or loss after a crash | Store offsets in a DuckLake table in the catalog; DuckLake transactions span tables in one catalog. Needs our own transaction code, because dlt isn't atomic across tables |
| C4 | Debezium CDC apply: parse the op (c/u/d/r) and before/after images, order by LSN or source timestamp, turn tombstones into deletes | High | Database replication through Kafka is the main streaming case | Collapse each micro-batch to the latest change per key by LSN, then delete + insert. dlt's `cdc` strategy is a reference |
| C5 | Small-file and snapshot control | High | A commit every few seconds creates thousands of tiny files and snapshots a day | Turn on data inlining (`DATA_INLINING_ROW_LIMIT` > 0), then `flush_inlined_data`, `merge_adjacent_files` and `expire_snapshots` every 15–60 minutes from `ducklake-ops` |
| C6 | Catalog write-throughput limits | High | The Postgres catalog is the single commit point; too many writers cause conflicts and retries | Cap commit rate per table, batch several tables per commit, size the connection pool, monitor retry counts |
| C7 | Initial snapshot to streaming handoff | Medium | CDC needs a consistent backfill before streaming changes apply | Bulk-load the snapshot with the batch target, then start streaming from the recorded LSN or offset |
| C8 | Google Pub/Sub consumer | Medium | GCP-native events not routed through Kafka | Streaming pull with acks after commit (at-least-once); dedup on message id |
| C9 | AWS Kinesis consumer | Medium | AWS-native streams not routed through Kafka | KCL-style lease and checkpoint, checkpoint after commit |
| C10 | Schema registry (Avro, Protobuf, JSON Schema) | Medium | Typed streams carry schemas in the registry, not in the message | Confluent / Glue / Azure Schema Registry lookups mapped to DuckLake types; schema evolution through the same add-column and widening rules |
| C11 | Dead-letter queue for bad messages | Medium | One poison message must not stop a partition | Route parse or type failures to a DLQ topic or a DuckLake quarantine table |
| C12 | Lag, freshness and commit-latency metrics with alerts | Medium | Operators need to see consumer lag and data freshness | Prometheus / OpenTelemetry metrics; alert on lag and on commit retries |
| C13 | Evaluate pg_duckpipe for Postgres sources | Medium | It already streams Postgres WAL into DuckLake inside Postgres (about 5 s default flush) | Use it where the source is Postgres and pg_ducklake is acceptable, instead of Debezium + C4 |
| C14 | Late and out-of-order events | Low | Event-time reports drift when events arrive late | Insert-only merge keyed on event id; partition by event time; watermark column |
| C15 | Outbound change feed | Low | Downstream streaming consumers want DuckLake changes | `table_changes()` export from `ducklake-ops` on a schedule |
| C16 | Spark Structured Streaming as a writer | Low | Teams already running Spark | MotherDuck's ducklake-spark connector is append-only and needs existing tables |

## Sources

- target-ducklake `main` at `e9dc3f3`. B1 and B2 checked in `target_ducklake/sinks.py` (`preprocess_record`, `_remove_temp_table_duplicates`); A2 in `connector.py` `_build_startup_script`; the GCS credential chain in `origin/definite`.
- [Feasibility study](target-ducklake-dlt-feasibility.md) for the dlt-backed target and the target-iceberg capability mapping.
- target-iceberg `6a76d77` (`~/checkout/xflow.ai/target-iceberg`).
- DuckLake extension branch `v1.5-variegata` (DuckDB 1.5.2); pg_duckpipe and ducklake-spark READMEs in `~/checkout/ducklake`.
- A4's fix in dlt is inferred from its merge design, not traced line by line.
