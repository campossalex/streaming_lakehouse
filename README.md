# Streaming Lakehouse — Open-Source Edition

The `streaming_lakehouse` workshop on open source only: **Apache Flink 1.20**, **Apache
Fluss 0.9.1** and **Apache Iceberg 1.10**, with Lakekeeper as the Iceberg REST catalog,
MinIO as object storage and Trino as an independent reader. Everything runs in Docker
Compose; there is no Kubernetes and no Ververica Platform.

Attendees write every statement in the **web Flink SQL editor** (http://localhost:8084),
a one-page UI in front of Flink's SQL Gateway — the same editor `../coffee_shop_oss`
uses. The attendee walkthrough is [WORKSHOP.md](WORKSHOP.md), with the same labs and step
numbers as the VVP edition in `../streaming_lakehouse/instructions.MD`. This file is for
whoever runs the environment.

---

## Architecture

```
  ┌───────────┐   orders_log    ┌──────────────────────────────┐
  │ order-gen │ ──────────────► │ Redpanda (Kafka)             │
  └───────────┘                 └──────────────┬───────────────┘
                                               │ job: kafka-to-fluss
                                               ▼
                                 fluss.orders.orders_log  (Log table)
                                               │
   PostgreSQL product_catalog                  │ job: product-enrichment
     │ job: pgcdc-to-fluss (postgres-cdc)      │ lookup join FOR SYSTEM_TIME AS OF
     ▼                                         ▼
   fluss.orders.product_lookup (PK) ──► fluss.orders.orders_enriched  (Log table)
                                               │          │
                  job: Fluss Lake Tiering      │          │ job: revenue-analytics-sink
                  Service (tiering.sh)         ▼          ▼   (TUMBLE 5 min)
                                               │   PostgreSQL revenue_5m ──► Grafana
                                               ▼
               Iceberg  warehouse.orders.orders_enriched
               ├─ metadata ── Lakekeeper (REST catalog) ── catalog-db (Postgres)
               └─ Parquet ─── MinIO  s3://warehouse/lakehouse/
                                               │
                           Trino ◄─────────────┘──► CloudBeaver
```

Bonus track: `order-status-sync` upserts `orders_log` into the PK table
`fluss.orders.order_status`, which is tiered the same way.

| Component | Container(s) | Role |
|-----------|--------------|------|
| Fluss | `zookeeper`, `coordinator-server`, `tablet-server` | Log and PK tables; creates the Iceberg table when one is ALTERed to `table.datalake.enabled` |
| Flink | `jobmanager`, `taskmanager` | Session cluster: the pipeline jobs, the tiering service, ad hoc queries |
| Flink SQL | `sql-gateway`, `sql-editor`, `sql-client` | Web editor (via the gateway's REST API) and the terminal client |
| Lake | `minio`, `lakekeeper`, `catalog-db` | Object storage and the Iceberg REST catalog |
| Readers | `trino`, `cloudbeaver` | Plain Iceberg reads with no Fluss in the path |
| Homepage | `homepage` | nginx serving `homepage/index.html` on port 80: a card per web UI, links built from the host the page was opened on, and a reachability check every 15 s |
| Sources/sinks | `redpanda`, `postgres`, `order-gen`, `grafana` | Order events, product catalog (CDC), order analytics dashboard |

---

## Prerequisites

| Tool | Version |
|------|---------|
| Docker + Docker Compose v2 | `docker compose version` ≥ 2.20 |
| `curl`, `jq` | Any recent version |
| **10 GB RAM available to Docker** | About 7–8 GB in use with every job running |
| Free host ports | 80, 3000, 5432, 8080–8084, 8181, 8978, 9000, 9001, 9123, 19092 |

The ports overlap with `../coffee_shop_oss` and `../lakehouse_oss`; stop those first
(`docker compose stop` in their directories).

---

## Quick Start

```bash
cd scenarios/streaming_lakehouse_oss

# For a workshop: everything up, nothing submitted — attendees start from here
./start.sh --services-only

# For a demo: everything up AND every lab done (all jobs + tiering running)
./start.sh
```

`start.sh` downloads the JARs on first run, starts the stack, waits for it, and — without
`--services-only` — starts the tiering service and submits every job. Either way it ends
by printing the runbook: every manual step as a copy-pasteable command.

With everything submitted, the first Iceberg snapshot lands within about a minute and
the first Grafana window about six minutes in (a 5-minute window plus the watermark).

`./start.sh --reset` removes every volume first — Fluss tables, the Iceberg lake, Kafka,
PostgreSQL — which is how to hand a clean environment to the next group.

| Service | URL |
|---------|-----|
| **Environment homepage** — links to everything below, live status, connection details | http://localhost |
| Flink SQL editor | http://localhost:8084 |
| Flink Web UI | http://localhost:8081 |
| CloudBeaver (Trino) | http://localhost:8978 |
| Lakekeeper UI | http://localhost:8181/ui |
| MinIO Console | http://localhost:9001 (`admin` / `password`) |
| Redpanda Console | http://localhost:8082 |
| Grafana | http://localhost:3000 |

---

## Running the steps by hand

The attendee path is the SQL editor. Every step also has a terminal equivalent, which is
what `start.sh` uses:

```bash
# Lab 1 and 2 — the pipeline jobs
docker compose exec sql-client /opt/submit.sh kafka cdc enrich

# Lab 3 — the tiering service, then opt the tables in. Attendees use the editor's
# "Tiering service: Start" button instead (see "The tiering service" below)
docker compose exec jobmanager /opt/tiering.sh
docker compose exec sql-client /opt/submit.sh lake

# Lab 5 and the bonus track
docker compose exec sql-client /opt/submit.sh revenue status

# An interactive SQL client. Only 02_sources.sql is needed: see "What survives a session"
docker compose exec sql-client /opt/flink/bin/sql-client.sh -i /opt/sql/ddl/02_sources.sql

# Trino from a terminal
docker compose exec trino trino --catalog warehouse --schema orders
```

`submit.sh` concatenates `flink_sql/ddl/*.sql` into one init file, loads it with `-i`,
and runs one job file with `-f` per name; it fails a step that prints `[ERROR]` or that
should have produced a Job ID and did not. `tiering.sh` refuses to start a second copy of
the tiering service.

### Files

| File | Purpose |
|------|---------|
| `flink_sql/ddl/01_fluss.sql` | The `fluss` catalog, the `orders` database and all four Fluss tables |
| `flink_sql/ddl/02_sources.sql` | Kafka source, postgres-cdc source, JDBC `revenue_5m` sink (column meanings: `pg_streaming_lakehouse_ddl.sql`) |
| `flink_sql/jobs/10…50_*.sql` | One long-running `INSERT INTO` each, in lab order |
| `flink_sql/lake/enable_tiering.sql` | The two `ALTER TABLE ... 'table.datalake.enabled'` statements |
| `flink_sql/explore.sql` | The labs' SELECTs and ALTERs; one editor example per `-- ---- title` section |
| `trino_sql/lakehouse.sql` | The Trino queries for CloudBeaver: Steps 13, 16–18, 24 |
| `sql-editor/` | Flask proxy (`app.py`) in front of the SQL Gateway, and the one-page UI |
| `tiering.sh`, `submit.sh` | Mounted into `jobmanager` and `sql-client` respectively |
| `tiering.args` | The tiering service's program arguments — the one copy, used by `tiering.sh` and the editor's Start button |
| `pg_streaming_lakehouse_ddl.sql` | Seeds `product_catalog` (500 rows) and sets up CDC; runs once, via `docker-entrypoint-initdb.d` |

---

## The web SQL editor

Two services provide http://localhost:8084:

- **`sql-gateway`**: Flink's SQL Gateway, the engine the SQL Client embeds, exposed as a
  REST API on port 8083. Sessions never expire (`sql-gateway.session.idle-timeout=0`), so
  a coffee break does not drop an attendee's tables.
- **`sql-editor`**: a small Flask app that serves the page and proxies to the gateway,
  which sends no CORS headers.

It behaves as in `coffee_shop_oss` (see that GUIDE for the details): one session per
browser, kept across reloads; statements run in order, each with its own result panel; a
streaming `SELECT` stays live until **Cancel**, which cancels its Flink job; an `INSERT
INTO` is a real job that outlives the tab. The examples menu has each `ddl/` file, every
`explore.sql` section and each job file (with a trailing `RESET 'pipeline.name'`).

Differences from the coffee shop's copy:

- **No placeholder substitution and no redaction.** This scenario has no secrets, so the
  SQL files use literal hostnames and statements reach the gateway verbatim.
- **Optimizer hints are kept.** The statement splitter used to drop every `/* ... */`
  block, hints included, so `/*+ OPTIONS('snapshot-id' = '...') */` silently vanished
  and a time-travel query read the latest snapshot instead. `/*+` is now passed through.
  (`coffee_shop_oss/sql-editor/app.py` still has the old behaviour.)
- **`PRELOAD_DDL=true` is safe to combine with attendees' own DDL**: every statement in
  `ddl/` is `IF NOT EXISTS`.

---

## The tiering service: deployed, then started

In the VVP edition the tiering service is a Deployment that attendees open and click
**Start** on. Flink has no stopped deployments — a job either runs or does not exist —
so this edition splits it the nearest way it can:

- **Deploy:** `start.sh` (both modes) has the editor upload `lib/tiering/fluss-flink-tiering-*.jar`
  to the JobManager. An uploaded JAR is listed in the Flink Web UI under *Submit New Job*,
  ready to run.
- **Start:** the editor's header shows *Tiering service: deployed, not running* and a
  **Start** button, which runs the uploaded JAR with `tiering.args` through the
  JobManager's REST API (`POST /jars/<id>/run`) — the same call the Web UI makes. The
  header then shows the job's state, polled every 10 s, with a link to it.

Details worth knowing:

- **Checkpointing.** A job submitted this way is built by the JobManager with the
  cluster's `config.yaml`, so it gets the 30 s checkpoint interval it needs to commit.
- **Only one copy.** Start does nothing when a tiering job is already active, however it
  was started, and so does `tiering.sh`. The Web UI's Submit New Job has no such check:
  submitting from there twice runs two tiering services that compete for the same tables.
- **JobManager restarts.** Uploaded JARs live in the JobManager's `/tmp`, so a restart
  forgets them and the header shows *not running* instead of *deployed, not running*.
  Start re-uploads the JAR itself first, so it still works.
- **Where it shows.** The editor's `/api/config` reports `tiering: true` only when it finds
  the JAR and `tiering.args` (mounted into `sql-editor` in `docker-compose.yml`). The same
  `index.html` is used by `coffee_shop_oss`, where the control stays hidden.

---

## What survives a session

This is the one concept that differs most from the VVP edition, where tables live in a
persistent platform catalog.

| Object | Where it is stored | After *New session*, a gateway restart, or a new `sql-client` |
|--------|--------------------|------------------------------------------------------------------|
| `fluss.orders.*` tables | In Fluss | **Still there** |
| The `fluss` (and `iceberg`) catalog registration | File-based CatalogStore, volume `catalog-store`, shared by `sql-client` and `sql-gateway` | **Still there** |
| `orders_log_kafka`, `product_catalog_cdc`, `revenue_5m_sink` | Flink's in-memory `default_catalog` | **Gone** — re-run `ddl/02_sources.sql` |
| Running jobs | Flink cluster | **Unaffected**: connector options were baked into the JobGraph at submission |

So after a gateway restart, attendees only re-run `02_sources.sql`, and only if they
want to submit another job that reads Kafka or CDC.

---

## Implementation notes

| Concern | How this scenario does it |
|---------|---------------------------|
| Versions | Flink 1.20 (`apache/flink:1.20-java17`), Fluss 0.9.1-incubating, Iceberg 1.10.1 — the same versions the VVP edition's tiering setup uses |
| Fluss config | `FLUSS_PROPERTIES` env var, appended to `server.yaml` by the image's entrypoint. Not a bind-mounted `server.yaml`: the entrypoint edits that file in place with `sed -i` |
| Fluss remote storage | A shared named volume at `/fluss/remote-data`, not `s3://` — see below |
| Iceberg catalog | Lakekeeper warehouse `warehouse`, `sts-enabled: false`, `remote-signing-enabled: false`, static MinIO keys everywhere |
| Tiering service | `fluss-flink-tiering` JAR via `flink run -d` from the `jobmanager` container, so it inherits the cluster's 30s checkpoint interval — it commits to Iceberg on checkpoints |
| Iceberg plugin in Fluss | The image's `plugins/iceberg/` has `fluss-lake-iceberg` but not S3FileIO; the entrypoints copy `iceberg-aws`, `iceberg-aws-bundle` and `failsafe` in from `lib/` |
| Hadoop | Trino's relocated `hadoop-apache`, not `flink-shaded-hadoop-2-uber` — see below |
| CDC | `wal_level=logical` on the Postgres command line; slot and publication created by the init script |
| Revenue sink | Explicit `jdbc` table with a PRIMARY KEY (upsert), not a JDBC catalog |
| Grafana | Datasource and the *Order Analytics* dashboard (`grafana/dashboards/order_analytics.json`) provisioned, pinned to the datasource uid `lakehouse_dwh` |

Four of these cost real debugging time and are worth knowing about before changing
anything.

**Fluss's remote data is a local volume, not MinIO.** With `remote.data.dir` on `s3://`,
Fluss hands clients short-lived STS credentials (`GetSessionToken`), which MinIO does not
implement, so every Flink read that touches remote data fails with
`NoAwsCredentialsException`. The Iceberg lake still lives on MinIO: Iceberg's S3FileIO
takes static keys. The volume is mounted at the same path in the Fluss servers and all
Flink containers, and is made world-writable by `fluss-remote-init`, because both sides
write to it — the servers their segments and snapshots, the tiering job its per-commit
`*.offsets` files.

**It is not mounted under `/tmp/fluss`.** Docker creates a mount point's missing parents
as `root:root 0755`, and `/tmp/fluss` is the Fluss client's scratch directory. Flink runs
as uid 9999, so a streaming read of a PK table — which downloads the table's KV snapshot
there first — failed with
`IOException: Failed to create directory /tmp/fluss/kv-snapshots-...`.

**Flink needs S3 keys as `AWS_*` environment variables.** Fluss gives clients a table's
`table.datalake.iceberg.*` options (see `SHOW CREATE TABLE`) but drops the credential
ones. Without `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION` on the Flink
containers, the union read, the `$lake` tables and the `$lake$snapshots` table all fail
with `SdkClientException: Unable to load credentials from any of the providers in the
chain`. The tiering job does not need them: `tiering.sh` passes the keys as arguments.

**No `flink-shaded-hadoop-2-uber`.** Iceberg needs Hadoop's `Configuration` class on the
classpath, and the usual way to get it on Flink is the uber JAR. But it bundles Avro 1.7,
which shadows the Avro 1.12 Iceberg is built against, and the tiering job restart-loops
on its first write with `NoSuchMethodError: ... LogicalTypes.timestampNanos()`. Trino's
`hadoop-apache` relocates all of Hadoop's dependencies, Avro included.
`download-jars.sh` deletes an uber JAR it finds left over in `lib/`.

---

## Troubleshooting

**`orders_enriched` never shows up in Trino.** Tiering needs all three of: the tiering
job running (`curl -s localhost:8081/jobs/overview | jq -r '.jobs[].name'` lists
`Fluss Lake Tiering Service - iceberg`), the `ALTER TABLE` having run, and new data in
the table. Then look at the TaskManager:

```bash
docker compose logs taskmanager | grep -E "Committed snapshot|Exception" | tail
```

**The coordinator logs `Fail to change state for table N from Scheduled to Tiered`.**
Harmless. Fluss 0.9.1's coordinator logs this pair of ERRORs at nearly every tiering
commit in this setup, whatever the freshness, while the commits themselves succeed —
check with the `$snapshots` query in `trino_sql/lakehouse.sql`.

**A Fluss table can't be tiered: the ALTER fails, or its Iceberg table never fills.**
The table was created before the servers had `datalake.format: iceberg`. That setting is
copied into the table at `CREATE TABLE` time and fixes its bucketing; DROP and re-create
the table.

**`NoSuchMethodError: ... org.apache.avro.LogicalTypes.timestampNanos()`.** An old Avro
is on the Flink classpath — almost always `flink-shaded-hadoop-2-uber` in `lib/`. Re-run
`./download-jars.sh` (it removes it), then
`docker compose up -d --force-recreate jobmanager taskmanager sql-client sql-gateway`
and resubmit: the recreated JobManager has no jobs.

**`VerifyError` mentioning `S3V4RestSignerClient` at the first commit.** The Lakekeeper
warehouse was created with remote signing enabled. It must be `false`, as in
`lakekeeper/create-warehouse.json`; check with
`curl -s 'localhost:8181/catalog/v1/config?warehouse=warehouse' | jq .overrides`.

**The CDC job fails with `replication slot ... is active`.** Only one reader per slot:
another `pgcdc-to-fluss` is already running, possibly one an attendee submitted. Cancel
the duplicate in the Flink Web UI.

**Ad hoc queries stay in `CREATED` / never return rows.** The TaskManager has 12 slots
and each running job holds one. Six pipeline jobs plus several forgotten streaming
SELECTs fill it up: cancel what you are not using, from the editor or the Flink Web UI.

**Grafana panels are empty.** A window is written only once it closes: the first row
lands about 5 minutes after `revenue-analytics-sink` starts. Check with
`docker compose exec postgres psql -U root -d orders -c 'SELECT count(*) FROM revenue_5m;'`.

---

## Teardown

```bash
docker compose down -v   # remove containers and every volume — the normal way to stop
```

A plain `docker compose down` (no `-v`) is not a useful middle ground: PostgreSQL, MinIO
and the Lakekeeper catalog are on named volumes and survive, but ZooKeeper and the tablet
server keep Fluss's metadata and data inside their containers, so the Fluss tables are
gone while their Iceberg copies remain. Use `./start.sh --reset` to start clean.

Whatever survives, the jobs do not: the session cluster has no HA. And resubmitting
`kafka-to-fluss` on top of existing Fluss data duplicates it, because the Kafka table
reads from `earliest-offset`. Between groups, `./start.sh --reset` is the only clean
restart.
