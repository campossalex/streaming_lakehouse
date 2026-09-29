# Real-Time to Lakehouse: Streaming Orders with Apache Fluss & Iceberg — Open-Source Edition

In this hands-on workshop you will build a **Streaming Lakehouse** with **Apache Flink**,
**Apache Fluss** and **Apache Iceberg**, entirely on open source. You'll ingest a
real-time stream of e-commerce order events from Kafka, store it in Fluss tables, enrich
it with product data replicated live from PostgreSQL via CDC, and transparently tier the
result into Iceberg for long-term analytics — all with Flink SQL.

A data generator is already running and publishing synthetic order events as JSON to the
Kafka topic `orders_log`. Each event is an order moving through its lifecycle: `PLACED`,
`PAID`, `SHIPPED`, `DELIVERED` (or, occasionally, `CANCELLED`). You will build the
ingestion → state → enrichment pipeline, enable Fluss's native tiering to Iceberg, and
then query the very same table both as a live stream and as a historical Iceberg table.

This is the open-source edition of the Ververica Platform workshop in
`../streaming_lakehouse`. The labs and step numbers are the same; what changes is where
you type the SQL and how a job gets deployed.

## Your tools

| Tool | URL | What you use it for |
|------|-----|---------------------|
| **Lab homepage** | http://localhost | Links to every tool below, with a live up/down status, and the connection details |
| **Flink SQL editor** | http://localhost:8084 | Write and run every Flink SQL statement |
| **Flink Web UI** | http://localhost:8081 | See, inspect and cancel the running jobs |
| **Redpanda Console** | http://localhost:8082 | Inspect the `orders_log` Kafka topic |
| **CloudBeaver** | http://localhost:8978 | Query the Iceberg tables with **Trino** — no Flink involved |
| **Lakekeeper** | http://localhost:8181/ui | Browse the Iceberg REST catalog Fluss tiers into |
| **MinIO Console** | http://localhost:9001 (`admin` / `password`) | See the Parquet and metadata files on object storage |
| **Grafana** | http://localhost:3000 | (optional, Lab 5) a near-real-time order analytics dashboard |

Your instructor started the environment with `./start.sh --services-only`: everything is
running, and nothing has been submitted yet.

### Using the Flink SQL editor

- Type or paste SQL into the editor and press **Run** — or <kbd>⌘</kbd>/<kbd>Ctrl</kbd>+<kbd>Enter</kbd>.
  With text selected, only the selection runs; otherwise the whole editor runs, one
  statement after another, each with its own result panel.
- A streaming `SELECT` keeps its panel live until you press **Cancel**. Cancel it before
  moving on — each one is a real Flink job holding a slot.
- An `INSERT INTO` becomes a **long-running Flink job**. Its panel links to the job in
  the Flink Web UI, and it keeps running when you cancel, close the tab or start a new
  query. That is how a job is "deployed" here: there are no drafts and no deployments,
  the `INSERT` *is* the deployment.
- Every statement in this guide is also in the **Examples** menu, so you can load it
  instead of copying it.
- Your session survives a page reload. **New session** throws it away: the Kafka and CDC
  tables you created are gone with it, and you would have to create them again (the
  Fluss tables are not affected — more on that in Step 2).

---

## Lab 1: Fluss Catalog & Kafka Ingestion

In this lab you will set up the Fluss catalog, create the Fluss table order events will
land in, register the Kafka topic the generator is producing to, and run a simple job
that carries events from one to the other.

### Step 1: Create the Fluss catalog

Fluss is Apache's native streaming storage for the lakehouse. Creating the catalog
registers Fluss in Flink, so you can create and query Fluss tables from SQL.

Open the **Flink SQL editor** and run:

```sql
CREATE CATALOG IF NOT EXISTS fluss WITH (
  'type'              = 'fluss',
  'bootstrap.servers' = 'coordinator-server:9123'
);
```

> [!NOTE]
> Check it worked with `SHOW CATALOGS;` — you should see `default_catalog` and `fluss`.
> All Fluss tables in this lab live under `fluss.orders.*`.

### Step 2: Create the Fluss database and Log table

A Fluss **Log table** is an ordered, append-only store — every order event written to it
is kept and can be replayed from the beginning at any time. The `WATERMARK` declaration
lets this table drive time-based window operations later.

```sql
CREATE DATABASE IF NOT EXISTS fluss.orders;

CREATE TABLE IF NOT EXISTS fluss.orders.orders_log (
  `order_id`    STRING,
  `customer_id` STRING,
  `product_id`  STRING,
  `status`      STRING,
  `amount`      DECIMAL(10, 2),
  `event_time`  TIMESTAMP(3),
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
) WITH (
  'bucket.num' = '3'
);
```

`SHOW TABLES IN fluss.orders;` now lists `orders_log` — empty for now, since nothing is
writing to it yet.

> [!NOTE]
> **This table outlives your session.** It is stored in Fluss, not in Flink, so it is
> still there after *New session*, from the terminal SQL client, and after a restart. The
> `fluss` catalog registration survives too: the environment keeps catalog registrations
> in a file-based *CatalogStore*. The tables in the next step are different.

### Step 3: Create the Kafka source table

The generator doesn't write to Fluss directly — it publishes JSON events to the Kafka
topic `orders_log`. Register that topic as a Flink table:

```sql
CREATE TABLE IF NOT EXISTS orders_log_kafka (
  `order_id`    STRING,
  `customer_id` STRING,
  `product_id`  STRING,
  `status`      STRING,
  `amount`      DECIMAL(10, 2),
  `event_time`  TIMESTAMP(3),
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
) WITH (
  'connector'                    = 'kafka',
  'format'                       = 'json',
  'json.ignore-parse-errors'     = 'true',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id'          = 'flink-streaming-lakehouse',
  'scan.startup.mode'            = 'earliest-offset',
  'topic'                        = 'orders_log'
);
```

The generator emits `event_time` as `yyyy-MM-dd HH:mm:ss.SSS`, which is the JSON format's
default SQL timestamp shape. Flink parses it straight into `TIMESTAMP(3)` — no
intermediate `STRING` column or `TO_TIMESTAMP()` needed.

This table lives in Flink's `default_catalog`, which is **in memory and per session**.
Flink only holds the definition; there is nothing in Kafka to store it in.

> [!NOTE]
> Open the **Redpanda Console** → **Topics** → `orders_log` to see the raw JSON messages
> before they reach Flink.

### Step 4: Run the source → sink job

This is the simple source → sink job the rest of the workshop builds on: read every
event from the Kafka topic and insert it into the Fluss table.

```sql
SET 'pipeline.name' = 'kafka-to-fluss';

INSERT INTO fluss.orders.orders_log
SELECT
  `order_id`,
  `customer_id`,
  `product_id`,
  `status`,
  `amount`,
  `event_time`
FROM orders_log_kafka;
```

The result panel shows a link to the new job. Open the **Flink Web UI** — `kafka-to-fluss`
is listed under *Running Jobs*. Leave it running for the rest of the workshop.

> [!NOTE]
> `SET 'pipeline.name'` names the job in the Flink Web UI. It is a session setting, so it
> sticks: run `RESET 'pipeline.name';` afterwards, or your next queries will show up
> under the same name. The job examples in the menu do this for you.

### Step 5: Confirm order events are landing in Fluss

```sql
SELECT * FROM fluss.orders.orders_log;
```

You will see order events appear in real time, with `status` progressing through
`PLACED` → `PAID` → `SHIPPED` → `DELIVERED` (some orders show `CANCELLED` instead).
Press **Cancel** to stop the query.

> [!NOTE]
> Unlike Kafka, the Fluss Log table has no retention window to worry about. That makes it
> a reliable long-term event store, and it's exactly what makes tiering to Iceberg in
> Lab 3 safe: nothing is lost in the handoff. Kafka is the ingestion buffer; Fluss is the
> durable, queryable copy.

> [!TIP]
> Add `WHERE order_id = 'ORD-TEST'` to the query above. That order is never cancelled, so
> it always shows a full status history: `PLACED → PAID → SHIPPED → DELIVERED`.

---

## Lab 2: Product Enrichment

In this lab you will replicate a product dimension live from PostgreSQL via CDC, and
enrich the order stream with product details via a lookup join.

### Step 6: Create the product_lookup Fluss PK table

PostgreSQL already has a `product_catalog` table with 500 realistic products (name,
category, brand, price, cost, weight, rating, ...). Rather than re-typing that data into
Fluss, you'll replicate it live via **Change Data Capture** — any insert or update in
PostgreSQL shows up here automatically.

Create a Fluss **Primary Key (PK) table** with the same shape as `product_catalog`:

```sql
CREATE TABLE IF NOT EXISTS fluss.orders.product_lookup (
  `product_id`   STRING,
  `sku`          STRING,
  `product_name` STRING,
  `category`     STRING,
  `brand`        STRING,
  `unit_price`   DECIMAL(10, 2),
  `cost`         DECIMAL(10, 2),
  `weight_kg`    DECIMAL(6, 2),
  `in_stock`     BOOLEAN,
  `rating`       DECIMAL(2, 1),
  `created_at`   TIMESTAMP(3),
  PRIMARY KEY (`product_id`) NOT ENFORCED
) WITH (
  'bucket.num' = '3'
);
```

### Step 7: Create the postgres-cdc source table

The `postgres-cdc` connector snapshots the existing table on startup, then streams every
later change. PostgreSQL is already configured for it (logical WAL, a replication-enabled
`cdc_user`, a publication and a replication slot) — you only register the source:

```sql
CREATE TABLE IF NOT EXISTS product_catalog_cdc (
  `product_id`   STRING,
  `sku`          STRING,
  `product_name` STRING,
  `category`     STRING,
  `brand`        STRING,
  `unit_price`   DECIMAL(10, 2),
  `cost`         DECIMAL(10, 2),
  `weight_kg`    DECIMAL(6, 2),
  `in_stock`     BOOLEAN,
  `rating`       DECIMAL(2, 1),
  `created_at`   TIMESTAMP(3),
  PRIMARY KEY (`product_id`) NOT ENFORCED
) WITH (
  'connector'                 = 'postgres-cdc',
  'hostname'                  = 'postgres',
  'port'                      = '5432',
  'username'                  = 'cdc_user',
  'password'                  = 'admin1',
  'database-name'             = 'orders',
  'schema-name'               = 'public',
  'table-name'                = 'product_catalog',
  'slot.name'                 = 'flink_cdc_lakehouse_slot',
  'decoding.plugin.name'      = 'pgoutput',
  'debezium.publication.name' = 'all_tables_pub'
);
```

### Step 8: Start the CDC → Fluss sync job

```sql
SET 'pipeline.name' = 'pgcdc-to-fluss';

INSERT INTO fluss.orders.product_lookup
SELECT * FROM product_catalog_cdc;

RESET 'pipeline.name';
```

Another long-running job. It first snapshots all 500 products, then keeps
`product_lookup` in sync with every future change. Confirm it worked:

```sql
SELECT COUNT(*) AS products FROM fluss.orders.product_lookup;
```

A streaming `COUNT` is itself a changelog: you will watch it climb (the `-U`/`+U` rows)
and settle at `500`, the number of products this e-commerce company sells.

> [!TIP]
> Leave the count running and change a product in PostgreSQL from a terminal:
>
> ```bash
> docker compose exec postgres psql -U root -d orders -c \
>   "INSERT INTO product_catalog (product_id, product_name, category, unit_price)
>    VALUES ('PRD-00501', 'Workshop Mug', 'Home & Kitchen', 12.00);"
> ```
>
> The count moves to `501` within a few seconds, with no job restarted.

### Step 9: Create the orders_enriched Log table

This Log table stores every order event, enriched with product metadata. It is the table
you'll tier into Iceberg in the next lab — the single source of truth for both
real-time and historical analytics.

```sql
CREATE TABLE IF NOT EXISTS fluss.orders.orders_enriched (
  `order_id`     STRING,
  `customer_id`  STRING,
  `product_id`   STRING,
  `product_name` STRING,
  `category`     STRING,
  `status`       STRING,
  `amount`       DECIMAL(10, 2),
  `unit_price`   DECIMAL(10, 2),
  `event_time`   TIMESTAMP(3),
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
) WITH (
  'bucket.num' = '3'
);
```

### Step 10: Start the enrichment job

The `FOR SYSTEM_TIME AS OF t.proc_time` clause tells Flink to look up each order's
product in the `product_lookup` PK table at the moment the order event is processed.

```sql
SET 'pipeline.name' = 'product-enrichment';

INSERT INTO fluss.orders.orders_enriched
SELECT
  t.`order_id`,
  t.`customer_id`,
  t.`product_id`,
  p.`product_name`,
  p.`category`,
  t.`status`,
  t.`amount`,
  p.`unit_price`,
  t.`event_time`
FROM (
  SELECT *, PROCTIME() AS `proc_time`
  FROM fluss.orders.orders_log
) AS t
JOIN fluss.orders.product_lookup FOR SYSTEM_TIME AS OF t.`proc_time` AS p
  ON t.`product_id` = p.`product_id`;

RESET 'pipeline.name';
```

Once it's running, check that enriched events are flowing:

```sql
SELECT * FROM fluss.orders.orders_enriched;
```

Cancel the query once you see rows carrying `product_name` and `category`.

> [!WARNING]
> Don't simplify this to a plain `JOIN ... ON` without `FOR SYSTEM_TIME AS OF`.
> `product_lookup` is a PK (upsert) table, so a regular join against it produces a
> changelog that can include UPDATE/DELETE records — and `orders_enriched` is an
> append-only Log table, which can't accept them. Removing the clause fails with:
> `Table sink 'fluss.orders.orders_enriched' doesn't support consuming update and delete changes`.
> The lookup join is what makes this one insert-only output row per input row instead of
> a stateful two-sided join — it's required, not optional boilerplate.

---

## Lab 3: Enabling the Lakehouse — Tiering Fluss to Iceberg

`orders_enriched` now carries the complete, enriched order stream. This is where the
workshop's core idea comes in: Fluss can **continuously and automatically tier** a
table's data into Apache Iceberg, in the open Parquet + manifest format, without you
writing an ingestion job.

### Step 11: Start the Datalake Tiering Service

The **Fluss Datalake Tiering Service** is a long-running Flink job, shipped as a JAR by
the Fluss project. It reads new Fluss data and writes it out as native Iceberg data files
and snapshots, into the Lakekeeper Iceberg REST catalog, backed by MinIO object storage.

Your instructor has already **deployed** it: the JAR is uploaded to Flink and waiting to
run. You only have to start it.

In the **Flink SQL editor** header, find **Tiering service: deployed, not running** and
click **Start**. After a few seconds it turns green and says *running*, with a link to
the job. The **Flink Web UI** now lists a job named `Fluss Lake Tiering Service - iceberg`.
Leave it running — it will pick up every table you enable tiering on in the next step.

> [!TIP]
> The deployed JAR is also listed in the Flink Web UI under **Submit New Job**, which is
> what the Start button uses under the hood. If you prefer the terminal:
> `docker compose exec jobmanager /opt/tiering.sh`. The Start button and `tiering.sh` do
> nothing if a tiering service is already running. **Submit New Job does not check**:
> submitting from there a second time runs two tiering services that compete for the
> same tables, so use it only if the header says *not running*.

> [!NOTE]
> This is one service, shared by every Fluss table in the cluster. You don't deploy a
> new tiering job per table — you tell an existing table to opt in.

### Step 12: Enable tiering on orders_enriched

Enabling tiering is a single `ALTER TABLE`:

```sql
ALTER TABLE fluss.orders.orders_enriched
SET ('table.datalake.enabled' = 'true', 'table.datalake.freshness' = '30s');
```

`table.datalake.freshness` is how far behind Fluss the Iceberg copy is allowed to fall —
30 seconds here instead of the default 3 minutes, so you don't spend the workshop
staring at an empty Iceberg table.

> [!NOTE]
> Run `SHOW CREATE TABLE fluss.orders.orders_enriched;`. Besides `table.datalake.enabled`,
> you'll find `table.datalake.format = 'iceberg'` and the Lakekeeper settings — those were
> already there. The Fluss cluster is configured for Iceberg, and copies that
> configuration into every table when it is created. Enabling tiering on a table created
> *before* that configuration existed is not possible: it would have to be re-created.

### Step 13: Verify the tiered data landed in Iceberg

Give the tiering service a minute to complete its first round. Then open
**CloudBeaver**, select the **Trino — Lakehouse** connection in the left panel, click
**SQL** in the top toolbar to open an editor, and run:

```sql
SHOW TABLES FROM warehouse.orders;
```

You should see `orders_enriched`. Open **Lakekeeper** to browse the same catalog visually
— the `warehouse` warehouse, its `orders` namespace, and the table Fluss just tiered — and
the **MinIO Console** to see the actual files, under
`warehouse/lakehouse/orders/orders_enriched/`: `data/` holds Parquet, `metadata/` the
Iceberg snapshots and manifests.

> [!NOTE]
> If the table doesn't appear yet, the tiering service may still be starting — wait
> another minute and re-run the query. In the Flink Web UI, the tiering job's checkpoint
> count going up is a good sign: it commits to Iceberg on checkpoints.

### Step 14: Why this matters

Once a table is tiered, its data exists twice — but for two different purposes, not as a
wasteful copy:

- **Fluss** keeps the most recent data close, optimized for millisecond point lookups and
  low-latency streaming reads.
- **Iceberg** holds the full history in cheap, columnar object storage, readable by any
  Iceberg-compatible engine — Trino, Spark, DuckDB, or Flink itself — with no dependency
  on Fluss or Flink being up.

This is the "freshness + scale" promise of a streaming lakehouse: one pipeline, one table
definition, two storage tiers working together automatically.

---

## Lab 4: Querying the Lakehouse — Real-Time + Historical

Now that `orders_enriched` is tiering into Iceberg, let's see what that unlocks: the same
table, queried two different ways, for two different purposes.

### Step 15: Re-run the real-time query — this is now a union read

Back in the **Flink SQL editor**, run the exact query from Step 10 again:

```sql
SELECT * FROM fluss.orders.orders_enriched;
```

You didn't change a thing, but under the hood something important happened: Flink now
performs a **union read**. It reads the rows already tiered into Iceberg straight from
the Parquet files, and continues with the fresh rows that are still only in Fluss —
returned as one seamless result. No query rewrite, no manual `UNION`, no awareness
required that tiering is even happening.

You can also read the Iceberg side on its own, through the same catalog, by adding
`$lake` to the table name. It trails the Fluss side by up to one freshness interval:

```sql
SET 'execution.runtime-mode' = 'batch';

SELECT status, COUNT(*) AS events
FROM fluss.orders.`orders_enriched$lake`
GROUP BY status;

SET 'execution.runtime-mode' = 'streaming';
```

*Make sure the backtick characters are copied correctly.*

### Step 16: Query the same table from Trino

Switch to **CloudBeaver** and run:

```sql
SELECT * FROM warehouse.orders.orders_enriched LIMIT 20;
```

This is the same data — but Trino has no idea Fluss or Flink exist. It reads plain
Iceberg Parquet files and manifests through the Lakekeeper REST catalog. Note the three
columns at the end that Fluss added on the way in — `__bucket`, `__offset` and
`__timestamp` — recording exactly where each row came from in Fluss.

### Step 17: Run a historical analytics query

Because the tiered data lives in columnar Parquet, Trino can scan the *entire* order
history cheaply — no need to replay the whole Fluss log through a Flink job.

```sql
SELECT
  product_name,
  category,
  COUNT(*)      AS orders,
  SUM(amount)   AS revenue
FROM warehouse.orders.orders_enriched
WHERE status <> 'CANCELLED'
GROUP BY product_name, category
ORDER BY revenue DESC;
```

You'll see lifetime revenue per product, computed as a single batch scan.

### Step 18 (bonus): Time travel with Iceberg

Fluss gives you the current and recent past. Iceberg gives you something Fluss alone
doesn't: **time travel** — querying the table exactly as it looked at an earlier snapshot.
There is one snapshot per tiering commit. List them, from the Flink SQL editor:

```sql
SET 'execution.runtime-mode' = 'batch';

SELECT snapshot_id, committed_at, operation
FROM fluss.orders.`orders_enriched$lake$snapshots`
ORDER BY committed_at;

SET 'execution.runtime-mode' = 'streaming';
```

…or from Trino, where the same metadata table is called `orders_enriched$snapshots`:

```sql
SELECT snapshot_id, committed_at, summary['added-records'] AS added_records
FROM warehouse.orders."orders_enriched$snapshots"
ORDER BY committed_at;
```

Copy an older `snapshot_id`, substitute it below, and run it in **CloudBeaver**:

```sql
SELECT COUNT(*) AS orders_at_that_point
FROM warehouse.orders.orders_enriched FOR VERSION AS OF 1234567890123456789;
```

You get the row count as it was at that exact snapshot — a question Fluss's live tables
cannot answer, because Fluss doesn't keep point-in-time snapshots the way Iceberg does.
Trino can also travel by wall-clock time:
`FOR TIMESTAMP AS OF current_timestamp - INTERVAL '10' MINUTE`.

> [!TIP]
> Flink can read Iceberg directly too, with no Fluss in the path. The *Bonus: Iceberg's
> own catalog in Flink* and *Bonus: time travel through the Iceberg catalog* examples in
> the editor register Lakekeeper as a second Flink catalog and run this same query with a
> `/*+ OPTIONS('snapshot-id' = '...') */` hint.

---

## Lab 5: Freshness Dashboard in Grafana (Optional)

> [!NOTE]
> This lab is a stretch goal. If you're short on time, skip to the wrap-up — Labs 1
> through 4 already deliver the complete streaming lakehouse pattern.

### Step 19: Create a windowed order analytics job

This job aggregates `orders_enriched` into 5-minute tumbling windows per product category
and writes the results to PostgreSQL, where Grafana reads them. First, the sink — a
PostgreSQL table declared through Flink's JDBC connector:

```sql
CREATE TABLE IF NOT EXISTS revenue_5m_sink (
  `window_start`      TIMESTAMP(3),
  `window_end`        TIMESTAMP(3),
  `category`          STRING,
  `order_count`       BIGINT,
  `revenue`           DECIMAL(12, 2),
  `avg_order_value`   DECIMAL(10, 2),
  `max_order_value`   DECIMAL(10, 2),
  `avg_unit_price`    DECIMAL(10, 2),
  `paid_count`        BIGINT,
  `shipped_count`     BIGINT,
  `delivered_count`   BIGINT,
  `cancelled_count`   BIGINT,
  `delivered_revenue` DECIMAL(12, 2),
  `cancelled_revenue` DECIMAL(12, 2),
  `event_count`       BIGINT,
  `unique_customers`  BIGINT,
  `unique_products`   BIGINT,
  PRIMARY KEY (`window_start`, `window_end`, `category`) NOT ENFORCED
) WITH (
  'connector'  = 'jdbc',
  'url'        = 'jdbc:postgresql://postgres:5432/orders',
  'table-name' = 'public.revenue_5m',
  'username'   = 'root',
  'password'   = 'admin1'
);
```

Then the job:

```sql
SET 'pipeline.name' = 'revenue-analytics-sink';

INSERT INTO revenue_5m_sink
SELECT
  window_start,
  window_end,
  `category`,
  COUNT(*) FILTER (WHERE `status` = 'PLACED')                                        AS order_count,
  CAST(COALESCE(SUM(`amount`) FILTER (WHERE `status` = 'PLACED'), 0) AS DECIMAL(12, 2)) AS revenue,
  CAST(AVG(`amount`) FILTER (WHERE `status` = 'PLACED') AS DECIMAL(10, 2))           AS avg_order_value,
  MAX(`amount`) FILTER (WHERE `status` = 'PLACED')                                   AS max_order_value,
  CAST(AVG(`unit_price`) FILTER (WHERE `status` = 'PLACED') AS DECIMAL(10, 2))       AS avg_unit_price,
  COUNT(*) FILTER (WHERE `status` = 'PAID')                                          AS paid_count,
  COUNT(*) FILTER (WHERE `status` = 'SHIPPED')                                       AS shipped_count,
  COUNT(*) FILTER (WHERE `status` = 'DELIVERED')                                     AS delivered_count,
  COUNT(*) FILTER (WHERE `status` = 'CANCELLED')                                     AS cancelled_count,
  CAST(COALESCE(SUM(`amount`) FILTER (WHERE `status` = 'DELIVERED'), 0) AS DECIMAL(12, 2)) AS delivered_revenue,
  CAST(COALESCE(SUM(`amount`) FILTER (WHERE `status` = 'CANCELLED'), 0) AS DECIMAL(12, 2)) AS cancelled_revenue,
  COUNT(*)                                                                           AS event_count,
  COUNT(DISTINCT `customer_id`)                                                      AS unique_customers,
  COUNT(DISTINCT `product_id`)                                                       AS unique_products
FROM TABLE(
  TUMBLE(TABLE fluss.orders.orders_enriched, DESCRIPTOR(`event_time`), INTERVAL '5' MINUTE)
)
GROUP BY window_start, window_end, `category`;

RESET 'pipeline.name';
```

Every order emits one event per status it passes through, so each measure is scoped to a
single status with `FILTER (WHERE ...)`: `order_count` and `revenue` count what was
**booked** (`PLACED`), the `*_count` columns trace the **lifecycle**, and
`delivered_revenue` / `cancelled_revenue` what was fulfilled and lost. Summing `amount`
over every event would count each order up to four times.

Because `orders_enriched` is tiered, this job's read is a union read too: it starts from
the history in Iceberg, then continues from Fluss.

### Step 20: Explore the dashboard

Open **Grafana** → **Dashboards** → **Streaming Lakehouse — Order Analytics**. It is
already provisioned, wired to the `orders` database — nothing to import.

- **KPI row**: orders and revenue booked, average order value, revenue delivered, the
  cancellation rate, and how many distinct products sold in the last window.
- **Revenue per 5-minute window**: booked vs delivered vs cancelled, over time.
- **Order lifecycle per 5-minute window**: how many orders reached each status. The
  lines run close together in steady state; a gap that keeps widening means orders are
  piling up at a stage.
- **Revenue booked by category**, ranked, next to a **category × window heatmap** —
  darker cells mean more revenue — to spot which categories are heating up.
- **Cancellation rate** and **average order value by category**, ranked.
- **Category detail**: every measure in one sortable table.

A window is written once it closes, so the first rows appear about 5 minutes after the
job starts, and a new point every 5 minutes after that. Hover a panel title's ⓘ for what
each one measures.

Congrats, you made it! You have built a complete streaming lakehouse — from a Kafka-fed
generator, through Log and PK table storage in Fluss, to CDC-driven enrichment,
transparent tiering into Apache Iceberg, and real-time *and* historical queries against
the very same table.

---

## Bonus Track: Tiering an Upsert (PK) Table

Everything you tiered so far (`orders_enriched`) is a Log table — append-only, one row per
event. Fluss's tiering works just as well on **primary-key (upsert) tables**, where each
write replaces the previous value for that key. This bonus track builds a live
order-status table and tiers it with the exact same `ALTER TABLE`.

### Step 21: Create the order_status PK table

A PK table stores exactly one row per key: every write with the same key is an upsert.
This gives you a live view of each order's current state without accumulating duplicates.

```sql
CREATE TABLE IF NOT EXISTS fluss.orders.order_status (
  `order_id`    STRING,
  `customer_id` STRING,
  `status`      STRING,
  `amount`      DECIMAL(10, 2),
  `last_update` TIMESTAMP(3),
  PRIMARY KEY (`order_id`) NOT ENFORCED
) WITH (
  'bucket.num' = '3'
);
```

### Step 22: Start the state aggregation job

This job reads the Log table and writes the PK table. As an order advances from `PLACED`
to `DELIVERED`, its single row is updated in place.

```sql
SET 'pipeline.name' = 'order-status-sync';

INSERT INTO fluss.orders.order_status
SELECT
  `order_id`,
  `customer_id`,
  `status`,
  `amount`,
  `event_time` AS `last_update`
FROM fluss.orders.orders_log;

RESET 'pipeline.name';
```

### Step 23: Enable tiering on order_status

The same `ALTER TABLE` you ran in Lab 3 — no special-casing for upsert tables:

```sql
ALTER TABLE fluss.orders.order_status
SET ('table.datalake.enabled' = 'true', 'table.datalake.freshness' = '30s');
```

### Step 24: Verify order_status landed in Iceberg

Give the tiering service a minute, then from **CloudBeaver**:

```sql
SELECT * FROM warehouse.orders.order_status LIMIT 20;

SELECT status, COUNT(*) AS orders
FROM warehouse.orders.order_status
GROUP BY status;
```

You're looking at the *current* state of every order — one row per `order_id` — served
entirely out of Iceberg, independent of Fluss. The upsert semantics survive the trip:
each tiering commit writes the new row versions plus Iceberg **row-level delete files**
that retire the old ones. Look at the `operation` column of
`warehouse.orders."order_status$snapshots"` — it says `overwrite`, not `append` — and at
`warehouse.orders."order_status$files"`, where `content` 1 and 2 are the position- and
equality-delete files.
