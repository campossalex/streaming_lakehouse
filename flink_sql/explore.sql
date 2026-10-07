-- =====================================================================
-- Interactive queries  (the SELECT and ALTER steps of every lab)
--
-- NOT submitted by start.sh, and deliberately outside ddl/ and jobs/ so submit.sh never
-- picks it up. Each `-- ---- title` section below is one entry in the web SQL editor's
-- examples menu (http://localhost:8084). From a terminal instead:
--
--   docker compose exec sql-client /opt/flink/bin/sql-client.sh -i /opt/sql/ddl/02_sources.sql
--
-- Only 02_sources.sql is needed there: the fluss catalog registration is kept by the
-- CatalogStore, and the Fluss tables by Fluss. Press `q` to leave a result grid.
-- =====================================================================


-- ------------------------------------- Lab 1, Step 5: events landing in Fluss
-- Order events appear in real time, status moving PLACED -> PAID -> SHIPPED ->
-- DELIVERED (a few CANCELLED). Cancel the query once you have seen enough.
SELECT * FROM fluss.orders.orders_log;


-- --------------------------------------- Lab 1, Step 5: the demo order's history
-- ORD-TEST is the generator's fixed demo order: it is never cancelled, so it always
-- shows the full lifecycle, one status at a time as the generator advances it.
SELECT * FROM fluss.orders.orders_log WHERE order_id = 'ORD-TEST';


-- ----------------------------------------- Lab 2, Step 8: products replicated
-- A streaming COUNT: it climbs while the CDC snapshot lands, then settles at 500 and
-- stays live — change a row in PostgreSQL and it is reflected here.
SELECT COUNT(*) AS products FROM fluss.orders.product_lookup;


-- ----------------------------------------- Lab 2, Step 10: enriched events
-- Rows now carry product_name and category from product_lookup.
SELECT * FROM fluss.orders.orders_enriched;


-- ---------------------------------------- Lab 3, Step 11: enable tiering
-- A single ALTER TABLE: it only marks the table. Nothing reaches Iceberg until the
-- tiering service runs — start it next (Step 12: the Start button in the header).
ALTER TABLE fluss.orders.orders_enriched
SET ('table.datalake.enabled' = 'true', 'table.datalake.freshness' = '30s');


-- --------------------------------------- Lab 3: what the ALTER changed
-- table.datalake.enabled is now true. The table.datalake.format and the
-- table.datalake.iceberg.* options were already there: they were copied from the Fluss
-- servers' configuration when the table was created.
SHOW CREATE TABLE fluss.orders.orders_enriched;


-- ------------------------------------ Lab 4, Step 15: one table, two tiers
-- $lake reads the Iceberg side alone; the plain table name is a union read of Iceberg
-- plus the rows still only in Fluss. The union read has more events and a latest_event
-- only seconds old; $lake trails it by up to one tiering commit. Run both again: the
-- union read grows every time, $lake jumps only when a commit lands.
SET 'execution.runtime-mode' = 'batch';

-- Lake layer only (Iceberg): the rows tiering has committed so far
SELECT COUNT(*) AS events, MAX(event_time) AS latest_event
FROM fluss.orders.`orders_enriched$lake`;

-- Union read, both layers: Iceberg + the rows still only in Fluss
SELECT COUNT(*) AS events, MAX(event_time) AS latest_event
FROM fluss.orders.orders_enriched;

SET 'execution.runtime-mode' = 'streaming';


-- ------------------------------- Lab 4, Step 15: a streaming union read
-- The same query as Step 10. It now starts from the rows in Iceberg and carries on with
-- live rows from Fluss. Expect 20-60 s before the first rows: the Iceberg side is
-- planned first.
SELECT * FROM fluss.orders.orders_enriched;


-- ----------------------------- Lab 4, Step 18: Iceberg snapshots, seen from Flink
-- One snapshot per tiering commit. Copy an older snapshot_id for the time-travel query
-- in trino_sql/lakehouse.sql, or for the Iceberg catalog query below.
SET 'execution.runtime-mode' = 'batch';
SELECT snapshot_id, committed_at, operation
FROM fluss.orders.`orders_enriched$lake$snapshots`
ORDER BY committed_at;
SET 'execution.runtime-mode' = 'streaming';


-- ------------------------------------ Bonus: Iceberg's own catalog in Flink
-- The lake is plain Iceberg: any engine with a REST catalog client can read it, Flink
-- included, with no Fluss in the path. This registers Lakekeeper as a second catalog.
-- The S3 keys are needed because the warehouse vends no credentials (sts-enabled=false).
CREATE CATALOG IF NOT EXISTS iceberg WITH (
  'type'                 = 'iceberg',
  'catalog-type'         = 'rest',
  'uri'                  = 'http://lakekeeper:8181/catalog',
  'warehouse'            = 'lakehouse',
  'io-impl'              = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint'          = 'http://minio:9000',
  's3.access-key-id'     = 'admin',
  's3.secret-access-key' = 'password',
  's3.path-style-access' = 'true',
  'client.region'        = 'us-east-1'
);

SHOW TABLES IN iceberg.orders;


-- ---------------------------------- Bonus: time travel through the Iceberg catalog
-- Replace the snapshot id with one from the $lake$snapshots query above.
SET 'execution.runtime-mode' = 'batch';
SELECT COUNT(*) AS events_at_that_point
FROM iceberg.orders.orders_enriched /*+ OPTIONS('snapshot-id' = '1234567890123456789') */;
SET 'execution.runtime-mode' = 'streaming';
