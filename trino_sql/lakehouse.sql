-- =====================================================================
-- Trino queries against the tiered Iceberg tables  (Lab 3 Step 13, Lab 4 Steps 16-18,
-- Bonus Step 24)
--
-- Run these in CloudBeaver (http://localhost:8978, connection "Trino — Lakehouse"), or
-- from a terminal:
--
--   docker compose exec trino trino --catalog lakehouse --schema orders
--
-- Trino knows nothing about Fluss or Flink: it reads Iceberg metadata from Lakekeeper
-- and Parquet files from MinIO. The tables appear here only once the tiering service is
-- running AND a table has been ALTERed to 'table.datalake.enabled' = 'true'.
-- =====================================================================

-- Step 13: the table Fluss just tiered. Empty until the first tiering commit.
SHOW TABLES FROM lakehouse.orders;

-- Step 16: the same rows Flink sees. Note the three extra columns Fluss adds on the way
-- in — __bucket, __offset and __timestamp — which record where each row came from.
SELECT * FROM lakehouse.orders.orders_enriched LIMIT 20;

-- Step 16: Trino sees only the Iceberg side, so this matches the $lake query in Flink
-- (Step 15), never the union read.
SELECT COUNT(*) AS events, MAX(event_time) AS latest_event
FROM lakehouse.orders.orders_enriched;

-- Step 17: lifetime delivered revenue per product, as one columnar batch scan of the
-- history. orders_enriched has one row per status EVENT, each with the full amount, so
-- the filter must pick a single status — `<> 'CANCELLED'` would count an order up to
-- four times. Use status = 'PLACED' for revenue booked.
SELECT
  product_name,
  category,
  COUNT(*)    AS orders,
  SUM(amount) AS revenue
FROM lakehouse.orders.orders_enriched
WHERE status = 'DELIVERED'
GROUP BY product_name, category
ORDER BY revenue DESC;

-- Step 18: one snapshot per tiering commit.
SELECT snapshot_id, committed_at, operation, summary['added-records'] AS added_records
FROM lakehouse.orders."orders_enriched$snapshots"
ORDER BY committed_at;

-- Step 18: time travel. Substitute a snapshot_id from the query above.
SELECT COUNT(*) AS events_at_that_point
FROM lakehouse.orders.orders_enriched FOR VERSION AS OF 1234567890123456789;

-- Step 18, the same by wall-clock time instead of id. The point in time must be after the
-- table's first snapshot, so keep the interval short on a freshly tiered table.
SELECT COUNT(*) AS events_two_minutes_ago
FROM lakehouse.orders.orders_enriched
FOR TIMESTAMP AS OF current_timestamp - INTERVAL '2' MINUTE;

-- The files behind the table: many small ones, one set per commit.
SELECT file_path, record_count, file_size_in_bytes
FROM lakehouse.orders."orders_enriched$files"
ORDER BY file_size_in_bytes DESC
LIMIT 20;

-- Bonus Step 24: the upsert table, tiered. Upserts become Iceberg row-level deletes
-- plus inserts, so this shows one row per order, like Fluss does.
SELECT * FROM lakehouse.orders.order_status LIMIT 20;

SELECT status, COUNT(*) AS orders
FROM lakehouse.orders.order_status
GROUP BY status;
