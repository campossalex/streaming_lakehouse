-- =====================================================================
-- The Fluss catalog and every Fluss table  (Lab 1 Steps 1-2, Lab 2 Steps 6 and 9,
-- Bonus Step 21)
--
-- Unlike ddl/02_sources.sql, nothing here is lost when a session ends:
--
--   - The tables live in Fluss itself. Once created, fluss.orders.* is there for every
--     session, every job and every restart of the SQL Gateway.
--   - The `fluss` catalog REGISTRATION survives too: docker-compose.yml enables a
--     file-based CatalogStore, shared by sql-client and sql-gateway, which persists
--     catalog configurations.
--
-- So this file only really needs to run once. It is still loaded into every job's
-- session by submit.sh (with -i), and every statement is IF NOT EXISTS so that replaying
-- it is a no-op.
--
-- Fluss is started with datalake.format = iceberg (docker-compose.yml), which is copied
-- into each table's properties here, at CREATE TABLE time. That is what later lets
-- Lab 3 tier orders_enriched and order_status with a single ALTER TABLE.
-- =====================================================================


-- ------------------------------------------------ Step 1: the Fluss catalog
CREATE CATALOG IF NOT EXISTS fluss WITH (
  'type'              = 'fluss',
  'bootstrap.servers' = 'coordinator-server:9123'
);


-- ------------------------------------------- Step 2: database + the Log table
-- A Fluss Log table is an ordered, append-only store. The WATERMARK lets it drive
-- event-time windows later (Lab 5).
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


-- ---------------------------------------- Step 6: product_lookup (PK table)
-- Same shape as PostgreSQL's product_catalog, which Lab 2 replicates into it via CDC.
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


-- ------------------------------------- Step 9: orders_enriched (Log table)
-- Every order event, enriched with product metadata. The table Lab 3 tiers to Iceberg.
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


-- ----------------------------------- Bonus Step 21: order_status (PK table)
-- One row per order, upserted in place as it moves PLACED -> DELIVERED.
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
