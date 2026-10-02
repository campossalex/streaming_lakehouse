-- =====================================================================
-- External sources  (Lab 1 Step 3, Lab 2 Step 7)
--
-- These tables are NOT Fluss tables: they describe a Kafka topic and a PostgreSQL table
-- read through CDC. (Lab 5's PostgreSQL sink needs no table here: it comes from the
-- postgres catalog in ddl/03_postgres.sql.) They live in Flink's
-- default catalog, which is in-memory and per-session — so, unlike ddl/01_fluss.sql,
-- they vanish when the session ends and every new session needs them again. That is why
-- submit.sh hands every job this file with -i.
--
-- A running job is unaffected: at submission the planner bakes the connector options
-- into the JobGraph, and the cluster never consults a catalog again.
-- =====================================================================


-- ---------------------------------------- Step 3: the Kafka source table
-- The generator emits event_time as "yyyy-MM-dd HH:mm:ss.SSS" (Python's
-- isoformat(" ", "milliseconds")), the JSON format's default SQL timestamp shape, so it
-- parses straight into TIMESTAMP(3) — no STRING column or TO_TIMESTAMP() needed.
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


-- ------------------------------------ Step 7: the postgres-cdc source table
-- Snapshots product_catalog on startup, then streams every change. PostgreSQL was set
-- up for this by pg_streaming_lakehouse_ddl.sql: wal_level=logical, a replication user,
-- the slot and the publication named below.
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
