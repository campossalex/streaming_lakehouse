-- =====================================================================
-- External sources and sinks  (Lab 1 Step 3, Lab 2 Step 7, Lab 5 Step 19)
--
-- These tables are NOT Fluss tables: they describe a Kafka topic, a PostgreSQL table
-- read through CDC, and a PostgreSQL table written through JDBC. They live in Flink's
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


-- -------------------------------------- Step 19: the revenue_5m JDBC sink
-- Column meanings are documented on the PostgreSQL table, in pg_streaming_lakehouse_ddl.sql.
-- Declared as a plain jdbc table rather than through a JDBC catalog: the PRIMARY KEY is
-- what makes it an upsert sink, and a reflected catalog table gives no control over it.
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
