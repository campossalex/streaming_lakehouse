-- PostgreSQL setup for the streaming_lakehouse_oss scenario.
--
-- Run once by the postgres container's docker-entrypoint-initdb.d, connected to the
-- `orders` database (POSTGRES_DB) as `root` (POSTGRES_USER, a superuser). A second
-- `docker compose up` on the same volume skips it; `docker compose down -v` re-runs it.
--
-- The CDC prerequisites below — wal_level=logical, a replication user, a slot and a
-- publication — are what the postgres-cdc source in flink_sql/ddl/02_sources.sql needs.
-- wal_level is set on the server command line in docker-compose.yml, not here: it is a
-- postmaster setting and cannot be changed from SQL.

-- CDC user required by the postgres-cdc Flink connector (Lab 2)
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_user WHERE usename = 'cdc_user') THEN
    CREATE USER cdc_user WITH PASSWORD 'admin1' REPLICATION;
  END IF;
END
$$;

GRANT CONNECT ON DATABASE orders TO cdc_user;

-- ── Order analytics sink (Lab 5, optional) ───────────────────────────────────
-- One row per 5-minute tumbling window and product category, written by the
-- revenue-analytics-sink job (flink_sql/jobs/50_revenue.sql). Feeds the Grafana
-- "Order Analytics" dashboard.
--
-- Every order emits one event per status it passes through, so each measure below is
-- scoped to ONE status: revenue is what was booked (PLACED), delivered_revenue what was
-- fulfilled (DELIVERED), cancelled_revenue what was lost. Summing every event's amount
-- would count each order up to four times.
CREATE TABLE IF NOT EXISTS revenue_5m (
    window_start       TIMESTAMP      NOT NULL,
    window_end         TIMESTAMP      NOT NULL,
    category           VARCHAR(30)    NOT NULL,
    -- orders booked in the window (PLACED events)
    order_count        BIGINT,
    revenue            NUMERIC(12, 2),
    avg_order_value    NUMERIC(10, 2),
    max_order_value    NUMERIC(10, 2),
    avg_unit_price     NUMERIC(10, 2),
    -- the order lifecycle: how many orders reached each later status in the window
    paid_count         BIGINT,
    shipped_count      BIGINT,
    delivered_count    BIGINT,
    cancelled_count    BIGINT,
    delivered_revenue  NUMERIC(12, 2),
    cancelled_revenue  NUMERIC(12, 2),
    -- breadth of activity, across every event in the window
    event_count        BIGINT,
    unique_customers   BIGINT,
    unique_products    BIGINT,
    PRIMARY KEY (window_start, window_end, category)
);

-- ── Product catalog (Lab 2 — postgres-cdc source) ────────────────────────────
-- Source of truth for fluss.orders.product_lookup. Seeded with 500 synthetic
-- products (datagen/data/products.csv, mounted at /products.csv by docker-compose.yml)
-- so the postgres-cdc -> Fluss pipeline has something realistic to replicate.
CREATE TABLE IF NOT EXISTS product_catalog (
    product_id    VARCHAR(20)   PRIMARY KEY,
    sku           VARCHAR(20),
    product_name  VARCHAR(255),
    category      VARCHAR(50),
    brand         VARCHAR(100),
    unit_price    NUMERIC(10, 2),
    cost          NUMERIC(10, 2),
    weight_kg     NUMERIC(6, 2),
    in_stock      BOOLEAN,
    rating        NUMERIC(2, 1),
    created_at    TIMESTAMP
);

-- Emitting full before-images makes UPDATEs arrive in Flink as a proper
-- -U/+U pair rather than an upsert with no prior row. Not strictly needed with a
-- primary key, but it is what the connector's documentation recommends.
ALTER TABLE product_catalog REPLICA IDENTITY FULL;

\copy product_catalog (product_id, sku, product_name, category, brand, unit_price, cost, weight_kg, in_stock, rating, created_at) FROM '/products.csv' WITH (FORMAT csv, HEADER true)

-- Grant CDC user read access (needed by the postgres-cdc connector's snapshot phase)
GRANT USAGE ON SCHEMA public TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO cdc_user;

-- Logical replication slot consumed by the Flink postgres-cdc connector.
-- Pre-created so the lab's CDC table can name it; the connector would otherwise create
-- it on first start, which needs the connecting user to own the publication too.
SELECT pg_create_logical_replication_slot('flink_cdc_lakehouse_slot', 'pgoutput')
WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'flink_cdc_lakehouse_slot');

-- Publication that exposes the tables to the CDC connector via pgoutput
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'all_tables_pub') THEN
    EXECUTE 'CREATE PUBLICATION all_tables_pub FOR ALL TABLES';
  END IF;
END
$$;
