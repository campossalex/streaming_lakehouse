-- The source system: the shop's operational PostgreSQL (service `postgres`, database `shop`).
--
-- Run once by the postgres container's docker-entrypoint-initdb.d, connected to the
-- `shop` database (POSTGRES_DB) as `root` (POSTGRES_USER, a superuser, for setup only).
-- A second `docker compose up` on the same volume skips it; `docker compose down -v`
-- re-runs it. The data warehouse is a separate server: pg_dwh_ddl.sql.
--
-- Two users, each with only what it needs:
--   shop_user  owns product_catalog: the "application", and CloudBeaver's connection,
--              so attendees can edit a product and watch CDC carry the change.
--   cdc_user   REPLICATION + SELECT: what the postgres-cdc source in
--              flink_sql/ddl/02_sources.sql connects as.
--
-- The CDC prerequisites — wal_level=logical, a replication user, a slot and a
-- publication — are set up here, except wal_level: it is a postmaster setting, so it is
-- on the server command line in docker-compose.yml.

CREATE USER shop_user WITH PASSWORD 'admin1';
CREATE USER cdc_user WITH PASSWORD 'admin1' REPLICATION;

-- Only these two can connect to the shop database.
REVOKE CONNECT ON DATABASE shop FROM PUBLIC;
GRANT CONNECT ON DATABASE shop TO shop_user, cdc_user;

-- ── Product catalog (Lab 2 — postgres-cdc source) ────────────────────────────
-- Source of truth for fluss.orders.product_lookup. Seeded with 500 synthetic
-- products (datagen/data/products.csv, mounted at /products.csv by docker-compose.yml)
-- so the postgres-cdc -> Fluss pipeline has something realistic to replicate.
CREATE TABLE product_catalog (
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

ALTER TABLE product_catalog OWNER TO shop_user;

-- The CDC user reads it: the snapshot phase is a SELECT, the stream comes from the slot.
GRANT USAGE ON SCHEMA public TO cdc_user;
GRANT SELECT ON product_catalog TO cdc_user;

-- Logical replication slot consumed by the Flink postgres-cdc connector. A slot belongs
-- to one database: this one. Pre-created so the lab's CDC table can name it; the
-- connector would otherwise create it on first start, which needs the connecting user
-- to own the publication too.
SELECT pg_create_logical_replication_slot('flink_cdc_lakehouse_slot', 'pgoutput');

-- The publication the connector reads through, via pgoutput.
CREATE PUBLICATION all_tables_pub FOR TABLE product_catalog;
