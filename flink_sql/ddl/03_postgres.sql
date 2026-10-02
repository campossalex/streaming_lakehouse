-- =====================================================================
-- The PostgreSQL catalog  (Lab 5 Step 19)
--
-- A JDBC catalog exposes the tables of PostgreSQL's `orders` database to Flink as
-- postgres.orders.<table>, with their columns and primary keys read from PostgreSQL
-- itself. Lab 5 writes to postgres.orders.revenue_5m through it, so there is no sink
-- table to declare: revenue_5m's PRIMARY KEY (window_start, window_end, category) comes
-- with it, and that key is what makes the JDBC sink upsert each window's row.
--
-- Like the fluss catalog, the registration is kept by the file-based CatalogStore
-- (docker-compose.yml), so it survives New session and gateway restarts.
--
-- Reading product_catalog through this catalog would be a one-off JDBC scan, not change
-- capture: the CDC source in ddl/02_sources.sql stays a table of its own.
--
-- Needs flink-connector-jdbc-core + flink-connector-jdbc-postgres, not the all-in-one
-- flink-connector-jdbc 3.3.0 JAR — see download-jars.sh.
-- =====================================================================

CREATE CATALOG IF NOT EXISTS postgres WITH (
  'type'             = 'jdbc',
  'base-url'         = 'jdbc:postgresql://postgres:5432',
  'default-database' = 'orders',
  'username'         = 'root',
  'password'         = 'admin1'
);
