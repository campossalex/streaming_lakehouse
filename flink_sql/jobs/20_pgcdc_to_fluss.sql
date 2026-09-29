-- =====================================================================
-- JOB: pgcdc-to-fluss  (Lab 2, Step 8)
--
--   PostgreSQL product_catalog (CDC)  ->  fluss.orders.product_lookup
--
-- Snapshots all 500 products, then keeps product_lookup in sync with every later
-- INSERT/UPDATE/DELETE in PostgreSQL. product_lookup is a PK table, so the changelog
-- lands as upserts and deletes.
--
-- Submit on its own with:  docker compose exec sql-client /opt/submit.sh cdc
-- =====================================================================

SET 'pipeline.name' = 'pgcdc-to-fluss';

INSERT INTO fluss.orders.product_lookup
SELECT * FROM product_catalog_cdc;
