-- =====================================================================
-- JOB: product-enrichment  (Lab 2, Step 10)
--
--   fluss.orders.orders_log  ⋈  fluss.orders.product_lookup  ->  fluss.orders.orders_enriched
--
-- FOR SYSTEM_TIME AS OF t.proc_time makes this a LOOKUP join: each order event looks up
-- its product in the product_lookup PK table at the moment it is processed.
--
-- Do not simplify it to a plain JOIN ... ON. product_lookup is an upsert table, so a
-- regular join produces UPDATE/DELETE records, and orders_enriched is an append-only Log
-- table that cannot accept them:
--   Table sink 'fluss.orders.orders_enriched' doesn't support consuming update and
--   delete changes
-- The lookup join emits one insert-only row per input row instead.
--
-- Submit on its own with:  docker compose exec sql-client /opt/submit.sh enrich
-- =====================================================================

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
