-- =====================================================================
-- JOB: order-status-sync  (Bonus Track, Step 22)
--
--   fluss.orders.orders_log  ->  fluss.orders.order_status  (PK table, one row per order)
--
-- Reads the Log table and upserts the PK table: as an order advances PLACED ->
-- DELIVERED, its single row is updated in place.
--
-- Submit on its own with:  docker compose exec sql-client /opt/submit.sh status
-- =====================================================================

SET 'pipeline.name' = 'order-status-sync';

INSERT INTO fluss.orders.order_status
SELECT
  `order_id`,
  `customer_id`,
  `status`,
  `amount`,
  `event_time` AS `last_update`
FROM fluss.orders.orders_log;
