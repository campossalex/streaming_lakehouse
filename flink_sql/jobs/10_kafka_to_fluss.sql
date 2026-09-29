-- =====================================================================
-- JOB: kafka-to-fluss  (Lab 1, Step 4)
--
--   Kafka topic orders_log  ->  fluss.orders.orders_log
--
-- The "simple source -> sink job" the rest of the workshop builds on. Kafka is the
-- ingestion buffer; the Fluss Log table is the durable, queryable copy with no
-- retention window.
--
-- Submit on its own with:  docker compose exec sql-client /opt/submit.sh kafka
-- =====================================================================

SET 'pipeline.name' = 'kafka-to-fluss';

INSERT INTO fluss.orders.orders_log
SELECT
  `order_id`,
  `customer_id`,
  `product_id`,
  `status`,
  `amount`,
  `event_time`
FROM orders_log_kafka;
