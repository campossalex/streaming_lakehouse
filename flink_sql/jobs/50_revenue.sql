-- =====================================================================
-- JOB: revenue-analytics-sink  (Lab 5, Step 19 — optional)
--
--   fluss.orders.orders_enriched  ->  TUMBLE 1 min  ->  postgres.dwh.revenue_1m  ->  Grafana
--
-- One row per window and category. Each order emits an event per status it reaches,
-- so every measure is scoped to one status with FILTER (WHERE ...): order_count and
-- revenue count what was booked (PLACED), the *_count columns trace the lifecycle, and
-- delivered_ / cancelled_revenue what was fulfilled and lost. COUNT(DISTINCT ...) is
-- over every event in the window.
--
-- Once orders_enriched is tiered (Lab 3), this streaming read is a union read too: it
-- starts from the Iceberg snapshot and continues from Fluss where Iceberg ends. Every
-- window already in the history is written as soon as the job has caught up; after
-- that, each new window is written once it closes (1 minute plus the 5-second watermark
-- delay).
--
-- revenue_1m is the PostgreSQL table itself, through the postgres catalog
-- (ddl/03_postgres.sql); its primary key makes this an upsert sink.
--
-- Submit on its own with:  docker compose exec sql-client /opt/submit.sh revenue
-- =====================================================================

SET 'pipeline.name' = 'revenue-analytics-sink';

INSERT INTO postgres.dwh.revenue_1m
SELECT
  window_start,
  window_end,
  `category`,
  COUNT(*) FILTER (WHERE `status` = 'PLACED')                                        AS order_count,
  CAST(COALESCE(SUM(`amount`) FILTER (WHERE `status` = 'PLACED'), 0) AS DECIMAL(12, 2)) AS revenue,
  CAST(AVG(`amount`) FILTER (WHERE `status` = 'PLACED') AS DECIMAL(10, 2))           AS avg_order_value,
  MAX(`amount`) FILTER (WHERE `status` = 'PLACED')                                   AS max_order_value,
  CAST(AVG(`unit_price`) FILTER (WHERE `status` = 'PLACED') AS DECIMAL(10, 2))       AS avg_unit_price,
  COUNT(*) FILTER (WHERE `status` = 'PAID')                                          AS paid_count,
  COUNT(*) FILTER (WHERE `status` = 'SHIPPED')                                       AS shipped_count,
  COUNT(*) FILTER (WHERE `status` = 'DELIVERED')                                     AS delivered_count,
  COUNT(*) FILTER (WHERE `status` = 'CANCELLED')                                     AS cancelled_count,
  CAST(COALESCE(SUM(`amount`) FILTER (WHERE `status` = 'DELIVERED'), 0) AS DECIMAL(12, 2)) AS delivered_revenue,
  CAST(COALESCE(SUM(`amount`) FILTER (WHERE `status` = 'CANCELLED'), 0) AS DECIMAL(12, 2)) AS cancelled_revenue,
  COUNT(*)                                                                           AS event_count,
  COUNT(DISTINCT `customer_id`)                                                      AS unique_customers,
  COUNT(DISTINCT `product_id`)                                                       AS unique_products
FROM TABLE(
  TUMBLE(TABLE fluss.orders.orders_enriched, DESCRIPTOR(`event_time`), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end, `category`;
