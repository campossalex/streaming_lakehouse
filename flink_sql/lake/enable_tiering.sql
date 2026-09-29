-- =====================================================================
-- Enable datalake tiering  (Lab 3 Step 12, Bonus Step 23)
--
-- Not a job: an ALTER TABLE is a metadata change in Fluss. The tiering service
-- (tiering.sh — one Flink job shared by every table) picks the table up on its next
-- round and starts committing it to Iceberg as warehouse.orders.<table>.
--
-- table.datalake.freshness is the target lag between Fluss and Iceberg. The tiering job
-- commits on Flink checkpoints, so the cluster's 30s checkpoint interval
-- (docker-compose.yml) is the floor.
--
-- Run with:  docker compose exec sql-client /opt/submit.sh lake
-- =====================================================================

ALTER TABLE fluss.orders.orders_enriched
SET ('table.datalake.enabled' = 'true', 'table.datalake.freshness' = '30s');

ALTER TABLE fluss.orders.order_status
SET ('table.datalake.enabled' = 'true', 'table.datalake.freshness' = '30s');
