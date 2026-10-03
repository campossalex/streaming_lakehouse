-- The data warehouse: a PostgreSQL server of its own (service `postgres-dwh`, database
-- `dwh`), separate from the shop's source database (pg_shop_ddl.sql).
--
-- Run once by the postgres-dwh container's docker-entrypoint-initdb.d, connected to the
-- `dwh` database (POSTGRES_DB) as `root` (POSTGRES_USER, a superuser, for setup only).
-- A second `docker compose up` on the same volume skips it; `docker compose down -v`
-- re-runs it.
--
-- One user, dwh_user, owns revenue_5m. Flink's `postgres` JDBC catalog
-- (flink_sql/ddl/03_postgres.sql), Grafana and CloudBeaver all connect as it.

CREATE USER dwh_user WITH PASSWORD 'admin1';

-- Only dwh_user can connect to the warehouse.
REVOKE CONNECT ON DATABASE dwh FROM PUBLIC;
GRANT CONNECT ON DATABASE dwh TO dwh_user;

-- Flink's PostgreSQL catalog lists every database on the server (SELECT datname FROM
-- pg_database). Without the default `postgres` database, it lists only `dwh`.
DROP DATABASE postgres;

-- ── Order analytics (Lab 5) ───────────────────────────────────────────────────
-- One row per 5-minute tumbling window and product category, written by the
-- revenue-analytics-sink job (flink_sql/jobs/50_revenue.sql). Feeds the Grafana
-- "Order Analytics" dashboard.
--
-- Every order emits one event per status it passes through, so each measure below is
-- scoped to ONE status: revenue is what was booked (PLACED), delivered_revenue what was
-- fulfilled (DELIVERED), cancelled_revenue what was lost. Summing every event's amount
-- would count each order up to four times.
CREATE TABLE revenue_5m (
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

ALTER TABLE revenue_5m OWNER TO dwh_user;
