#!/usr/bin/env bash
# Submits the pipeline as separate Flink jobs, one per lab step, sharing one set of DDL.
# Runs INSIDE the sql-client container, where it is mounted at /opt/submit.sh:
#
#   docker compose exec sql-client /opt/submit.sh                 all of them, in order
#   docker compose exec sql-client /opt/submit.sh kafka cdc       some, by name
#
# Names, in lab order:
#   kafka    jobs/10_kafka_to_fluss.sql   Lab 1   Kafka orders_log -> fluss.orders.orders_log
#   cdc      jobs/20_pgcdc_to_fluss.sql   Lab 2   Postgres CDC     -> fluss.orders.product_lookup
#   enrich   jobs/30_enrichment.sql       Lab 2   lookup join      -> fluss.orders.orders_enriched
#   lake     lake/enable_tiering.sql      Lab 3   ALTER TABLE ... 'table.datalake.enabled' (not a job)
#   status   jobs/40_order_status.sql     Bonus   orders_log       -> fluss.orders.order_status
#   revenue  jobs/50_revenue.sql          Lab 5   TUMBLE 1 min     -> Postgres revenue_1m
#
# The tiering service is not in this list: it is a JAR, not SQL. See tiering.sh.
#
# Why the DDL is passed with -i
# -----------------------------
# The Kafka and CDC tables in ddl/02_sources.sql live in Flink's default catalog,
# which is in-memory and per-session, so a table created by one submission does not exist
# for the next. `sql-client.sh -i` loads the DDL into each session before running the -f
# script. ddl/01_fluss.sql rides along: it is IF NOT EXISTS throughout, so after the first
# run it changes nothing.
set -euo pipefail

readonly SQL_DIR=/opt/sql

JOBS=("$@")
[ ${#JOBS[@]} -eq 0 ] && JOBS=(kafka cdc enrich lake status revenue)

WORK="$(mktemp -d /tmp/pipeline.XXXXXX)"
trap 'rm -rf "$WORK"' EXIT

# -i takes ONE file, not a list. Concatenating the DDL in sorted order gives the same
# result with no ambiguity about ordering.
INIT="$WORK/init.sql"
for f in "$SQL_DIR"/ddl/*.sql; do
  printf -- '-- ===== %s =====\n' "$(basename "$f")" >> "$INIT"
  cat "$f" >> "$INIT"
  printf '\n' >> "$INIT"
done

job_file() {
  case "$1" in
    kafka)   echo "$SQL_DIR/jobs/10_kafka_to_fluss.sql" ;;
    cdc)     echo "$SQL_DIR/jobs/20_pgcdc_to_fluss.sql" ;;
    enrich)  echo "$SQL_DIR/jobs/30_enrichment.sql" ;;
    lake)    echo "$SQL_DIR/lake/enable_tiering.sql" ;;
    status)  echo "$SQL_DIR/jobs/40_order_status.sql" ;;
    revenue) echo "$SQL_DIR/jobs/50_revenue.sql" ;;
    *)       return 1 ;;
  esac
}

FAILED=""

submit_one() {
  local name="$1" file out
  if ! file="$(job_file "$name")"; then
    echo "unknown job '$name' (expected: kafka, cdc, enrich, lake, status, revenue)" >&2
    FAILED="$FAILED $name"
    return
  fi

  echo
  echo "--- ${name}: $(basename "$file") ---"
  out="$(/opt/flink/bin/sql-client.sh -i "$INIT" -f "$file" 2>&1)" || true
  printf '%s\n' "$out" | grep -vE '^(WARNING|[A-Z][a-z]{2} [0-9]{2}, [0-9]{4})' || true

  # Judge by evidence, not by exit status: sql-client exits 0 after
  # "[ERROR] Could not execute SQL statement" as well.
  if printf '%s\n' "$out" | grep -q '\[ERROR\]'; then
    echo "  !! ${name} failed — see the [ERROR] above" >&2
    FAILED="$FAILED $name"
  elif [ "$name" != "lake" ] && ! printf '%s\n' "$out" | grep -qE 'Job ID: *[0-9a-f]{32}|JobID [0-9a-f]{32}'; then
    echo "  !! ${name} did not produce a Job ID — nothing was submitted" >&2
    FAILED="$FAILED $name"
  fi
}

for name in "${JOBS[@]}"; do submit_one "$name"; done

echo
if [ -n "$FAILED" ]; then
  echo "FAILED:${FAILED}" >&2
  exit 1
fi
echo "all requested steps done"
