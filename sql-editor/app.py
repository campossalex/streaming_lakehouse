"""Web SQL editor for the streaming lakehouse workshop.

A thin proxy in front of the Flink SQL Gateway. The browser cannot call the gateway
directly (it sends no CORS headers), and a gateway session starts as empty as a bare
`sql-client.sh`: the default catalog is in-memory, so the Kafka and CDC source tables in
flink_sql/ddl/02_sources.sql are needed again in every new session. The Fluss catalog is
not — its registration is kept by the shared CatalogStore, and its tables by Fluss.

  - optionally (PRELOAD_DDL=true) runs flink_sql/ddl/*.sql into each new session first,
    like `sql-client.sh -i`. Off by default: in the workshop attendees issue the DDL
    themselves. Every statement in those files is IF NOT EXISTS, so a preloaded session
    and an attendee's own CREATE TABLE do not collide.
  - builds the examples menu from flink_sql/: the DDL files, the explore.sql sections,
    and the job files.

This workshop has no secrets, so there is no placeholder substitution and nothing to
redact: statements reach the gateway verbatim.
"""
import glob
import hashlib
import hmac
import json
import os
import re
import threading
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from urllib.parse import quote, urlparse

import requests
from flask import Flask, Response, jsonify, request, send_from_directory

GATEWAY = os.environ.get("GATEWAY_URL", "http://sql-gateway:8083").rstrip("/")
SQL_DIR = os.environ.get("SQL_DIR", "/opt/sql")
PRELOAD_DDL = os.environ.get("PRELOAD_DDL", "false").lower() in ("1", "true", "yes")
FLINK = os.environ.get("FLINK_URL", "http://jobmanager:8081").rstrip("/")
TIERING_JAR_DIR = os.environ.get("TIERING_JAR_DIR", "/opt/tiering")
TIERING_ARGS = os.environ.get("TIERING_ARGS", "/opt/tiering.args")
LAKEKEEPER = os.environ.get("LAKEKEEPER_URL", "http://lakekeeper:8181").rstrip("/")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000").rstrip("/")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "admin")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "password")
S3_REGION = os.environ.get("S3_REGION", "us-east-1")
# Flink's built-in, in-memory catalog: table.builtin-catalog-name in docker-compose.yml.
BUILTIN_CATALOG = os.environ.get("BUILTIN_CATALOG", "source_catalog")

app = Flask(__name__, static_folder="static")


# ── SQL text handling ─────────────────────────────────────────────────────────

def split_statements(sql):
    """Split a script into statements on `;`, dropping comments.

    A character scanner rather than str.split: string literals can hold `;` or `--`,
    and explore.sql puts a `--` comment inside a WITH (...) clause.
    """
    out, buf, i, n = [], [], 0, len(sql)
    while i < n:
        c = sql[i]
        if c in ("'", "`", '"'):
            j = i + 1
            while j < n:
                if sql[j] == c:
                    if j + 1 < n and sql[j + 1] == c:  # doubled quote is an escape
                        j += 2
                        continue
                    break
                j += 1
            buf.append(sql[i:j + 1])
            i = j + 1
        elif sql.startswith("--", i):
            j = sql.find("\n", i)
            i = n if j < 0 else j
        elif sql.startswith("/*+", i):
            # A hint, not a comment: `/*+ OPTIONS('snapshot-id' = '...') */` is part of
            # the statement, and dropping it would silently run a different query.
            j = sql.find("*/", i + 3)
            j = n if j < 0 else j + 2
            buf.append(sql[i:j])
            i = j
        elif sql.startswith("/*", i):
            j = sql.find("*/", i + 2)
            i = n if j < 0 else j + 2
        elif c == ";":
            out.append("".join(buf))
            buf = []
            i += 1
        else:
            buf.append(c)
            i += 1
    out.append("".join(buf))
    return [s.strip() for s in out if s.strip()]


def ddl_files():
    return sorted(glob.glob(os.path.join(SQL_DIR, "ddl", "*.sql")))


def render_ddl():
    statements = []
    for path in ddl_files():
        with open(path) as f:
            statements += split_statements(f.read())
    return statements


DDL = render_ddl()

BANNER = re.compile(r"^-- -{3,} (.+?)\s*$")


PIPELINE_NAME = re.compile(r"SET\s+'pipeline\.name'\s*=\s*'([^']+)'", re.I)

# Appended to each job example. The job file's SET 'pipeline.name' is a session option,
# so without this every SELECT run afterwards would show up in the Flink UI under the
# job's name.
RESET_NAME = """
-- Added by the editor: stop later statements in this session inheriting the job name.
RESET 'pipeline.name';
"""


# The examples menu, in WORKSHOP.md's order — the same order as the pipeline page: each
# lab's tables, sources, jobs and queries together, labelled with their step. Entries
# point into the SQL files by section title, so the SQL itself lives in one place.
#   ("ddl", file, section prefix)  a section of a ddl/ file
#   ("file", path)                 a whole file
#   ("job", file)                  a jobs/ file, plus a RESET of its pipeline.name
#   ("explore", section prefix)    a section of explore.sql
#   ("note", sql)                  guidance with nothing to run
MENU = [
    ("Lab 1 · Kafka → Fluss", [
        ("Step 1 · the Fluss catalog", ("ddl", "01_fluss.sql", "Step 1:")),
        ("Step 2 · database + orders_log", ("ddl", "01_fluss.sql", "Step 2:")),
        ("Step 3 · the Kafka source table", ("ddl", "02_sources.sql", "Step 3:")),
        ("Step 4 · job: kafka-to-fluss", ("job", "10_kafka_to_fluss.sql")),
        ("Step 5 · events landing in Fluss", ("explore", "Lab 1, Step 5: events landing")),
        ("Step 5 · the demo order's history", ("explore", "Lab 1, Step 5: the demo order")),
    ]),
    ("Lab 2 · Product enrichment", [
        ("Step 6 · product_lookup (PK table)", ("ddl", "01_fluss.sql", "Step 6:")),
        ("Step 7 · the postgres-cdc source table", ("ddl", "02_sources.sql", "Step 7:")),
        ("Step 8 · job: pgcdc-to-fluss", ("job", "20_pgcdc_to_fluss.sql")),
        ("Step 8 · products replicated", ("explore", "Lab 2, Step 8:")),
        ("Step 9 · orders_enriched (Log table)", ("ddl", "01_fluss.sql", "Step 9:")),
        ("Step 10 · job: product-enrichment", ("job", "30_enrichment.sql")),
        ("Step 10 · enriched events", ("explore", "Lab 2, Step 10:")),
    ]),
    ("Lab 3 · Tiering to Iceberg", [
        ("Step 11 · enable tiering", ("explore", "Lab 3, Step 11:")),
        ("Step 11 · what the ALTER changed", ("explore", "Lab 3: what the ALTER changed")),
        ("Step 12 · start the tiering service", ("note",
            "-- Step 12: start the Fluss Datalake Tiering Service.\n"
            "-- It is a JAR, not SQL: click Start next to \"Tiering service\" in this editor's\n"
            "-- header. From a terminal instead:  docker compose exec jobmanager /opt/tiering.sh\n")),
    ]),
    ("Lab 4 · Querying the lakehouse", [
        ("Step 15 · one table, two tiers", ("explore", "Lab 4, Step 15: one table")),
        ("Step 15 · a streaming union read", ("explore", "Lab 4, Step 15: a streaming")),
        ("Step 18 · Iceberg snapshots, seen from Flink", ("explore", "Lab 4, Step 18:")),
        ("Step 18 bonus · Iceberg's own catalog in Flink", ("explore", "Bonus: Iceberg's own catalog")),
        ("Step 18 bonus · time travel through the Iceberg catalog", ("explore", "Bonus: time travel")),
    ]),
    ("Lab 5 · Revenue to Grafana", [
        ("Step 19 · the postgres catalog", ("file", "ddl/03_postgres.sql")),
        ("Step 19 · job: revenue-analytics-sink", ("job", "50_revenue.sql")),
    ]),
]


def job_example(name):
    sql = read_sql(os.path.join("jobs", name))
    return sql.rstrip() + "\n" + (RESET_NAME if PIPELINE_NAME.search(sql) else "")


def load_examples():
    """The examples menu: MENU in lab order, then the complete DDL files. A job or an
    explore.sql section MENU does not list lands in "Other", so none goes missing."""
    explore = load_sections(os.path.join(SQL_DIR, "explore.sql"))
    used_explore, used_jobs, examples = set(), set(), []
    for group, items in MENU:
        for title, ref in items:
            kind = ref[0]
            if kind == "ddl":
                body = next(b for t, b in load_sections(os.path.join(SQL_DIR, "ddl", ref[1])) if t.startswith(ref[2]))
            elif kind == "file":
                body = read_sql(ref[1])
            elif kind == "job":
                body = job_example(ref[1]); used_jobs.add(ref[1])
            elif kind == "explore":
                t, body = next((t, b) for t, b in explore if t.startswith(ref[1])); used_explore.add(t)
            else:
                body = ref[1]
            examples.append({"group": group, "title": title, "sql": body})
    for t, b in explore:
        if t not in used_explore:
            examples.append({"group": "Other", "title": t, "sql": b})
    for path in sorted(glob.glob(os.path.join(SQL_DIR, "jobs", "*.sql"))):
        if os.path.basename(path) not in used_jobs:
            examples.append({"group": "Other", "title": os.path.basename(path), "sql": job_example(os.path.basename(path))})
    for path in ddl_files():   # all of a file at once, for loading everything in one run
        with open(path) as f:
            examples.append({"group": "Complete DDL files", "title": os.path.basename(path), "sql": f.read()})
    return examples


def load_sections(path):
    """A SQL file cut at its `-- ------ title` banners: [(title, body), ...]. Whatever
    comes before the first banner (the file's header) is dropped."""
    if not os.path.isfile(path):
        return []
    sections, title, body = [], None, []
    with open(path) as f:
        for line in f:
            m = BANNER.match(line)
            if m:
                if title:
                    sections.append((title, "".join(body).strip() + "\n"))
                title, body = m.group(1), []
            elif title:
                body.append(line)
    if title:
        sections.append((title, "".join(body).strip() + "\n"))
    return sections


def load_explore():
    return [{"group": "Queries", "title": t, "sql": b}
            for t, b in load_sections(os.path.join(SQL_DIR, "explore.sql"))]


# ── Gateway access ────────────────────────────────────────────────────────────

class GatewayError(Exception):
    pass


def gw(method, path, body=None):
    r = requests.request(method, GATEWAY + path, json=body, timeout=60)
    text = r.text
    if r.status_code >= 400:
        raise GatewayError(error_text(text))
    return json.loads(text) if text else {}


def error_text(text):
    """The gateway returns {"errors": ["Internal server error.", "<stack trace>"]}."""
    try:
        errors = json.loads(text).get("errors") or [text]
    except ValueError:
        errors = [text]
    return "\n".join(e for e in errors if e != "Internal server error.") or text


def result_path(session, op, token=0):
    return f"/v2/sessions/{session}/operations/{op}/result/{token}?rowFormat=JSON"


# A query's rows reach the client through Flink's collect sink. With exactly-once
# checkpointing (the session's 30 s interval) it holds them until a checkpoint completes,
# so a streaming SELECT shows nothing for up to 30 s, then a batch every 30 s.
# At-least-once lets them through as they come; at worst a failover repeats some rows.
# Queries only, per statement: INSERT jobs keep exactly-once.
QUERY = re.compile(r"\s*(\(\s*)*(SELECT|WITH|VALUES|TABLE)\b", re.I)
QUERY_CONFIG = {"execution.checkpointing.mode": "AT_LEAST_ONCE"}


def submit(session, sql):
    body = {"statement": sql}
    if QUERY.match(sql):
        body["executionConfig"] = QUERY_CONFIG
    return gw("POST", f"/v1/sessions/{session}/statements", body)["operationHandle"]


def execute_and_wait(session, sql, timeout=60):
    op = submit(session, sql)
    path, deadline = result_path(session, op), time.time() + timeout
    while path:
        res = gw("GET", path)
        if res.get("resultType") == "EOS":
            break
        if time.time() > deadline:
            raise GatewayError(f"timed out after {timeout}s: {sql[:80]}")
        if res.get("resultType") == "NOT_READY":
            time.sleep(0.2)
        path = res.get("nextResultUri")
    gw("DELETE", f"/v1/sessions/{session}/operations/{op}/close")


def json_error(e, status=502):
    return jsonify({"error": str(e)}), status


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


@app.route("/pipeline")
def pipeline_page():
    return send_from_directory(app.static_folder, "pipeline.html")


@app.get("/api/config")
def config():
    return jsonify({"preloadDdl": PRELOAD_DDL, "tiering": tiering_available()})


# ── The tiering service: deployed ahead of time, started with one click ──────────
#
# Flink has no "deployment that is not running": a job either runs or does not exist.
# The nearest thing is an UPLOADED JAR — the JobManager lists it under Submit New Job,
# ready to run — so "deploy" here means upload (start.sh --services-only does it), and
# "Start" runs the uploaded JAR with tiering.args. Both go through the JobManager's REST
# API, the same calls its web UI makes.
#
# Submitted this way the job is built by the JobManager with the cluster's own
# config.yaml, so it inherits the 30s checkpoint interval it needs to commit to Iceberg.

TIERING_JOB = "Fluss Lake Tiering Service"
ACTIVE = {"CREATED", "INITIALIZING", "RUNNING", "RESTARTING", "FAILING", "CANCELLING", "RECONCILING"}


def tiering_jar_file():
    jars = sorted(glob.glob(os.path.join(TIERING_JAR_DIR, "fluss-flink-tiering-*.jar")))
    return jars[-1] if jars else None


def tiering_available():
    return bool(tiering_jar_file()) and os.path.isfile(TIERING_ARGS)


def tiering_args():
    with open(TIERING_ARGS) as f:
        lines = [l.strip() for l in f if l.strip() and not l.lstrip().startswith("#")]
    # programArgsList, not a single programArgs string: no quoting rules to get wrong.
    return [part for line in lines for part in line.split()]


def flink(method, path, **kw):
    r = requests.request(method, FLINK + path, timeout=kw.pop("timeout", 30), **kw)
    if r.status_code >= 400:
        raise GatewayError(error_text(r.text))
    return r.json() if r.text else {}


def uploaded_tiering_jar():
    """The id of the tiering JAR already uploaded to the JobManager, or None. Uploads
    live in the JobManager's /tmp, so a JobManager restart forgets them."""
    name = os.path.basename(tiering_jar_file() or "")
    for f in flink("GET", "/jars").get("files", []):
        if f.get("name") == name:
            return f["id"]
    return None


def deploy_tiering():
    jar_id = uploaded_tiering_jar()
    if jar_id:
        return jar_id
    path = tiering_jar_file()
    with open(path, "rb") as f:
        res = flink("POST", "/jars/upload", timeout=120,
                    files={"jarfile": (os.path.basename(path), f, "application/java-archive")})
    return os.path.basename(res["filename"])


def tiering_job():
    jobs = flink("GET", "/jobs/overview").get("jobs", [])
    live = [j for j in jobs if j["name"].startswith(TIERING_JOB) and j["state"] in ACTIVE]
    return max(live, key=lambda j: j["start-time"]) if live else None


def tiering_status():
    job = tiering_job()
    return {
        "available": tiering_available(),
        "deployed": uploaded_tiering_jar() is not None,
        "job": {"id": job["jid"], "state": job["state"]} if job else None,
    }


@app.get("/api/tiering")
def tiering_get():
    if not tiering_available():
        return jsonify({"available": False})
    try:
        return jsonify(tiering_status())
    except (GatewayError, requests.RequestException) as e:
        return json_error(f"could not reach the Flink JobManager: {e}")


@app.post("/api/tiering/deploy")
def tiering_deploy():
    try:
        jar_id = deploy_tiering()
    except (GatewayError, requests.RequestException, OSError) as e:
        return json_error(f"could not upload the tiering JAR: {e}")
    return jsonify({"jar": jar_id, **tiering_status()})


@app.post("/api/tiering/start")
def tiering_start():
    """Idempotent: with a tiering job already running this changes nothing — two tiering
    jobs would compete for the same tables. Re-uploads the JAR if a JobManager restart
    lost it, so Start works whether or not start.sh deployed it first."""
    try:
        if tiering_job() is None:
            jar_id = deploy_tiering()
            flink("POST", f"/jars/{jar_id}/run", timeout=120,
                  json={"programArgsList": tiering_args()})
        return jsonify(tiering_status())
    except (GatewayError, requests.RequestException, OSError) as e:
        return json_error(f"could not start the tiering service: {e}")


@app.post("/api/session")
def open_session():
    load_ddl = (request.get_json(silent=True) or {}).get("ddl", PRELOAD_DDL)
    try:
        session = gw("POST", "/v1/sessions", {"sessionName": "sql-editor"})["sessionHandle"]
        if load_ddl:
            for stmt in DDL:
                execute_and_wait(session, stmt)
    except (GatewayError, requests.RequestException) as e:
        return json_error(f"could not open a session with the DDL loaded:\n{e}")
    # A pipeline already running (full ./start.sh): its source tables, so they can be queried.
    restored = []
    if not load_ddl:
        try:
            restored = restore_sources(session)
        except (GatewayError, requests.RequestException):
            pass
    return jsonify({"session": session, "ddl": len(DDL) if load_ddl else 0, "sources": restored})


@app.post("/api/split")
def split():
    return jsonify({"statements": split_statements(request.get_json()["sql"])})


@app.post("/api/run/<session>")
def run(session):
    try:
        op = submit(session, request.get_json()["sql"])
    except (GatewayError, requests.RequestException) as e:
        return json_error(e, 400)
    return jsonify({"op": op})


@app.get("/api/result/<session>/<op>/<int:token>")
def result(session, op, token):
    try:
        res = gw("GET", result_path(session, op, token))
    except (GatewayError, requests.RequestException) as e:
        return json_error(e, 400)
    nxt = res.get("nextResultUri")
    return jsonify({
        "type": res.get("resultType"),
        "jobId": res.get("jobID"),
        "isQuery": res.get("isQueryResult"),
        "columns": [c["name"] for c in (res.get("results") or {}).get("columns", [])],
        "rows": (res.get("results") or {}).get("data", []),
        "next": int(nxt.split("/result/")[1].split("?")[0]) if nxt else None,
    })


# Close, not cancel: a streaming SELECT's operation is FINISHED as soon as its job is
# submitted, so /cancel refuses it ("Failed to convert the Operation Status from
# FINISHED to CANCELED"). Closing releases the result fetcher, which cancels the job.
@app.post("/api/cancel/<session>/<op>")
def cancel(session, op):
    try:
        gw("DELETE", f"/v1/sessions/{session}/operations/{op}/close")
    except (GatewayError, requests.RequestException) as e:
        return json_error(e)
    return jsonify({})


@app.post("/api/heartbeat/<session>")
def heartbeat(session):
    try:
        gw("POST", f"/v1/sessions/{session}/heartbeat")
    except (GatewayError, requests.RequestException) as e:
        return json_error(e, 410)
    return jsonify({})


@app.post("/api/close/<session>")
def close(session):
    try:
        gw("DELETE", f"/v1/sessions/{session}")
    except (GatewayError, requests.RequestException):
        pass  # already gone (idle timeout) — nothing to do
    return Response(status=204)


# ── Catalog browser ───────────────────────────────────────────────────────────
#
# Runs SHOW / DESCRIBE in the browser's own session, so the tree also shows that
# session's in-memory tables (orders_log_kafka, ...), which no other session can see.

def query_rows(session, sql, timeout=60):
    """Run one statement and return every row's fields. For bounded statements only."""
    op = submit(session, sql)
    path, rows, columns, deadline = result_path(session, op), [], [], time.time() + timeout
    try:
        while path:
            res = gw("GET", path)
            if res.get("resultType") == "EOS":
                break
            if time.time() > deadline:
                raise GatewayError(f"timed out after {timeout}s: {sql}")
            results = res.get("results") or {}
            columns = [c["name"] for c in results.get("columns", [])] or columns
            rows += [r["fields"] for r in results.get("data", [])]
            if res.get("resultType") == "NOT_READY":
                time.sleep(0.2)
            path = res.get("nextResultUri")
    finally:
        gw("DELETE", f"/v1/sessions/{session}/operations/{op}/close")
    return columns, rows


def ident(*parts):
    return ".".join("`" + p.replace("`", "``") + "`" for p in parts)


@app.get("/api/catalog/<session>/current")
def catalog_current(session):
    """The session's current catalog and database: what an unqualified name in a DDL
    statement resolves against, so the browser knows which tree level it changed."""
    try:
        _, c = query_rows(session, "SHOW CURRENT CATALOG")
        _, d = query_rows(session, "SHOW CURRENT DATABASE")
    except (GatewayError, requests.RequestException) as e:
        return json_error(e)
    return jsonify({"catalog": c[0][0], "database": d[0][0]})


@app.get("/api/catalog/<session>/structure")
def catalog_structure(session):
    """One table's columns (DESCRIBE) and its DDL (SHOW CREATE), for the structure
    dialog. SHOW CREATE TABLE refuses a view, so a view falls back to SHOW CREATE VIEW."""
    c, d, t = (request.args.get(k) for k in ("catalog", "database", "table"))
    if not (c and d and t):
        return json_error("catalog, database and table are required", 400)
    try:
        names, rows = query_rows(session, f"DESCRIBE {ident(c, d, t)}")
        kind, ddl = "table", None
        try:
            _, out = query_rows(session, f"SHOW CREATE TABLE {ident(c, d, t)}")
        except GatewayError:
            kind = "view"
            _, out = query_rows(session, f"SHOW CREATE VIEW {ident(c, d, t)}")
        ddl = out[0][0] if out else None
    except (GatewayError, requests.RequestException) as e:
        return json_error(e)
    return jsonify({"kind": kind, "columns": [dict(zip(names, r)) for r in rows], "ddl": ddl})


@app.get("/api/catalog/<session>")
def catalog(session):
    """No arguments: the catalogs. ?catalog=: its databases. + &database=: its tables and
    views. + &table=: that table's columns, as DESCRIBE reports them."""
    c, d, t = (request.args.get(k) for k in ("catalog", "database", "table"))
    try:
        if not c:
            # The built-in catalog first (the session's own tables), then the rest A–Z.
            _, rows = query_rows(session, "SHOW CATALOGS")
            ordered = sorted((r[0] for r in rows), key=lambda n: (n != BUILTIN_CATALOG, n))
            return jsonify({"items": [{"name": n} for n in ordered]})
        if not d:
            _, rows = query_rows(session, f"SHOW DATABASES IN {ident(c)}")
            return jsonify({"items": [{"name": r[0]} for r in rows]})
        if not t:
            # Views included: SHOW TABLES lists both. (SHOW VIEWS has no IN clause in
            # Flink 1.20, and USE would change the session's current database.)
            _, rows = query_rows(session, f"SHOW TABLES IN {ident(c, d)}")
            return jsonify({"items": sorted(({"name": r[0]} for r in rows), key=lambda i: i["name"])})
        columns, rows = query_rows(session, f"DESCRIBE {ident(c, d, t)}")
        return jsonify({"items": [dict(zip(columns, r)) for r in rows]})
    except (GatewayError, requests.RequestException) as e:
        return json_error(e)


# ── Pipeline page (static/pipeline.html) ─────────────────────────────────────
#
# The architecture diagram as a control surface: each box is a table (a source or a
# sink), each arrow the Flink job that writes it. The page asks /api/pipeline/status what
# exists and what runs, and runs every action's SQL in the browser's own session, like
# the editor — the two share one session.

# Box -> its table, and the arrow (job) that feeds it. `listed` is the name SHOW TABLES
# prints when it differs from the table's own (the JDBC catalog qualifies the schema).
NODES = {
    "kafka":           {"table": (BUILTIN_CATALOG, "default_database", "orders_log_kafka"), "source": True},
    "product_catalog": {"table": (BUILTIN_CATALOG, "default_database", "product_catalog_cdc"), "source": True},
    "orders_log":      {"table": ("fluss", "orders", "orders_log"), "feed": "kafka_to_fluss"},
    "product_lookup":  {"table": ("fluss", "orders", "product_lookup"), "feed": "pgcdc_to_fluss"},
    "orders_enriched": {"table": ("fluss", "orders", "orders_enriched"), "feed": "enrichment"},
    "revenue_1m":      {"table": ("postgres", "dwh", "revenue_1m"), "listed": "public.revenue_1m",
                        "feed": "revenue", "precreated": True},
}
EDGES = {
    "kafka_to_fluss": {"from": ["kafka"], "to": "orders_log", "file": "jobs/10_kafka_to_fluss.sql"},
    "pgcdc_to_fluss": {"from": ["product_catalog"], "to": "product_lookup", "file": "jobs/20_pgcdc_to_fluss.sql"},
    "enrichment":     {"from": ["orders_log", "product_lookup"], "to": "orders_enriched", "file": "jobs/30_enrichment.sql"},
    "revenue":        {"from": ["orders_enriched"], "to": "revenue_1m", "file": "jobs/50_revenue.sql"},
}
# Pre-created on page load: revenue_1m already exists in the warehouse (postgres-dwh), so Flink only needs
# the postgres catalog. The two sources are not: attendees create them from their boxes.
BOOTSTRAP_FILES = ["ddl/03_postgres.sql"]

# ...unless the pipeline already reads them. A full ./start.sh (not --services-only) submits
# the jobs through submit.sh, whose SQL Client sessions are the only ones that ever had the
# source tables: they live in the in-memory default catalog. So a browser session gets
# them too — the same IF NOT EXISTS DDL — once the job reading each one is running.
SOURCES = {   # box -> (its DDL section, the arrow whose job reads it)
    "kafka": (("ddl/02_sources.sql", "Step 3:"), "kafka_to_fluss"),
    "product_catalog": (("ddl/02_sources.sql", "Step 7:"), "pgcdc_to_fluss"),
}


def read_sql(rel):
    with open(os.path.join(SQL_DIR, rel)) as f:
        return f.read()


def section(rel, prefix):
    for title, body in load_sections(os.path.join(SQL_DIR, rel)):
        if title.startswith(prefix):
            return f"-- {title}\n{body}"
    raise KeyError(f"no '{prefix}' section in {rel}")


def without_header(sql):
    """A job file minus its leading `-- ===` comment block: the pop-up says what it is."""
    lines = sql.splitlines()
    i = 0
    while i < len(lines) and (lines[i].startswith("--") or not lines[i].strip()):
        i += 1
    return "\n".join(lines[i:]).strip() + "\n"


@app.get("/api/pipeline/sql")
def pipeline_sql():
    """Every action's default SQL, cut from flink_sql/ so the files stay the one source."""
    catalog = section("ddl/01_fluss.sql", "Step 1:")
    database = "CREATE DATABASE IF NOT EXISTS fluss.orders;\n"
    view = lambda t, note: f"-- {note}\nSELECT * FROM {t};\n"
    explore = load_sections(os.path.join(SQL_DIR, "explore.sql"))
    union_view = next(b for t, b in explore if t.startswith("Lab 4, Step 15: one table"))
    snapshots = next(b for t, b in explore if t.startswith("Lab 4, Step 18:"))
    iceberg_catalog = next(b for t, b in explore if t.startswith("Bonus: Iceberg's own catalog"))
    time_travel = next(b for t, b in explore if t.startswith("Bonus: time travel"))
    enable = split_statements(read_sql("lake/enable_tiering.sql"))[0]
    return jsonify({
        "nodes": {
            "kafka": {"create": section("ddl/02_sources.sql", "Step 3:"),
                      "view": view("orders_log_kafka", "The raw order events on the Kafka topic. A streaming read: it keeps going until you cancel or close.")},
            "product_catalog": {"create": section("ddl/02_sources.sql", "Step 7:"),
                                "view": view("product_catalog_cdc", "PostgreSQL's product_catalog, read through CDC: the snapshot, then every change.")},
            "orders_log": {"create": catalog + "\n" + section("ddl/01_fluss.sql", "Step 2:"),
                           "view": view("fluss.orders.orders_log", "Every order event, as kafka-to-fluss writes it.")},
            "product_lookup": {"create": catalog + "\n" + database + "\n" + section("ddl/01_fluss.sql", "Step 6:"),
                               "view": view("fluss.orders.product_lookup", "One row per product, kept in sync with PostgreSQL by pgcdc-to-fluss.")},
            "orders_enriched": {"create": catalog + "\n" + database + "\n" + section("ddl/01_fluss.sql", "Step 9:"),
                                "view": view("fluss.orders.orders_enriched", "Order events joined with product details. Once tiered, this is a union read: Iceberg first, then Fluss.")},
            "revenue_1m": {"view": "-- revenue_1m in the data warehouse (postgres-dwh), through the postgres catalog: a bounded read, newest window first.\n"
                                   "SET 'execution.runtime-mode' = 'batch';\n"
                                   "SELECT * FROM postgres.dwh.revenue_1m ORDER BY window_start DESC, category;\n"
                                   "SET 'execution.runtime-mode' = 'streaming';\n"},
            "lake": {"view": "-- orders_enriched in Iceberg, through Fluss's $lake view: its snapshots.\n" + snapshots},
            "iceberg_flink": {"create": "-- Lab 4, Step 18 bonus: Lakekeeper as a Flink catalog of its own.\n" + iceberg_catalog,
                              "view": "-- Lab 4, Step 18 bonus: time travel, read by Flink straight from Iceberg.\n" + time_travel},
            "union": {"view": "-- The union read: Iceberg's history plus Fluss's fresh rows, next to Iceberg alone.\n" + union_view},
        },
        "edges": {
            **{e: {"sql": without_header(read_sql(spec["file"])) + RESET_NAME.lstrip("\n")} for e, spec in EDGES.items()},
            "tiering": {"sql": "-- Opt orders_enriched in: the tiering service starts committing it to Iceberg.\n" + enable + ";\n"},
        },
    })


@app.post("/api/pipeline/bootstrap/<session>")
def pipeline_bootstrap(session):
    """Register what the pre-created boxes need (the postgres catalog), and the sources a
    running pipeline already reads. IF NOT EXISTS throughout."""
    try:
        for rel in BOOTSTRAP_FILES:
            for stmt in split_statements(read_sql(rel)):
                execute_and_wait(session, stmt)
        restored = restore_sources(session)
    except (GatewayError, requests.RequestException, OSError) as e:
        return json_error(e)
    return jsonify({"ok": True, "sources": restored})


# Which table a job writes, from its plan. Matching jobs to arrows by what they write, not
# by name, keeps an arrow lit when an attendee edits pipeline.name in the pop-up. Fluss
# sinks print "Sink(orders.orders_log)", JDBC ones "Sink(table=[postgres.dwh.revenue_1m]".
SINK_FULL = re.compile(r"Sink\(table=\[([^\]]+)\]")
SINK_SHORT = re.compile(r"Sink\(([\w$.`]+)\)")
plan_sinks = {}


def job_sinks(jid):
    if jid not in plan_sinks:
        nodes = flink("GET", f"/jobs/{jid}/plan").get("plan", {}).get("nodes", [])
        text = " ".join(n.get("description", "") for n in nodes)
        plan_sinks[jid] = [s.replace("`", "") for s in SINK_FULL.findall(text) + SINK_SHORT.findall(text)]
    return plan_sinks[jid]


def writes(sinks, table):
    full = ".".join(table)
    return any(s == full or full.endswith("." + s) for s in sinks)


def active_jobs():
    """Flink jobs that are up, each with the tables it writes (from its plan)."""
    active = [j for j in flink("GET", "/jobs/overview").get("jobs", [])
              if j["state"] in ACTIVE and j["state"] != "CANCELLING"]
    for j in active:
        j["sinks"] = job_sinks(j["jid"])
    return active


def edge_jobs(active, edge):
    return [j for j in active if writes(j["sinks"], NODES[EDGES[edge]["to"]]["table"])]


def restore_sources(session):
    """Create in this session the source tables whose reading job runs (see SOURCES).
    Returns the boxes it created. Qualified, so a USE in the session does not misplace them."""
    running = {e for e in EDGES if any(j["state"] == "RUNNING" for j in edge_jobs(active_jobs(), e))}
    listed = names(session, f"SHOW TABLES IN {ident(BUILTIN_CATALOG, 'default_database')}")
    restored = []
    for node, ((rel, prefix), edge) in SOURCES.items():
        if edge in running and NODES[node]["table"][2] not in listed:
            for stmt in split_statements(section(rel, prefix)):
                execute_and_wait(session, stmt.replace(
                    "CREATE TABLE IF NOT EXISTS ", f"CREATE TABLE IF NOT EXISTS {ident(BUILTIN_CATALOG, 'default_database')}.", 1))
            restored.append(node)
    return restored


def names(session, sql):
    try:
        return {r[0] for r in query_rows(session, sql)[1]}
    except GatewayError:
        return set()   # e.g. the catalog or database does not exist yet


# ── The data probe: does a table return a row? ──
# The one status check that costs a Flink job, so it runs in the background, one at a
# time, in a session of its own, and only for a box whose feeding job runs and whose data
# is not confirmed yet. Confirmed boxes are not probed again.
probe_lock = threading.Lock()
probe_wake = threading.Event()
probe_ok, probe_next, probe_wanted = set(), {}, {}
PROBE_TIMEOUT, PROBE_RETRY = 90, 30


def want_probe(node):
    with probe_lock:
        if node in probe_ok or node in probe_wanted or time.time() < probe_next.get(node, 0):
            return
        probe_wanted[node] = f"SELECT 1 FROM {ident(*NODES[node]['table'])} LIMIT 1"
    probe_wake.set()


def first_row(session, sql):
    op = submit(session, sql)
    path, deadline = result_path(session, op), time.time() + PROBE_TIMEOUT
    try:
        while path and time.time() < deadline:
            res = gw("GET", path)
            if (res.get("results") or {}).get("data"):
                return True
            if res.get("resultType") == "EOS":
                return False
            time.sleep(0.5)
            path = res.get("nextResultUri")
        return False
    finally:
        try:
            gw("DELETE", f"/v1/sessions/{session}/operations/{op}/close")
        except (GatewayError, requests.RequestException):
            pass


def probe_worker():
    session = None
    while True:
        probe_wake.wait()
        probe_wake.clear()
        while True:
            with probe_lock:
                if not probe_wanted:
                    break
                node, sql = next(iter(probe_wanted.items()))
            ok = False
            try:
                if session is None:
                    session = gw("POST", "/v1/sessions", {"sessionName": "pipeline-probe"})["sessionHandle"]
                ok = first_row(session, sql)
            except (GatewayError, requests.RequestException):
                session = None   # gone or broken: a fresh one next time
            with probe_lock:
                probe_wanted.pop(node, None)
                if ok:
                    probe_ok.add(node)
                else:
                    probe_next[node] = time.time() + PROBE_RETRY


threading.Thread(target=probe_worker, daemon=True, name="pipeline-probe").start()

lake_prefix = None


def lake_table():
    """orders_enriched's Iceberg table metadata from Lakekeeper, or None if it has none."""
    global lake_prefix
    if lake_prefix is None:
        cfg = requests.get(f"{LAKEKEEPER}/catalog/v1/config", params={"warehouse": "lakehouse"}, timeout=10).json()
        lake_prefix = (cfg.get("overrides") or {}).get("prefix") or (cfg.get("defaults") or {}).get("prefix")
    r = requests.get(f"{LAKEKEEPER}/catalog/v1/{lake_prefix}/namespaces/orders/tables/orders_enriched", timeout=10)
    return r.json().get("metadata") if r.ok else None


# ── MinIO: how many Parquet files the Iceberg table has on object storage ──
# A ListObjectsV2 call signed with AWS Signature V4 by hand, so the editor needs no S3
# library. Path-style, as MinIO serves it.

def s3_get(path, params):
    now = datetime.now(timezone.utc)
    amz, day = now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")
    host = urlparse(S3_ENDPOINT).netloc
    enc = lambda v: quote(v, safe="-_.~")
    query = "&".join(f"{enc(k)}={enc(v)}" for k, v in sorted(params.items()))
    payload = hashlib.sha256(b"").hexdigest()
    signed = "host;x-amz-content-sha256;x-amz-date"
    canonical = "\n".join(["GET", quote(path), query, f"host:{host}", f"x-amz-content-sha256:{payload}",
                           f"x-amz-date:{amz}", "", signed, payload])
    scope = f"{day}/{S3_REGION}/s3/aws4_request"
    to_sign = "\n".join(["AWS4-HMAC-SHA256", amz, scope, hashlib.sha256(canonical.encode()).hexdigest()])
    key = ("AWS4" + S3_SECRET_KEY).encode()
    for part in (day, S3_REGION, "s3", "aws4_request"):
        key = hmac.new(key, part.encode(), hashlib.sha256).digest()
    signature = hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()
    auth = f"AWS4-HMAC-SHA256 Credential={S3_ACCESS_KEY}/{scope}, SignedHeaders={signed}, Signature={signature}"
    r = requests.get(f"{S3_ENDPOINT}{quote(path)}?{query}", timeout=10,
                     headers={"Authorization": auth, "x-amz-date": amz, "x-amz-content-sha256": payload})
    r.raise_for_status()
    return ET.fromstring(r.content)


def parquet_count(location):
    """Parquet files under the table's data/ folder. `location` is the table's s3:// URI."""
    loc = urlparse(location)
    bucket, prefix = loc.netloc, loc.path.strip("/") + "/data/"
    ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
    count, token = 0, None
    while True:
        params = {"list-type": "2", "prefix": prefix, "max-keys": "1000"}
        if token:
            params["continuation-token"] = token
        root = s3_get(f"/{bucket}", params)
        count += sum(1 for c in root.iter(ns + "Key") if c.text.endswith(".parquet"))
        if (root.findtext(ns + "IsTruncated") or "").lower() != "true":
            return count
        token = root.findtext(ns + "NextContinuationToken")


tiering_on = False   # once true it stays true: tiering cannot be switched off again


def datalake_enabled(session, table):
    """Whether SHOW CREATE TABLE has the table opted in to tiering."""
    try:
        ddl = query_rows(session, f"SHOW CREATE TABLE {ident(*table)}")[1][0][0]
        return "'table.datalake.enabled' = 'true'" in ddl
    except (GatewayError, IndexError):
        return False


@app.get("/api/pipeline/status/<session>")
def pipeline_status(session):
    global tiering_on
    try:
        catalogs = names(session, "SHOW CATALOGS")
        listed = {
            BUILTIN_CATALOG: names(session, f"SHOW TABLES IN {ident(BUILTIN_CATALOG, 'default_database')}"),
            "fluss": names(session, "SHOW TABLES IN `fluss`.`orders`") if "fluss" in catalogs else set(),
            "postgres": names(session, "SHOW TABLES IN `postgres`.`dwh`") if "postgres" in catalogs else set(),
        }
    except requests.RequestException as e:
        return json_error(e)
    exists = {n: (spec.get("listed") or spec["table"][2]) in listed.get(spec["table"][0], set())
              for n, spec in NODES.items()}

    # Jobs, by the table they write.
    try:
        active = active_jobs()
    except (GatewayError, requests.RequestException):
        active = []
    edges = {}
    for e, spec in EDGES.items():
        jobs = [{"jid": j["jid"], "name": j["name"], "state": j["state"]} for j in edge_jobs(active, e)]
        states = {j["state"] for j in jobs}
        edges[e] = {"state": "running" if "RUNNING" in states else
                             "failing" if states & {"RESTARTING", "FAILING"} else
                             "starting" if jobs else "idle",
                    "jobs": jobs, "ready": all(exists[n] for n in spec["from"] + [spec["to"]])}

    nodes = {}
    for n, spec in NODES.items():
        feed = edges.get(spec.get("feed"), {}).get("state")
        if not exists[n]:
            with probe_lock:
                probe_ok.discard(n)   # dropped: whatever was confirmed is gone with it
            state = "inactive"
        elif feed == "running":
            want_probe(n)
            state = "live" if n in probe_ok else "starting"
        elif spec.get("source") and not spec.get("feed"):
            state = "live"           # the Kafka topic and product_catalog: always flowing
        else:
            state = "ready"
        nodes[n] = {"state": state, "exists": exists[n]}

    # Tiering: the service, the opt-in on orders_enriched, and the Iceberg copy.
    try:
        tiering = tiering_status()
    except (GatewayError, requests.RequestException):
        tiering = {"available": False, "job": None}
    if exists["orders_enriched"] and not tiering_on:
        tiering_on = datalake_enabled(session, NODES["orders_enriched"]["table"])
    if not exists["orders_enriched"]:
        tiering_on = False
    snapshot, files, first_snapshot = None, None, None
    try:
        meta = lake_table() if tiering_on else None
        if meta:
            snapshot = meta.get("current-snapshot-id")
            snaps = sorted(meta.get("snapshots") or [], key=lambda x: x.get("timestamp-ms", 0))
            first_snapshot = str(snaps[0]["snapshot-id"]) if snaps else None
            files = parquet_count(meta["location"]) if meta.get("location") else None
    except (requests.RequestException, ValueError, KeyError, ET.ParseError):
        pass
    tjob = (tiering.get("job") or {}).get("state")
    nodes["tiering"] = {"state": "live" if tjob == "RUNNING" else "starting" if tjob else "ready"}
    nodes["lake"] = {"state": "live" if snapshot else "starting" if tiering_on and tjob == "RUNNING"
                     else "ready" if tiering_on else "inactive"}
    # Flink reading Iceberg through its own catalog (Lakekeeper registered as `iceberg`).
    # firstSnapshot fills the time-travel query's placeholder with a real snapshot id.
    ice = "iceberg" in catalogs
    nodes["iceberg_flink"] = {"exists": ice, "firstSnapshot": first_snapshot,
                              "state": "inactive" if not ice else "live" if snapshot else "ready"}
    # The union read, a consumer: possible as soon as orders_enriched exists (from Fluss
    # alone), and a real union of both tiers once Iceberg has data.
    nodes["union"] = {"exists": exists["orders_enriched"], "state":
                      "inactive" if not exists["orders_enriched"] else "live" if snapshot else "ready"}
    edges["tiering"] = {"state": "running" if tiering_on else "idle", "jobs": [],
                        "ready": exists["orders_enriched"]}
    edges["commits"] = {"state": "running" if snapshot and tjob == "RUNNING" else "idle", "jobs": []}
    # MinIO: the Parquet files actually on object storage, counted by listing them.
    nodes["minio"] = {"files": files, "state":
                      "inactive" if files is None else
                      ("live" if tjob == "RUNNING" else "ready") if files > 0 else
                      "starting" if tjob == "RUNNING" else "ready"}
    missing = [n for n, spec in NODES.items() if spec.get("precreated") and not exists[n]]
    missing += [n for n, (_, e) in SOURCES.items() if not exists[n] and edges[e]["state"] == "running"]
    return jsonify({"nodes": nodes, "edges": edges, "tiering": tiering, "bootstrap": bool(missing)})


# The data probe's jobs (want_probe): SELECT 1 FROM <table> LIMIT 1, as Flink names them.
PROBE_JOB = re.compile(r"\s*SELECT 1\s+FROM\b.*FETCH NEXT 1 ROWS ONLY\s*$", re.S | re.I)


@app.get("/api/jobs")
def jobs_list():
    """Every Flink job, newest first, for the catalog panel's Job Catalog: what kind of job
    it is (pipeline job, tiering service, interactive query) and, for an active pipeline
    job, the table it writes — read from its plan, as the pipeline status does."""
    try:
        jobs = flink("GET", "/jobs/overview").get("jobs", [])
    except (GatewayError, requests.RequestException) as e:
        return json_error(f"could not reach the Flink JobManager: {e}")
    out = []
    for j in sorted(jobs, key=lambda j: j.get("start-time", 0), reverse=True):
        name, state = j["name"], j["state"]
        if PROBE_JOB.match(name):
            continue   # the pipeline page's own "does this table return a row?" checks
        kind = "service" if name.startswith(TIERING_JOB) else "query" if name.lstrip().upper().startswith("SELECT") else "job"
        sinks = []
        if kind == "job" and state in ACTIVE:
            try:   # Fluss plans name the sink db.table only: show it with its catalog
                sinks = [x if x.count(".") >= 2 else "fluss." + x for x in job_sinks(j["jid"])]
            except (GatewayError, requests.RequestException):
                pass
        out.append({"jid": j["jid"], "name": " ".join(name.split()), "state": state, "kind": kind,
                    "sinks": sinks, "start": j.get("start-time"), "duration": j.get("duration"),
                    "active": state in ACTIVE})
    return jsonify({"jobs": out})


@app.post("/api/jobs/<jid>/cancel")
def job_cancel(jid):
    try:
        flink("PATCH", f"/jobs/{jid}?mode=cancel")
    except (GatewayError, requests.RequestException) as e:
        return json_error(e)
    return jsonify({})


@app.get("/api/examples")
def examples():
    return jsonify(load_examples())


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8084, threaded=True)
