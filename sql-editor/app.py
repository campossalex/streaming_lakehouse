"""Web SQL editor for the streaming_lakehouse_oss scenario.

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

This scenario has no secrets, so unlike coffee_shop_oss's copy of this app there is no
placeholder substitution and nothing to redact: statements reach the gateway verbatim.
"""
import glob
import json
import os
import re
import time

import requests
from flask import Flask, Response, jsonify, request, send_from_directory

GATEWAY = os.environ.get("GATEWAY_URL", "http://sql-gateway:8083").rstrip("/")
SQL_DIR = os.environ.get("SQL_DIR", "/opt/sql")
PRELOAD_DDL = os.environ.get("PRELOAD_DDL", "false").lower() in ("1", "true", "yes")
FLINK = os.environ.get("FLINK_URL", "http://jobmanager:8081").rstrip("/")
TIERING_JAR_DIR = os.environ.get("TIERING_JAR_DIR", "/opt/tiering")
TIERING_ARGS = os.environ.get("TIERING_ARGS", "/opt/tiering.args")

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


def load_examples():
    """The DDL files as written, then explore.sql cut at its `-- ------ title` banners,
    then the job files, each a long-running INSERT INTO."""
    examples = []
    for path in ddl_files():
        with open(path) as f:
            examples.append({"group": "DDL", "title": os.path.basename(path), "sql": f.read()})
    examples += load_explore()
    for path in sorted(glob.glob(os.path.join(SQL_DIR, "jobs", "*.sql"))):
        with open(path) as f:
            sql = f.read()
        m = PIPELINE_NAME.search(sql)
        title = os.path.basename(path) + (f" — {m.group(1)}" if m else "")
        examples.append({"group": "Jobs", "title": title,
                         "sql": sql.rstrip() + "\n" + (RESET_NAME if m else "")})
    return examples


def load_explore():
    path = os.path.join(SQL_DIR, "explore.sql")
    if not os.path.isfile(path):
        return []
    examples, title, body = [], None, []
    with open(path) as f:
        for line in f:
            m = BANNER.match(line)
            if m:
                if title:
                    examples.append({"group": "Queries", "title": title, "sql": "".join(body).strip() + "\n"})
                title, body = m.group(1), []
            elif title:
                body.append(line)
    if title:
        examples.append({"group": "Queries", "title": title, "sql": "".join(body).strip() + "\n"})
    return examples


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


def execute_and_wait(session, sql, timeout=60):
    op = gw("POST", f"/v1/sessions/{session}/statements", {"statement": sql})["operationHandle"]
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
    return jsonify({"session": session, "ddl": len(DDL) if load_ddl else 0})


@app.post("/api/split")
def split():
    return jsonify({"statements": split_statements(request.get_json()["sql"])})


@app.post("/api/run/<session>")
def run(session):
    try:
        res = gw("POST", f"/v1/sessions/{session}/statements",
                 {"statement": request.get_json()["sql"]})
    except (GatewayError, requests.RequestException) as e:
        return json_error(e, 400)
    return jsonify({"op": res["operationHandle"]})


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


@app.get("/api/examples")
def examples():
    return jsonify(load_examples())


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8084, threaded=True)
