#!/usr/bin/env node
// capture-screenshots.mjs — the WORKSHOP.md screenshots, taken from a running environment
//
// Drives the local Google Chrome, headless, through the Chrome DevTools Protocol: no npm
// packages, only Node 22+ (for its built-in WebSocket). Run it against a stack started in
// full mode, once the pipeline has run for a few minutes (Iceberg needs its first commits,
// Grafana a few windows):
//
//   ./start.sh --reset            # then wait ~5 minutes
//   node scripts/capture-screenshots.mjs [name ...]
//
// Writes docs/screenshots/<name>.png. With names, only those shots are taken.
// Every streaming query a shot starts is cancelled before the next shot.

import { spawn } from "node:child_process";
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const CHROME = process.env.CHROME || "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";
const HOST = process.env.LAB_HOST || "localhost";
const OUT = join(dirname(fileURLToPath(import.meta.url)), "..", "docs", "screenshots");
const W = 1440, H = 900, PORT = 9333;
const sleep = ms => new Promise(r => setTimeout(r, ms));

// Runs in the SQL editor page: type a statement (or load an example), run it, give it
// time to show rows. Returns once the panel has `rows` rows or the time is up.
const editorRun = (sql, { rows = 8, maxMs = 60000, example, editorH = 150 } = {}) => `(async () => {
  const wait = ms => new Promise(r => setTimeout(r, ms));
  for (let i = 0; i < 100 && !session; i++) await wait(200);   // the session opens on load
  setEditorHeight(${editorH}, false);   // a short editor, so the results get the room
  const text = ${JSON.stringify(example || "")}
    ? examples.find(e => e.title.startsWith(${JSON.stringify(example || "")})).sql : ${JSON.stringify(sql || "")};
  editor.setValue(text);
  runScript();
  const t0 = Date.now();
  while (Date.now() - t0 < ${maxMs}) {
    await wait(1000);
    const last = document.querySelector("#results .out:last-child");
    if (last && last.querySelectorAll("tbody tr").length >= ${rows}) break;
    if (!busy && last && !last.classList.contains("live")) break;   // a bounded query finished
  }
  editor.setCursor(0, 0);
})()`;
// Stop whatever the editor still runs (a streaming SELECT holds a Flink slot).
const editorStop = `(async () => { if (busy) { stopRequested = true; for (let i = 0; i < 50 && busy; i++) await new Promise(r => setTimeout(r, 200)); } })()`;

// Types into whatever has focus, then presses a key with modifiers (CDP input events).
async function type(cdp, text) { await cdp.send("Input.insertText", { text }); }
async function key(cdp, k, code, keyCode, modifiers = 0) {
  for (const type of ["keyDown", "keyUp"])
    await cdp.send("Input.dispatchKeyEvent", { type, key: k, code, windowsVirtualKeyCode: keyCode, modifiers });
}
// CloudBeaver: open a SQL editor on the Trino connection, run `sql`, wait for the grid.
const cloudbeaver = sql => async cdp => {
  await cdp.eval(`(async () => {
    const wait = ms => new Promise(r => setTimeout(r, ms));
    const find = async (fn, what) => { for (let i = 0; i < 80; i++) { const el = fn(); if (el) return el; await wait(250); } throw new Error("not found: " + what); };
    (await find(() => [...document.querySelectorAll("*")].find(e => e.children.length === 0 && e.textContent.trim() === "Trino — Lakehouse"), "connection")).click();
    await wait(800);
    (await find(() => document.querySelector('button[title^="Open SQL Editor"]'), "SQL button")).click();
    (await find(() => document.querySelector(".cm-content"), "editor")).focus();
  })()`);
  await sleep(800);
  await type(cdp, sql);
  await key(cdp, "Escape", "Escape", 27);   // close the autocomplete popup, or it eats the next key
  await cdp.eval(`(async () => {
    const wait = ms => new Promise(r => setTimeout(r, ms));
    await wait(300);
    document.querySelector('button[title^="Execute SQL Statement ("]').click();
    for (let i = 0; i < 120; i++) { if (document.querySelector('[role="gridcell"], .rdg-cell')) break; await wait(500); }
    await wait(1500);
  })()`);
};

// The shots, in WORKSHOP.md order. `steps` runs after load (with the CDP client), `setup`
// is evaluated in the page, `after` cleans up, `before` runs ahead of the navigation.
const SHOTS = [
  { name: "tools-homepage", url: `http://${HOST}/`, wait: 4000 },
  { name: "lab1-redpanda-topic", url: `http://${HOST}:8082/topics/orders_log`, wait: 8000 },
  { name: "lab1-orders-log", url: `http://${HOST}:8084/`,
    setup: editorRun("SELECT * FROM fluss.orders.orders_log;", { rows: 12 }), after: editorStop },
  { name: "lab2-product-count", url: `http://${HOST}:8084/`,
    setup: editorRun(null, { example: "Step 8 · products replicated", rows: 1, maxMs: 25000 }), after: editorStop },
  { name: "lab2-orders-enriched", url: `http://${HOST}:8084/`,
    setup: editorRun("SELECT * FROM fluss.orders.orders_enriched;", { rows: 12 }), after: editorStop },
  { name: "lab3-show-create", url: `http://${HOST}:8084/`,
    setup: editorRun("SHOW CREATE TABLE fluss.orders.orders_enriched;", { rows: 1 }) + `.then(() => {
      // Scroll the DDL to its WITH clause, where the datalake options are.
      const pre = document.querySelector("#results .out:last-child .doc pre");
      if (pre) pre.scrollTop = pre.scrollHeight; })` },
  // Cut below the running jobs: the completed list holds whatever ad hoc queries ran.
  { name: "lab3-flink-jobs", url: `http://${HOST}:8081/#/overview`, wait: 4000, clip: { x: 0, y: 0, width: W, height: 610 } },
  // The table page's URL holds the warehouse id, which changes with every reset.
  { name: "lab3-lakekeeper", wait: 5000, url: async () => {
      const { warehouses } = await fetch(`http://${HOST}:8181/management/v1/warehouse`).then(r => r.json());
      const id = warehouses.find(w => w.name === "lakehouse")["warehouse-id"];
      return `http://${HOST}:8181/ui/warehouse/${id}/namespace/orders/table/orders_enriched`;
    } },
  // MinIO: log in through its API with the lab's documented credentials, then browse.
  { name: "lab3-minio-files", wait: 5000,
    before: async cdp => {
      const loaded = cdp.once("Page.loadEventFired");
      await cdp.send("Page.navigate", { url: `http://${HOST}:9001/login` }); await loaded;
      await cdp.eval(`fetch("/api/v1/login", { method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ accessKey: "admin", secretKey: "password" }) }).then(r => r.status)`);
    },
    url: `http://${HOST}:9001/browser/warehouse/${encodeURIComponent("lakehouse/orders/orders_enriched/data/__bucket=0/")}` },
  { name: "lab4-union-read", url: `http://${HOST}:8084/`,
    setup: editorRun(null, { example: "Step 15 · one table", rows: 1, maxMs: 120000 }), after: editorStop },
  { name: "lab4-trino-select", url: `http://${HOST}:8978/`, wait: 6000,
    steps: cloudbeaver("SELECT * FROM lakehouse.orders.orders_enriched LIMIT 20") },
  { name: "lab4-trino-history", url: `http://${HOST}:8978/`, wait: 6000,
    steps: cloudbeaver("SELECT product_name, category, COUNT(*) AS orders, SUM(amount) AS revenue\nFROM lakehouse.orders.orders_enriched\nWHERE status = 'DELIVERED'\nGROUP BY product_name, category\nORDER BY revenue DESC") },
  { name: "lab5-revenue-rows", url: `http://${HOST}:8084/`,
    setup: editorRun("SET 'execution.runtime-mode' = 'batch';\nSELECT window_start, category, order_count, revenue, delivered_count, cancelled_count\nFROM postgres.dwh.revenue_1m ORDER BY window_start DESC, category LIMIT 20;\nSET 'execution.runtime-mode' = 'streaming';",
      { rows: 1, maxMs: 60000, editorH: 120 }) + `.then(() => {
      // Only the SELECT's panel: the two SETs around it say nothing.
      const outs = [...document.querySelectorAll("#results .out")];
      outs.filter(o => /^\\s*SET\\b/.test(o.querySelector(".stmt").textContent)).forEach(o => o.remove());
      outs.find(o => o.isConnected)?.classList.add("fill"); })`, after: editorStop },
  { name: "lab5-grafana", url: `http://${HOST}:3000/d/streaming-lakehouse-orders?orgId=1&from=now-20m&to=now&kiosk`, wait: 8000 },
];

// ── A minimal CDP client ──────────────────────────────────────────────────────
class Cdp {
  constructor(url) { this.ws = new WebSocket(url); this.id = 0; this.pending = new Map(); this.waiters = []; }
  open() {
    return new Promise((res, rej) => {
      this.ws.onopen = res; this.ws.onerror = rej;
      this.ws.onmessage = m => {
        const msg = JSON.parse(m.data);
        if (msg.id && this.pending.has(msg.id)) {
          const { resolve, reject } = this.pending.get(msg.id); this.pending.delete(msg.id);
          msg.error ? reject(new Error(msg.error.message)) : resolve(msg.result);
        } else if (msg.method) {
          this.waiters = this.waiters.filter(w => !(w.method === msg.method && (w.resolve(msg.params), true)));
        }
      };
    });
  }
  send(method, params = {}) {
    const id = ++this.id;
    this.ws.send(JSON.stringify({ id, method, params }));
    return new Promise((resolve, reject) => this.pending.set(id, { resolve, reject }));
  }
  once(method, timeout = 30000) {
    return Promise.race([new Promise(resolve => this.waiters.push({ method, resolve })), sleep(timeout)]);
  }
  async eval(expression) {
    const r = await this.send("Runtime.evaluate", { expression, awaitPromise: true, returnByValue: true });
    if (r.exceptionDetails) throw new Error(r.exceptionDetails.exception?.description || r.exceptionDetails.text);
    return r.result.value;
  }
}

async function main() {
  const only = process.argv.slice(2);
  const shots = only.length ? SHOTS.filter(s => only.includes(s.name)) : SHOTS;
  mkdirSync(OUT, { recursive: true });
  const profile = mkdtempSync(join(tmpdir(), "lab-shots-"));
  const chrome = spawn(CHROME, ["--headless=new", `--remote-debugging-port=${PORT}`, `--user-data-dir=${profile}`,
    `--window-size=${W},${H}`, "--hide-scrollbars", "--no-first-run", "--no-default-browser-check", "about:blank"],
    { stdio: "ignore" });
  try {
    let target;
    for (let i = 0; i < 50 && !target; i++) {
      await sleep(200);
      target = await fetch(`http://127.0.0.1:${PORT}/json/list`).then(r => r.json())
        .then(l => l.find(t => t.type === "page")).catch(() => null);
    }
    const cdp = new Cdp(target.webSocketDebuggerUrl);
    await cdp.open();
    await cdp.send("Page.enable");
    await cdp.send("Emulation.setDeviceMetricsOverride", { width: W, height: H, deviceScaleFactor: 1, mobile: false });
    // Light theme, whatever the OS uses: easier to read in a document.
    await cdp.send("Emulation.setEmulatedMedia", { features: [{ name: "prefers-color-scheme", value: "light" }] });
    for (const shot of shots) {
      process.stdout.write(`${shot.name} … `);
      if (shot.before) await shot.before(cdp);
      const loaded = cdp.once("Page.loadEventFired");
      await cdp.send("Page.navigate", { url: typeof shot.url === "function" ? await shot.url() : shot.url });
      await loaded;
      await sleep(shot.wait || 2500);
      try {
        if (shot.steps) await shot.steps(cdp);
        if (shot.setup) await cdp.eval(shot.setup);
        await sleep(800);
        const { data } = await cdp.send("Page.captureScreenshot",
          { format: "png", ...(shot.clip ? { clip: { ...shot.clip, scale: 1 } } : {}) });
        writeFileSync(join(OUT, `${shot.name}.png`), Buffer.from(data, "base64"));
        console.log("ok");
      } catch (e) {
        console.log("FAILED: " + e.message);
      } finally {
        if (shot.after) await cdp.eval(shot.after).catch(() => {});
      }
    }
    await cdp.send("Browser.close").catch(() => {});
  } finally {
    chrome.kill();
    await sleep(500);
    rmSync(profile, { recursive: true, force: true });
  }
}

main().catch(e => { console.error(e); process.exit(1); });
