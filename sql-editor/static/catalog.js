// The catalog browser and the table structure dialog, shared by the SQL editor
// (index.html) and the pipeline page (pipeline.html). Loaded after common.js; uses the
// page's own `session`. A page calls catalogInit() once, with what is page-specific:
//
//   catalogInit({
//     layout,    the element the panel is added to, as its first child
//     storeKey,  where the panel's open/closed state is remembered
//     insert,    (text) => …  a table or column name, double-clicked or "Insert name"
//     ddl,       (path) => …  the DDL button: path is [catalog, database, table]
//     foot,      the help line at the bottom of the panel (HTML)
//     onToggle,  optional (open) => …  after the panel opens or closes
//   })
//
// and calls catalogReload() when its session changes, and catalogAfterDdl() after a script.

let catalogHooks = null;

function catalogInit(hooks) {
  catalogHooks = hooks;
  hooks.layout.insertAdjacentHTML("afterbegin", `<!-- Catalog browser: catalogs > databases > tables > columns, listed with SHOW and
     DESCRIBE in this page's own session (so its in-memory tables show up too). -->
<aside id="catalog" hidden aria-label="Catalog browser">
  <div class="cat-head">
    <strong>Catalog</strong><span class="spacer"></span>
    <button type="button" id="catalogExpand" title="Expand all: every catalog and database" aria-label="Expand all catalogs and databases"><svg viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M7 10l5-5 5 5"/><path d="M7 14l5 5 5-5"/></svg></button>
    <button type="button" id="catalogCollapse" title="Collapse all" aria-label="Collapse the whole tree"><svg viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M7 5l5 5 5-5"/><path d="M7 19l5-5 5 5"/></svg></button>
    <button type="button" id="catalogRefresh" title="Refresh" aria-label="Refresh the catalog tree">↻</button>
    <button type="button" id="catalogClose" title="Close" aria-label="Close the catalog browser">✕</button>
  </div>
  <ul id="catalogTree" role="tree" aria-label="Catalogs"></ul>
  <p class="cat-foot" id="catalogFoot"></p>
</aside>`);
  document.body.insertAdjacentHTML("beforeend", `<!-- One table's structure, opened from the catalog browser's Info button. -->
<dialog id="structure" aria-labelledby="structTitle">
  <header>
    <span class="kind" id="structKind"></span>
    <h2 id="structTitle"></h2>
    <span class="spacer"></span>
    <button type="button" id="structInsert" title="Insert the table's name at the editor cursor">Insert name</button>
    <button type="button" id="structCopy" title="Copy the CREATE statement">Copy DDL</button>
    <button type="button" id="structClose" aria-label="Close">✕</button>
  </header>
  <div class="sbody" id="structBody"></div>
</dialog>`);
  $("catalogFoot").innerHTML = hooks.foot;
  $("structClose").onclick = () => $("structure").close();
  $("structure").addEventListener("click", e => { if (e.target === $("structure")) $("structure").close(); });   // backdrop
  $("structInsert").onclick = () => { $("structure").close(); catalogHooks.insert(qualified(structPath)); };
  $("structCopy").onclick = () => structDdl && copyText(structDdl, $("structCopy"));

  $("catalogToggle").onclick = () => catalogShow($("catalog").hidden);
  $("catalogClose").onclick = () => catalogShow(false);
  $("catalogRefresh").onclick = catalogRefreshAll;

  // Expand all: every catalog, then every database in them, level by level, so all tables
  // are listed. Not the tables themselves: that would be one DESCRIBE per table.
  $("catalogExpand").onclick = async () => {
    if (!session) return;
    const closed = depth => [...$("catalogTree").querySelectorAll('li[role="treeitem"]')]
      .filter(li => li.dataset.path.split("/").length === depth && !isOpen(li));
    for (const depth of [1, 2]) await Promise.all(closed(depth).map(li => catalogToggle(li, true)));
  };
  $("catalogCollapse").onclick = () => {
    for (const li of $("catalogTree").querySelectorAll('li[aria-expanded="true"]')) {
      li.setAttribute("aria-expanded", "false");
      li.querySelector(":scope > ul").hidden = true;
    }
    catalogOpen.clear();
  };
  if (store.get(catalogHooks.storeKey) === "1") catalogShow(true);
}


// Backticks only where Flink needs them, so inserted names read like hand-written SQL.
const quoteId = n => /^[A-Za-z_][A-Za-z0-9_]*$/.test(n) ? n : "`" + n.replace(/`/g, "``") + "`";
const qualified = path => path.map(quoteId).join(".");
const catalogOpen = new Set();   // expanded paths ("fluss", "fluss/orders", ...), kept across refreshes
const LEVELS = ["catalog", "database", "table"];
const KIND = { catalog: "CAT", database: "DB", table: "TBL" };

function catalogQuery(path) {
  const q = new URLSearchParams();
  path.forEach((v, i) => q.set(LEVELS[i], v));
  return `/api/catalog/${session}?${q}`;
}

function catalogMsg(text, err) {
  const li = document.createElement("li"); li.className = "cat-msg" + (err ? " err" : "");
  li.setAttribute("role", "none"); li.textContent = text; return li;
}

// One tree level under `ul`: path [] lists catalogs, [c] databases, [c, d] tables.
async function catalogLoad(ul, path) {
  ul.replaceChildren(catalogMsg("loading…"));
  let items;
  try { items = (await api(catalogQuery(path))).items; }
  catch (e) {
    const lines = e.message.split("\n").filter(l => l.startsWith("Caused by:"));
    ul.replaceChildren(catalogMsg((lines.pop() || e.message.split("\n")[0]).replace(/^Caused by:\s*(\w+\.)*/, ""), true));
    return;
  }
  ul.replaceChildren(...(items.length ? items.map(it => catalogNode(path.concat(it.name))) : [catalogMsg("empty")]));
  // Re-open whatever was open before a refresh.
  for (const li of ul.children) if (li.dataset.path && catalogOpen.has(li.dataset.path)) catalogToggle(li, true);
}

function catalogNode(path) {
  const level = LEVELS[path.length - 1];
  const li = document.createElement("li");
  li.setAttribute("role", "treeitem"); li.setAttribute("aria-expanded", "false");
  li.dataset.path = path.join("/");
  const row = document.createElement("div");
  row.className = "tnode " + level; row.tabIndex = 0;
  row.innerHTML = `<span class="caret" aria-hidden="true">▶</span><span class="kind"></span><span class="name"></span>`;
  row.querySelector(".kind").textContent = KIND[level];
  row.querySelector(".name").textContent = path[path.length - 1];
  row.title = qualified(path);
  if (level === "table") {
    const actions = document.createElement("span"); actions.className = "actions";
    actions.innerHTML = `<button type="button" data-act="info" title="Show the table's structure">Info</button><button type="button" data-act="ddl" title="Run SHOW CREATE TABLE">DDL</button>`;
    actions.onclick = e => {
      e.stopPropagation();
      const act = e.target.dataset.act;
      if (act === "info") openStructure(path);
      if (act === "ddl") catalogHooks.ddl(path);
    };
    row.append(actions);
    row.ondblclick = e => { e.preventDefault(); catalogHooks.insert(qualified(path)); };
  }
  row.onclick = () => catalogToggle(li);
  row.onkeydown = e => {
    if (e.key === "Enter" || e.key === " ") { e.preventDefault(); catalogToggle(li); }
    else if (e.key === "ArrowRight" && li.getAttribute("aria-expanded") === "false") catalogToggle(li, true);
    else if (e.key === "ArrowLeft" && li.getAttribute("aria-expanded") === "true") catalogToggle(li, false);
  };
  const ul = document.createElement("ul"); ul.setAttribute("role", "group"); ul.hidden = true;
  li.append(row, ul);
  return li;
}

async function catalogToggle(li, open) {
  const ul = li.querySelector(":scope > ul");
  open = open ?? li.getAttribute("aria-expanded") !== "true";
  li.setAttribute("aria-expanded", open);
  ul.hidden = !open;
  const key = li.dataset.path, path = key.split("/");
  if (!open) { catalogOpen.delete(key); return; }
  catalogOpen.add(key);
  return path.length < 3 ? catalogLoad(ul, path) : catalogColumns(ul, path, true);
}

// A table's columns, from DESCRIBE. `spinner` is off for a refresh, so the old columns
// stay on screen until the new ones replace them.
async function catalogColumns(ul, path, spinner) {
  if (spinner) ul.replaceChildren(catalogMsg("loading…"));
  try {
    const cols = (await api(catalogQuery(path))).items;
    ul.replaceChildren(...cols.map(c => {
      const row = document.createElement("li"); row.setAttribute("role", "none");
      row.innerHTML = `<div class="tnode column"><span class="name"></span><span class="type"></span></div>`;
      const node = row.firstChild;
      node.querySelector(".name").textContent = c.name;
      // DESCRIBE marks time attributes inside the type ("TIMESTAMP(3) *ROWTIME*"): badges instead.
      const attr = (c.type.match(/\*(ROWTIME|PROCTIME)\*/) || [])[1];
      node.querySelector(".type").textContent = c.type.replace(/\s*\*(ROWTIME|PROCTIME)\*/, "") + (c.null === false ? " NOT NULL" : "");
      node.querySelector(".type").title = c.type;
      const badge = (label, title) => { const b = document.createElement("span"); b.className = "badge"; b.textContent = label; b.title = title; node.append(b); };
      if (c.key) badge("PK", c.key);
      if (attr === "PROCTIME") badge("PROCTIME", "Processing-time attribute");
      if (c.watermark) badge("WM", "Event-time attribute · WATERMARK " + c.watermark);
      if (c.extras) node.title = c.extras;
      node.ondblclick = () => catalogHooks.insert(quoteId(c.name));
      return row;
    }));
  } catch (e) { ul.replaceChildren(catalogMsg(e.message.split("\n")[0], true)); }
}

// Full rebuild: on a new or resumed session, or when the panel opens.
function catalogReload() {
  if ($("catalog").hidden) return;
  if (!session) { $("catalogTree").replaceChildren(catalogMsg("no session")); return; }
  catalogLoad($("catalogTree"), []);
}

const catalogLi = path => $("catalogTree").querySelector(`li[data-path="${CSS.escape(path.join("/"))}"]`);
const isOpen = li => li?.getAttribute("aria-expanded") === "true";

// Refresh ONE level in place: nodes that still exist are kept as they are, open
// subtrees and all; new ones are added (and flash); dropped ones go. A level that is
// not on screen is left alone: it is fetched fresh whenever it is next expanded.
async function catalogRefreshLevel(path) {
  const li = path.length ? catalogLi(path) : null;
  if (path.length && !isOpen(li)) return;
  const ul = li ? li.querySelector(":scope > ul") : $("catalogTree");
  let items;
  try { items = (await api(catalogQuery(path))).items; } catch { return; }   // keep what is shown
  const have = new Map([...ul.children].filter(n => n.dataset.path).map(n => [n.dataset.path, n]));
  const nodes = items.map(it => {
    const key = path.concat(it.name).join("/");
    if (have.has(key)) return have.get(key);
    const n = catalogNode(path.concat(it.name));
    n.firstChild.classList.add("fresh");
    return n;
  });
  const keep = new Set(nodes.map(n => n.dataset.path));
  for (const key of have.keys()) if (!keep.has(key))   // dropped: forget it and anything under it
    for (const k of [...catalogOpen]) if (k === key || k.startsWith(key + "/")) catalogOpen.delete(k);
  ul.replaceChildren(...(nodes.length ? nodes : [catalogMsg("empty")]));
}

async function catalogRefreshColumns(path) {
  const li = catalogLi(path);
  if (isOpen(li)) await catalogColumns(li.querySelector(":scope > ul"), path, false);
}

// The ↻ button: every level on screen, top down, in place.
async function catalogRefreshAll() {
  if ($("catalog").hidden || !session) return;
  await catalogRefreshLevel([]);
  for (let depth = 1; depth <= 3; depth++) {
    const open = [...$("catalogTree").querySelectorAll('li[aria-expanded="true"]')]
      .map(li => li.dataset.path.split("/")).filter(p => p.length === depth);
    await Promise.all(open.map(p => depth < 3 ? catalogRefreshLevel(p) : catalogRefreshColumns(p)));
  }
}

// What a DDL statement changed. Its object name has 1-3 parts, plain or `quoted`.
const IDENT = "(?:`(?:[^`]|``)*`|[A-Za-z_$][\\w$]*)";
const DDL_RE = new RegExp(
  "^\\s*(create|drop|alter)\\s+(?:or\\s+replace\\s+)?(?:temporary\\s+)?(?:system\\s+)?" +
  "(catalog|database|table|view|function)\\s+(?:if\\s+(?:not\\s+)?exists\\s+)?" +
  `(${IDENT}(?:\\s*\\.\\s*${IDENT}){0,2})`, "i");
const nameParts = q => [...q.matchAll(new RegExp(IDENT, "g"))]
  .map(m => m[0].startsWith("`") ? m[0].slice(1, -1).replace(/``/g, "`") : m[0]);

// Does resolving this script's DDL need the session's current catalog and database?
// Only when it USEs or names an object without its full catalog.database prefix.
function needsCurrent(statements) {
  return statements.some(stmt => {
    if (/^\s*use\b/i.test(stmt)) return true;
    const m = stmt.match(DDL_RE);
    if (!m) return false;
    const k = m[2].toLowerCase(), n = nameParts(m[3]).length;
    return (k === "database" && n < 2) || ((k === "table" || k === "view") && n < 3);
  });
}

// After a script: refresh just the levels its successful DDL touched. Unqualified names
// resolve against `cur`, the session's current catalog and database from BEFORE the
// script ran, moved along by the script's own USE statements as they come.
async function catalogAfterDdl(statements, cur) {
  if ($("catalog").hidden || !session) return;
  if (!statements.some(s => /^\s*(create|drop|alter)\b/i.test(s))) return;
  cur = cur && { ...cur };
  const levels = new Map(), columns = new Map();
  for (const stmt of statements) {
    const use = stmt.match(new RegExp(`^\\s*use\\s+(catalog\\s+)?(${IDENT}(?:\\s*\\.\\s*${IDENT})?)\\s*$`, "i"));
    if (use) {
      const parts = nameParts(use[2]);
      if (!cur) continue;
      if (use[1]) cur = { catalog: parts[0], database: null };   // that catalog's default database: unknown here
      else if (parts.length === 2) cur = { catalog: parts[0], database: parts[1] };
      else cur = { ...cur, database: parts[0] };
      continue;
    }
    if (!/^\s*(create|drop|alter)\b/i.test(stmt)) continue;
    const m = stmt.match(DDL_RE);
    if (!m) return catalogRefreshAll();            // a DDL we do not parse: refresh what is shown
    const [, verb, kind, name] = m, parts = nameParts(name), k = kind.toLowerCase();
    if (k === "function") continue;                // functions are not in the tree
    if (k === "catalog") { levels.set("", []); continue; }
    const full = k === "database"
      ? (parts.length === 2 ? parts : [cur?.catalog, ...parts])
      : parts.length === 3 ? parts
      : parts.length === 2 ? [cur?.catalog, ...parts]
      : [cur?.catalog, cur?.database, ...parts];
    if (full.some(x => !x)) return catalogRefreshAll();   // could not resolve the name
    if (k === "database") { levels.set(full[0], [full[0]]); continue; }
    const db = full.slice(0, 2);
    levels.set(db.join("/"), db);
    if (verb.toLowerCase() === "alter") columns.set(full.join("/"), full);   // columns may have changed
  }
  // Shallow levels first: a refreshed catalog list may add the node a deeper one needs.
  for (const path of [...levels.values()].sort((a, b) => a.length - b.length)) await catalogRefreshLevel(path);
  await Promise.all([...columns.values()].map(catalogRefreshColumns));
}

function catalogShow(show) {
  $("catalog").hidden = !show;
  $("catalogToggle").setAttribute("aria-expanded", show);
  store.set(catalogHooks.storeKey, show ? "1" : null);
  if (show) catalogReload();
  catalogHooks.onToggle?.(show);   // e.g. the pipeline page re-fits its diagram to the narrower stage
}
// ── The structure dialog ──

// The WITH ( 'k' = 'v', ... ) options of a CREATE statement, in order.
function ddlOptions(ddl) {
  const i = ddl.search(/\)\s*WITH\s*\(/i);
  if (i < 0) return [];
  return [...ddl.slice(i).matchAll(/'((?:[^']|'')*)'\s*=\s*'((?:[^']|'')*)'/g)]
    .map(m => [m[1].replace(/''/g, "'"), m[2].replace(/''/g, "'")]);
}

let structPath = null, structDdl = "";

// `showDdl` opens the CREATE statement section straight away (the pipeline page's DDL button).
async function openStructure(path, showDdl) {
  structPath = path; structDdl = "";
  const d = $("structure");
  $("structTitle").textContent = qualified(path);
  $("structKind").textContent = "TBL";
  $("structBody").innerHTML = `<div class="loading"><span class="spinner" aria-hidden="true"></span>Reading the table's structure…</div>`;
  if (!d.open) d.showModal();
  const q = new URLSearchParams({ catalog: path[0], database: path[1], table: path[2] });
  let s;
  try { s = await api(`/api/catalog/${session}/structure?${q}`); }
  catch (e) {
    const cause = e.message.split("\n").filter(l => l.startsWith("Caused by:")).pop();
    $("structBody").innerHTML = `<div class="error"></div>`;
    $("structBody").firstChild.textContent = (cause || e.message.split("\n")[0]).replace(/^Caused by:\s*(\w+\.)*/, "");
    return;
  }
  if (structPath !== path) return;   // another table was opened meanwhile
  structDdl = s.ddl || "";
  $("structKind").textContent = s.kind === "view" ? "VIEW" : "TBL";
  const opts = ddlOptions(structDdl), opt = Object.fromEntries(opts);
  const pk = s.columns.find(c => c.key)?.key?.replace(/^PRI\((.*)\)$/, "$1");
  const wm = s.columns.filter(c => c.watermark);
  const chip = (html, cls = "") => `<span class="chip ${cls}">${html}</span>`;
  const esc = t => String(t).replace(/[&<>"]/g, ch => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[ch]);
  const chips = [chip(`<b>${s.columns.length}</b> column${s.columns.length === 1 ? "" : "s"}`)];
  if (pk) chips.push(chip(`primary key <code>${esc(pk)}</code>`));
  for (const c of wm) chips.push(chip(`watermark <code>${esc(c.watermark)}</code>`));
  if (opt.connector) chips.push(chip(`connector <code>${esc(opt.connector)}</code>`));
  if (opt["table.datalake.enabled"] === "true")
    chips.push(chip(`tiered to <b>${esc(opt["table.datalake.format"] || "the lake")}</b>` +
                    (opt["table.datalake.freshness"] ? ` · freshness ${esc(opt["table.datalake.freshness"])}` : ""), "lake"));
  if (opt["table.merge-engine"]) chips.push(chip(`merge engine <code>${esc(opt["table.merge-engine"])}</code>`));
  const rows = s.columns.map((c, i) => {
    const attr = (c.type.match(/\*(ROWTIME|PROCTIME)\*/) || [])[1];
    const notes = [c.watermark && `event time · WATERMARK ${c.watermark}`, attr === "PROCTIME" && "processing time", c.extras]
      .filter(Boolean).map(esc).join(" · ");
    return `<tr><td class="num">${i + 1}</td><td class="name">${esc(c.name)}</td>` +
      `<td class="type">${esc(c.type.replace(/\s*\*(ROWTIME|PROCTIME)\*/, ""))}</td>` +
      `<td>${c.null ? "" : "NOT NULL"}</td>` +
      `<td>${c.key ? '<span class="badge">PK</span>' : ""}${c.watermark ? '<span class="badge">WM</span>' : ""}</td>` +
      `<td class="notes">${notes}</td></tr>`;
  }).join("");
  $("structBody").innerHTML =
    `<div class="chips">${chips.join("")}</div>` +
    `<h3>Columns</h3><table><thead><tr><th>#</th><th>Name</th><th>Type</th><th>Null</th><th>Key</th><th>Notes</th></tr></thead><tbody>${rows}</tbody></table>` +
    (opts.length ? `<h3>Options</h3><table><tbody>${opts.map(([k, v]) =>
      `<tr><td class="opt-key">${esc(k)}</td><td class="opt-val">${esc(v)}</td></tr>`).join("")}</tbody></table>` : "") +
    (structDdl ? `<details${showDdl ? " open" : ""}><summary>CREATE statement</summary><pre class="ddl"></pre></details>` : "");
  const pre = $("structBody").querySelector("pre.ddl");
  if (pre) CodeMirror.runMode(structDdl, "text/x-flinksql", pre);
}
