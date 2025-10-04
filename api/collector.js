// collector-fixed.js
// ESM Express app — Vercel-ready (patched for timeouts, parallel reads, and safer retries).
// Dependencies: express, body-parser, morgan, node-fetch
// Install: npm i express body-parser morgan node-fetch
// IMPORTANT: set env vars in Vercel project settings. See notes at bottom of file.

import express from "express";
import morgan from "morgan";
import fetch from "node-fetch";
import dotenv from "dotenv";

// Load .env in non-production (local dev). Place your local env vars in a .env file at project root.
if (process.env.NODE_ENV !== "production") {
  dotenv.config({ path: process.env.ENV_PATH || ".env" });
  console.log("[env] loaded for dev, GITHUB_TOKENS count:", (process.env.GITHUB_TOKENS || "").split(",").filter(Boolean).length);
}

const app = express();
app.use(morgan("tiny"));
// Use a tolerant JSON body parser to avoid `request size did not match content length` errors
// (some proxies / clients may send incorrect Content-Length headers). This parser reads the stream
// manually and enforces a hard byte limit.
const MAX_BODY_BYTES = Math.max(1024, parseInt(process.env.MAX_BODY_BYTES || "2097152", 10)); // default 2MB
app.use((req, res, next) => {
  // Only parse JSON-ish content here; let other routes pass through
  const ct = (req.headers['content-type'] || '').toLowerCase();
  if (!ct.includes('application/json')) return next();

  let received = 0;
  let chunks = '';
  req.setEncoding('utf8');

  req.on('data', (chunk) => {
    received += chunk.length;
    if (received > MAX_BODY_BYTES) {
      // stop parsing and fail fast
      req.connection && req.connection.destroy && req.connection.destroy();
      return; // stream will end with error on client side
    }
    chunks += chunk;
  });

  req.on('end', () => {
    if (!chunks) { req.body = {}; return next(); }
    try {
      req.body = JSON.parse(chunks);
      return next();
    } catch (e) {
      console.warn('tolerant-parser: invalid json', e.message);
      return res.status(400).json({ error: 'invalid JSON' });
    }
  });

  req.on('error', (err) => {
    console.warn('tolerant-parser error', err && err.message);
    return res.status(400).json({ error: 'request read error' });
  });
});

// ----------------- CONFIG (ENV) -----------------
const TOKENS = (process.env.GITHUB_TOKENS || "")
  .split(",").map(s => s.trim()).filter(Boolean);

const DATA_UTAMA_GISTS = (process.env.GIST_DATA_UTAMA || "")
  .split(",").map(s => s.trim()).filter(Boolean);

const GIST_ID_GLOBAL = (process.env.GIST_ID_GLOBAL || "").trim() || null;
const GIST_DATA_TANGGAL = (process.env.GIST_DATA_TANGGAL || "").trim() || null;

// Tunable defaults (made more conservative for serverless)
const MAX_ITEMS_PER_FILE = Math.max(1, parseInt(process.env.MAX_ITEMS_PER_FILE || "1000", 10));
const MAX_RETRIES = Math.max(1, parseInt(process.env.MAX_RETRIES || "2", 10)); // reduced default
const RETRY_BASE_MS = Math.max(50, parseInt(process.env.RETRY_BASE_MS || "300", 10));
const TOKEN_BACKOFF_SEC = Math.max(5, parseInt(process.env.TOKEN_BACKOFF_SEC || "60", 10));
const USER_AGENT = process.env.USER_AGENT || "membit-collector";
const MAX_SYNC_ATTEMPTS = Math.max(1, parseInt(process.env.MAX_SYNC_ATTEMPTS || "3", 10)); // reduced default
const SYNC_RETRY_BASE_MS = Math.max(50, parseInt(process.env.SYNC_RETRY_BASE_MS || "200", 10));
const READ_BATCH_SIZE = Math.min(Math.max(parseInt(process.env.READ_BATCH_SIZE || "200", 10), 1), 500);
const FETCH_TIMEOUT_MS = Math.max(1000, parseInt(process.env.FETCH_TIMEOUT_MS || "8000", 10));

// ----------------- small utils -----------------
function nowMs(){ return Date.now(); }
function sleep(ms){ return new Promise(r => setTimeout(r, ms)); }
function utcYMD(d = new Date()){ return d.toISOString().slice(0, 10); }

// ----------------- Token rotation & simple backoff -----------------
const tokenDisabledUntil = {}; // token -> timestamp ms
function isTokenAvailable(token){ return (tokenDisabledUntil[token] || 0) <= nowMs(); }

let tokenIndex = 0;
function pickNextAvailableToken(){
  if (!TOKENS.length) return null;
  const start = tokenIndex % TOKENS.length;
  for (let i = 0; i < TOKENS.length; i++){
    const idx = (start + i) % TOKENS.length;
    const t = TOKENS[idx];
    if (isTokenAvailable(t)){
      tokenIndex = (idx + 1) % TOKENS.length;
      return t;
    }
  }
  return null;
}
function disableTokenFor(token, sec){
  if (!token) return;
  tokenDisabledUntil[token] = nowMs() + sec * 1000;
}

// ----------------- GitHub Gist helpers with retry & token fallback -----------------
async function ghFetch(url, opts = {}){
  const token = pickNextAvailableToken();
  const headers = Object.assign({}, opts.headers || {});
  headers["User-Agent"] = USER_AGENT;
  if (token) headers["Authorization"] = `token ${token}`;

  // AbortController timeout (Node 18+ in Vercel provides global AbortController)
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
  try {
    const finalOpts = Object.assign({}, opts, { headers, signal: controller.signal });
    const start = Date.now();
    const res = await fetch(url, finalOpts);
    const took = Date.now() - start;
    console.log(`[ghFetch] url=${url} status=${res.status} token=${token? 'yes':'no'} took=${took}ms`);
    return { res, token };
  } catch (e){
    if (e.name === 'AbortError') {
      console.warn(`[ghFetch] timeout for ${url} after ${FETCH_TIMEOUT_MS}ms`);
    } else {
      console.warn(`[ghFetch] error fetching ${url}: ${e.message}`);
    }
    throw e;
  } finally {
    clearTimeout(timeout);
  }
}

async function ghFetchWithRetries(url, opts = {}){
  let lastErr = null;
  for (let attempt = 0; attempt < MAX_RETRIES; attempt++){
    try {
      console.log(`[ghFetchWithRetries] attempt=${attempt+1}/${MAX_RETRIES} url=${url}`);
      const { res, token } = await ghFetch(url, opts);
      if (res.status === 429){
        if (token) {
          disableTokenFor(token, TOKEN_BACKOFF_SEC);
          console.warn(`[ghFetchWithRetries] 429 -> disabling token for ${TOKEN_BACKOFF_SEC}s`);
        }
        const text = await res.text().catch(()=>"");
        lastErr = new Error(`GitHub 429: ${text}`);
        const backoff = RETRY_BASE_MS * Math.pow(2, attempt);
        console.log(`[ghFetchWithRetries] sleeping ${backoff}ms after 429`);
        await sleep(backoff);
        continue;
      }
      if (res.status >= 500){
        if (token) {
          disableTokenFor(token, Math.max(5, Math.floor(TOKEN_BACKOFF_SEC/4)));
          console.warn(`[ghFetchWithRetries] ${res.status} -> temporary disable token`);
        }
        const text = await res.text().catch(()=>"");
        lastErr = new Error(`GitHub ${res.status}: ${text}`);
        const backoff = RETRY_BASE_MS * Math.pow(2, attempt);
        console.log(`[ghFetchWithRetries] sleeping ${backoff}ms after ${res.status}`);
        await sleep(backoff);
        continue;
      }
      // return res (200..499) to caller
      return { res, token };
    } catch (e){
      lastErr = e;
      const backoff = RETRY_BASE_MS * Math.pow(2, attempt);
      console.log(`[ghFetchWithRetries] fetch error (${e.message}), sleeping ${backoff}ms before retry`);
      await sleep(backoff);
      continue;
    }
  }
  throw lastErr || new Error("ghFetchWithRetries failed");
}

// API base (allow mocking in dev/local via GITHUB_API_BASE)
const GITHUB_API_BASE = (process.env.GITHUB_API_BASE || "https://api.github.com").replace(/\/$/, "");

// Compatibility wrapper: patch helper used across the codebase. This uses the same
// ghFetchWithRetries flow and returns parsed JSON on success.
if (typeof globalThis.patchGistWithRetries === 'undefined') {
  globalThis.patchGistWithRetries = async function(gistId, filesObj) {
    const url = `${GITHUB_API_BASE}/gists/${gistId}`;
    const opts = {
      method: "PATCH",
      headers: { "Content-Type": "application/json", "User-Agent": USER_AGENT },
      body: JSON.stringify({ files: filesObj })
    };
    const { res } = await ghFetchWithRetries(url, opts);
    if (!res.ok) {
      const text = await res.text().catch(()=>"");
      throw new Error(`Failed PATCH gist ${gistId}: ${res.status} ${text}`);
    }
    const j = await res.json();
    console.log(`[PATCH RESP] gist=${gistId} files=${Object.keys(filesObj).join(',')} status=${res.status}`);
    return j;
  };
}

if (typeof globalThis.patchGistWithRetries === 'undefined') {
  globalThis.patchGistWithRetries = async function(gistId, filesObj) {
    const url = `${GITHUB_API_BASE}/gists/${gistId}`;
    const opts = {
      method: "PATCH",
      headers: { "Content-Type": "application/json", "User-Agent": USER_AGENT },
      body: JSON.stringify({ files: filesObj })
    };
    const { res } = await ghFetchWithRetries(url, opts);
    if (!res.ok) {
      const text = await res.text().catch(()=>"");
      throw new Error(`Failed PATCH gist ${gistId}: ${res.status} ${text}`);
    }
    const j = await res.json();
    console.log(`[PATCH RESP] gist=${gistId} files=${Object.keys(filesObj).join(',')} status=${res.status}`);
    return j;
  };
}

// ----------------- Date gist selection & daily reset -----------------
function pickDateGistId(){
  if (process.env.GIST_DATA_TANGGAL && process.env.GIST_DATA_TANGGAL.trim()) return process.env.GIST_DATA_TANGGAL.trim();
  if (GIST_ID_GLOBAL) return GIST_ID_GLOBAL;
  if (DATA_UTAMA_GISTS && DATA_UTAMA_GISTS.length > 0) return DATA_UTAMA_GISTS[0];
  return null;
}
async function getDateFromSelectedGist(){
  const gid = pickDateGistId();
  if (!gid) return null;
  try {
    const { filename, content } = await fetchGistContent(gid);
    if (!content) return null;
    if (typeof content === "string") return content;
    if (content.last_date) return String(content.last_date);
    if (content.date) return String(content.date);
    return null;
  } catch (e) {
    console.warn("getDateFromSelectedGist warning:", e.message);
    return null;
  }
}
async function writeDateToSelectedGist(dateStr){
  const gid = pickDateGistId();
  if (!gid) {
    console.warn("writeDateToSelectedGist: no gist available to write date");
    return null;
  }
  try {
    const gistInfo = await fetchGistContent(gid);
    const filename = gistInfo.filename || "data_tanggal.json";
    const filesObj = {};
    filesObj[filename] = { content: JSON.stringify({ last_date: dateStr }, null, 2) };
    return await patchGistWithRetries(gid, filesObj);
  } catch (e) {
    console.warn("writeDateToSelectedGist error:", e.message);
    return null;
  }
}
async function clearAllDataUtamaAndIdGlobal(){
  // clear data_utama in parallel (safe in most cases)
  await Promise.allSettled(DATA_UTAMA_GISTS.map(async (gid) => {
    try {
      const gi = await fetchGistContent(gid);
      const filename = gi.filename || "data_utama.json";
      const filesObj = {};
      filesObj[filename] = { content: JSON.stringify([], null, 2) };
      await patchGistWithRetries(gid, filesObj);
      console.log(`Cleared data_utama gist ${gid}`);
    } catch (e) {
      console.warn(`Failed clearing data_utama gist ${gid}: ${e.message}`);
    }
  }));

  // clear global id gist (single)
  if (GIST_ID_GLOBAL) {
    try {
      const gi = await fetchGistContent(GIST_ID_GLOBAL);
      const filename = gi.filename || "data_id_global.json";
      const filesObj = {};
      filesObj[filename] = { content: JSON.stringify({ seen: [] }, null, 2) };
      await patchGistWithRetries(GIST_ID_GLOBAL, filesObj);
      console.log("Cleared data_id_global gist");
    } catch (e) {
      console.warn("Failed clearing data_id_global:", e.message);
    }
  }
}
async function checkAndResetDaily(){
  const today = utcYMD();
  try {
    const last = await getDateFromSelectedGist();
    // If last is null, this is likely first-run / uninitialized. Do NOT clear existing gists in that case.
    // Instead, initialize the date gist to avoid accidental wipes on first run.
    if (last === null) {
      console.log("checkAndResetDaily: date gist uninitialized. Initializing to", today, "and skipping clear.");
      await writeDateToSelectedGist(today);
      return;
    }
    if (last === today) return;
    console.log("Daily reset triggered. last_date:", last, "today:", today);
    const dateGistId = pickDateGistId();
    if (!dateGistId) {
      console.warn("checkAndResetDaily: no gist configured to store date - skipping reset");
      return;
    }
    await clearAllDataUtamaAndIdGlobal();
    await writeDateToSelectedGist(today);
    console.log("Daily reset complete:", today);
  } catch (e) {
    console.warn("checkAndResetDaily error:", e.message ?? e);
  }
}

// ----------------- id_global helpers -----------------
async function getIdGlobalSet(){
  if (!GIST_ID_GLOBAL) return new Set();
  try {
    const { filename, content } = await fetchGistContent(GIST_ID_GLOBAL);
    if (!content) return new Set();
    let arr = [];
    if (Array.isArray(content)) arr = content;
    else if (Array.isArray(content.seen)) arr = content.seen;
    else if (Array.isArray(content.ids)) arr = content.ids;
    else return new Set();
    return new Set(arr.map(String));
  } catch (e){
    console.warn("getIdGlobalSet warning:", e.message);
    return new Set();
  }
}
async function writeIdGlobalSet(idArray){
  if (!GIST_ID_GLOBAL) throw new Error("GIST_ID_GLOBAL not configured");
  const payload = JSON.stringify({ seen: idArray }, null, 2);
  const gistInfo = await fetchGistContent(GIST_ID_GLOBAL);
  const filename = gistInfo.filename || "data_id_global.json";
  const filesObj = {};
  filesObj[filename] = { content: payload };
  return await patchGistWithRetries(GIST_ID_GLOBAL, filesObj);
}

// ----------------- data_utama helpers -----------------
async function readAllDataUtama(){
  // parallelize reads to reduce total latency
  const promises = DATA_UTAMA_GISTS.map(async (gid) => {
    try {
      const g = await fetchGistContent(gid);
      let arr = [];
      if (Array.isArray(g.content)) arr = g.content;
      else if (g.content && Array.isArray(g.content.posts)) arr = g.content.posts;
      else arr = [];
      return { gistId: gid, filename: g.filename || "data_utama.json", array: arr };
    } catch (e) {
      console.warn(`readAllDataUtama: can't read gist ${gid}: ${e.message}`);
      return { gistId: gid, filename: "data_utama.json", array: [] };
    }
  });
  return Promise.all(promises);
}

// ----------------- SAFE append (optimistic sync) -----------------
function jitterBackoff(attempt, baseMs = SYNC_RETRY_BASE_MS){
  const exp = Math.pow(2, attempt) * baseMs;
  const jitter = Math.floor(Math.random() * baseMs);
  return exp + jitter;
}

/*
 gistEntry: { gistId, filename, array: existingArray }
 incomingItems: array of objects to store
 returns { stored: [...], notStored: [...], updatedArray: [...] }
*/
if (typeof globalThis.safeAppendToGist === 'undefined') {
  globalThis.safeAppendToGist = async function(gistEntry, incomingItems){
    return safeAppendToGistSimple(gistEntry, incomingItems);
  };
}

// ----------------- ROUTES -----------------
// Support both root (when deployed as api/collector) and explicit /collector paths
app.options(["/collector", "/"], (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, X-SECRET");
  res.status(200).end();
});

// POST /collector
app.post(["/collector", "/"], async (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  try {
    await checkAndResetDaily();

    let items = [];
    if (Array.isArray(req.body)) items = req.body;
    else if (req.body && Array.isArray(req.body.posts)) items = req.body.posts;
    else return res.status(400).json({ error: "Body must be array or { posts: [...] }" });

    if (items.length === 0) return res.status(400).json({ error: "No items provided" });

    // normalize to objects
    items = items.map(it => (typeof it === "object" ? it : { text: String(it) }));

    // read id_global (dedupe)
    const seenSet = await getIdGlobalSet();

    const incomingById = [];
    const incomingNoId = [];
    let skipped = 0;
    for (const it of items){
      const id = it?.rest_id ?? it?.id ?? null;
      if (!id) incomingNoId.push(it);
      else {
        if (seenSet.has(String(id))) skipped++;
        else incomingById.push({ id: String(id), obj: it });
      }
    }

    const newItemsObjs = [...incomingById.map(x => x.obj), ...incomingNoId];
    if (newItemsObjs.length === 0){
      return res.status(200).json({ accepted: 0, skipped, stored: 0 });
    }

    const dataUtamaList = await readAllDataUtama();

    // Quick capacity check to avoid heavy work when there's absolutely no room
    const capacities = dataUtamaList.map(d => Math.max(0, MAX_ITEMS_PER_FILE - (Array.isArray(d.array) ? d.array.length : 0)));
    const totalCap = capacities.reduce((a,b) => a + b, 0);
    if (totalCap === 0){
      return res.status(202).json({ accepted: newItemsObjs.length, skipped, stored: 0, not_stored: newItemsObjs.length, warning: "No capacity in DATA_UTAMA gists" });
    }

    let remaining = newItemsObjs.slice();
    const storedItems = [];
    const updatedGists = [];

    for (let i = 0; i < dataUtamaList.length && remaining.length > 0; i++){
      const entry = dataUtamaList[i];
      try {
        const result = await safeAppendToGist(entry, remaining);
        if (result.stored && result.stored.length > 0){
          storedItems.push(...result.stored);
          updatedGists.push(entry.gistId);
        }
        remaining = result.notStored;
      } catch (e){
        console.warn(`safeAppendToGist failed for gist ${entry.gistId}: ${e.message}`);
        continue;
      }
    }

    // update id_global only for ids that were actually stored
    const storedIds = storedItems
      .map(it => it?.rest_id ?? it?.id ?? null)
      .filter(Boolean)
      .map(String);

    if (storedIds.length > 0){
      const merged = new Set([...Array.from(await getIdGlobalSet()), ...storedIds]);
      await writeIdGlobalSet(Array.from(merged));
    }

    const response = {
      accepted: newItemsObjs.length,
      skipped,
      stored: storedItems.length,
      not_stored: remaining.length,
      stored_gists: Array.from(new Set(updatedGists)),
      not_stored_examples: remaining.slice(0,5)
    };

    if (remaining.length > 0){
      response.warning = `Not enough capacity in configured GIST_DATA_UTAMA; ${remaining.length} items not stored. Add more gist IDs to GIST_DATA_UTAMA env.`;
      return res.status(202).json(response);
    }

    return res.status(201).json(response);

  } catch (err){
    console.error("POST /collector error:", err);
    return res.status(500).json({ error: err?.message ?? "internal" });
  }
});

// GET /collector with batch support
// query: ?batch=1&batch_size=200
app.get(["/collector", "/"], async (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  try {
    await checkAndResetDaily();

    const q = req.query || {};
    const batchParam = q.batch;
    const batchNum = Math.max(parseInt(batchParam || "1", 10) || 1, 1);

    const defaultBatchSize = READ_BATCH_SIZE;
    const batchSize = Math.min(Math.max(parseInt(q.batch_size || String(defaultBatchSize), 10) || defaultBatchSize, 1), 500);

    const dataUtamaList = await readAllDataUtama();
    const merged = [];
    for (const e of dataUtamaList) {
      if (Array.isArray(e.array)) merged.push(...e.array);
    }

    const total = merged.length;
    const batchCount = total > 0 ? Math.ceil(total / batchSize) : 0;
    const batchArr = Array.from({ length: batchCount }, (_, i) => i + 1);

    if (batchCount === 0) {
      return res.status(200).json({ batches: [], total, batch_count: 0, batch: [] });
    }

    if (batchNum > batchCount) {
      return res.status(200).json({
        batches: [{ idx: batchNum, count: 0, posts: [] }],
        total,
        batch_count: batchCount,
        batch: batchArr,
      });
    }

    const start = (batchNum - 1) * batchSize;
    const slice = merged.slice(start, start + batchSize);
    const result = [{ idx: batchNum, count: slice.length, posts: slice }];

    return res.status(200).json({ batches: result, total, batch_count: batchCount, batch: batchArr });
  } catch (err) {
    console.error("GET /collector error:", err);
    return res.status(500).json({ error: err?.message ?? "internal" });
  }
});

// health
app.get("/health", (req, res) => {
  res.json({
    ok: true,
    gist_data_utama_count: DATA_UTAMA_GISTS.length,
    gist_id_global: !!GIST_ID_GLOBAL,
    max_items_per_file: MAX_ITEMS_PER_FILE,
    tokens: TOKENS.length
  });
});

// For Vercel, exporting the Express app directly can be used as the default handler.
// serverless-http wrapper is optional for Vercel and can introduce extra overhead. Exporting the
// app directly usually works: Vercel will call it as a Node.js serverless function (app is a function).
// If you prefer to keep serverless-http for other providers, you can revert to `export default serverless(app);`.

import serverless from "serverless-http";

// Start a local HTTP server when not in production for quick dev testing
if (process.env.NODE_ENV !== "production") {
  const port = parseInt(process.env.PORT || "3000", 10);
  app.listen(port, () => console.log(`collector dev server listening at http://localhost:${port}`));
}

// Export serverless handler for Vercel — this provides the proper (req,res) function wrapper
export default serverless(app);

/*
DEPLOY NOTES (Vercel):
- Add/Update environment variables in your Vercel Project > Settings > Environment Variables:
  - GITHUB_TOKENS (comma-separated)
  - GIST_DATA_UTAMA (comma-separated gist IDs)
  - GIST_ID_GLOBAL (optional)
  - GIST_DATA_TANGGAL (optional)
  - FETCH_TIMEOUT_MS (ms, default 8000)
  - MAX_RETRIES (default reduced to 2)
  - MAX_SYNC_ATTEMPTS (default reduced to 3)
- Redeploy the project after updating env vars.
- Monitor function logs in Vercel to confirm reduced timeouts and watch ghFetch logs.

WHY THESE CHANGES:
- Adds fetch timeout via AbortController to avoid hanging network calls.
- Parallelizes expensive reads/clears to reduce total latency in serverless environment.
- Lowers retry/sync defaults to fail-fast and avoid long backoff loops inside a single request.
- Adds logging so you can observe where time is spent in requests.
*/
