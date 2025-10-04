// api/collector.js
// Serverless-compatible collector for Vercel / Next.js API routes
import { Redis } from "@upstash/redis";

const redis = Redis.fromEnv();

// env / defaults
const FLUSH_SECRET = process.env.FLUSH_SECRET || null;
const CHUNK_SIZE = parseInt(process.env.CHUNK_SIZE || "100", 10);
const READ_BATCH_SIZE = Math.min(Math.max(parseInt(process.env.BATCH_SIZE || "200", 10), 1), 500);
const BATCH_PUSH_THRESHOLD = parseInt(process.env.BATCH_PUSH_THRESHOLD || "0", 10) || 0;
const RETRY_ATTEMPTS = Math.max(parseInt(process.env.RETRY_ATTEMPTS || "3", 10), 1);

const LAST_DATE_KEY = "membit:last_date";
const INPUT_LIST = "membit:input:list";
const INPUT_SEEN = "membit:input:seen";
const READ_LIST = "membit:read:list";
const READ_SEEN = "membit:read:seen";
const PROCESSING_LIST = "membit:processing:list";
const LOCK_FLUSH = "membit:lock:flush";

// helpers
const utcYMD = (d = new Date()) => d.toISOString().slice(0, 10);
const itemIdOf = (it) => it?.rest_id ?? it?.id ?? null;

async function acquireLock(ttlMs = 60_000) {
  try {
    const token = `t-${Math.random().toString(36).slice(2,9)}`;
    const ok = await redis.setnx(LOCK_FLUSH, token);
    if (ok === 1) {
      await redis.pexpire(LOCK_FLUSH, ttlMs);
      return token;
    }
    return null;
  } catch (e) { return null; }
}
async function releaseLock() {
  try { await redis.del(LOCK_FLUSH); } catch {}
}

async function checkAndResetDaily() {
  const today = utcYMD();
  try {
    const last = await redis.get(LAST_DATE_KEY);
    if (last !== today) {
      await redis.del(INPUT_LIST, INPUT_SEEN, READ_LIST, READ_SEEN);
      await redis.set(LAST_DATE_KEY, today);
    }
  } catch (e) { /* ignore */ }
}

// detect placeholder / invalid content in READ_LIST
async function readListLooksValid() {
  const first = await redis.lindex(READ_LIST, 0);
  if (!first) return false;
  try { JSON.parse(first); return true; } catch { return false; }
}

// If PROCESSING_LIST has stale items (serverless restarts), move back to input to allow retry.
// This runs quickly at beginning of flushAll to avoid separate startup tasks.
async function recoverProcessingToInput() {
  const procLen = Number(await redis.llen(PROCESSING_LIST) || 0);
  if (procLen === 0) return 0;
  let moved = 0;
  while (true) {
    const itm = await redis.rpop(PROCESSING_LIST);
    if (!itm) break;
    await redis.lpush(INPUT_LIST, itm);
    moved++;
    if (moved >= 1000) break; // safety cap per invocation
  }
  return moved;
}

async function ensureReadStructures() {
  let totalRead = Number(await redis.llen(READ_LIST) || 0);

  if (totalRead > 0) {
    const valid = await readListLooksValid();
    if (!valid) {
      await redis.del(READ_LIST, READ_SEEN);
      totalRead = 0;
    }
  }

  if (totalRead === 0) {
    const inputs = await redis.lrange(INPUT_LIST, 0, -1);
    if (!inputs || inputs.length === 0) return 0;
    let moved = 0;
    for (const s of inputs) {
      let obj;
      try { obj = JSON.parse(s); } catch { obj = { _raw: s }; }
      const id = itemIdOf(obj);
      try {
        await redis.rpush(READ_LIST, JSON.stringify(obj));
        if (id) await redis.sadd(READ_SEEN, String(id));
        moved++;
      } catch (e) { /* skip */ }
    }
    if (moved > 0) {
      await redis.del(INPUT_LIST, INPUT_SEEN);
    }
    return moved;
  }
  return -1;
}

async function flushAll() {
  // bootstrap if read list empty
  const boot = await ensureReadStructures();
  if (boot >= 0) return { flushed: boot, bootstrap: true };

  // recover processing items back to input if any
  await recoverProcessingToInput();

  let flushed = 0;
  while (true) {
    // atomically move one item from input -> processing
    const s = await redis.rpoplpush(INPUT_LIST, PROCESSING_LIST);
    if (!s) break;

    let parsed = null;
    try { parsed = JSON.parse(s); } catch {}

    if (!parsed) {
      // push raw object to READ_LIST (so it won't get lost)
      const obj = { _raw: s };
      try { await redis.rpush(READ_LIST, JSON.stringify(obj)); flushed++; } catch (e) {
        // failed writing -> move back and break
        await redis.lpush(INPUT_LIST, s);
        await redis.lrem(PROCESSING_LIST, 1, s);
        break;
      }
      await redis.lrem(PROCESSING_LIST, 1, s);
      continue;
    }

    const id = itemIdOf(parsed);
    if (!id) {
      parsed._generated_id = `_noid_${Math.random().toString(36).slice(2,9)}`;
      try { await redis.rpush(READ_LIST, JSON.stringify(parsed)); flushed++; } catch (e) {
        await redis.lpush(INPUT_LIST, s);
        await redis.lrem(PROCESSING_LIST, 1, s);
        break;
      }
      await redis.lrem(PROCESSING_LIST, 1, s);
      continue;
    }

    try {
      const added = await redis.sadd(READ_SEEN, String(id));
      if (added === 1) {
        await redis.rpush(READ_LIST, JSON.stringify(parsed));
        flushed++;
      } // else duplicate -> skip
    } catch (e) {
      await redis.lpush(INPUT_LIST, s);
      await redis.lrem(PROCESSING_LIST, 1, s);
      break;
    }

    await redis.lrem(PROCESSING_LIST, 1, s);

    // throttle / safety: break if one invocation processed too many items
    if (flushed >= CHUNK_SIZE) break;
  }

  return { flushed, bootstrap: false };
}

async function readSlice(offset, count) {
  const end = offset + count - 1;
  const arr = await redis.lrange(READ_LIST, offset, end);
  return arr.map((s) => {
    try { return JSON.parse(s); } catch { return null; }
  }).filter(Boolean);
}

// serverless handler
export default async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, X-SECRET");

  if (req.method === "OPTIONS") return res.status(200).end();

  try {
    await checkAndResetDaily();

    // GET
    if (req.method === "GET") {
      const q = req.query || {};
      const flush = Object.prototype.hasOwnProperty.call(q, "flush");

      if (flush) {
        if (FLUSH_SECRET) {
          const provided = req.headers["x-secret"] || req.headers["X-SECRET"];
          if (!provided || provided !== FLUSH_SECRET) {
            return res.status(401).json({ error: "Invalid flush secret" });
          }
        }
        const token = await acquireLock();
        if (!token) return res.status(423).json({ error: "Flush in progress" });
        try {
          const result = await flushAll();
          return res.status(200).json(result);
        } finally {
          await releaseLock();
        }
      }

      // normal read
      const batchesParam = q.batches;
      const batchSize = Math.min(Math.max(parseInt(q.batch_size || String(READ_BATCH_SIZE), 10) || READ_BATCH_SIZE, 1), 500);
      const total = Number(await redis.llen(READ_LIST) || 0);

      if (batchesParam) {
        const parts = String(batchesParam).split(",").map((s) => parseInt(s.trim(), 10)).filter((n) => Number.isFinite(n) && n > 0);
        const result = [];
        for (const idx of parts) {
          const start = (idx - 1) * batchSize;
          const slice = await readSlice(start, batchSize);
          result.push({ idx, count: slice.length, posts: slice });
        }
        return res.status(200).json({ batches: result, total });
      }

      const cursor = Math.max(parseInt(q.cursor || "0", 10) || 0, 0);
      const limit = Math.min(Math.max(parseInt(q.limit || String(batchSize), 10) || batchSize, 1), 1000);
      const slice = await readSlice(cursor, limit);
      const nextCursor = cursor + slice.length;
      return res.status(200).json({ record: slice, cursor: nextCursor, total });
    }

    // POST
    if (req.method === "POST") {
      let items = [];
      if (Array.isArray(req.body)) items = req.body;
      else if (req.body && Array.isArray(req.body.posts)) items = req.body.posts;
      else return res.status(400).json({ error: "Body must be array or { posts: [...] }" });

      let accepted = 0, skipped = 0;
      for (const it of items) {
        const id = itemIdOf(it);
        const serialized = JSON.stringify(it);
        if (!id) {
          await redis.rpush(INPUT_LIST, serialized);
          accepted++;
        } else {
          const alreadyRead = await redis.sismember(READ_SEEN, String(id));
          if (alreadyRead) { skipped++; continue; }
          const added = await redis.sadd(INPUT_SEEN, String(id));
          if (added === 1) {
            await redis.rpush(INPUT_LIST, serialized);
            accepted++;
          } else skipped++;
        }
      }

      const totalInput = Number(await redis.llen(INPUT_LIST) || 0);

      // optional: auto-flush if input queue too big (runs flushAll in same invocation)
      if (BATCH_PUSH_THRESHOLD > 0 && totalInput >= BATCH_PUSH_THRESHOLD) {
        const token = await acquireLock();
        if (token) {
          try { await flushAll(); } catch (e) { /* ignore */ } finally { await releaseLock(); }
        }
      }

      return res.status(201).json({ accepted, skipped, total_in_input: totalInput });
    }

    res.setHeader("Allow", "GET, POST, OPTIONS");
    return res.status(405).json({ error: "Method not allowed" });

  } catch (err) {
    console.error("API /collector unhandled", err);
    return res.status(500).json({ error: err?.message ?? "internal" });
  }
}
