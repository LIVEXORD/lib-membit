// api/collector.js
// Minimal Collector: Upstash Redis only + daily reset
// - POST: accept array or { posts: [...] } -> push to input:list (dedupe in input:seen)
// - GET: read paginated from read:list, support ?batches=1,2,3 & ?batch_size=200 OR cursor/limit
// - ?flush=1 : flush staged input -> dedupe -> append to read

import { Redis } from "@upstash/redis";

const redis = Redis.fromEnv(); // Upstash Redis
const FLUSH_SECRET = process.env.FLUSH_SECRET || null;

const CHUNK_SIZE = parseInt(process.env.CHUNK_SIZE || "100", 10);
const BATCH_PUSH_THRESHOLD = parseInt(process.env.BATCH_PUSH_THRESHOLD || "100", 10);
const READ_BATCH_SIZE = Math.min(Math.max(parseInt(process.env.BATCH_SIZE || "200", 10), 1), 500);
const RETRY_ATTEMPTS = Math.max(parseInt(process.env.RETRY_ATTEMPTS || "3", 10), 1);

const INPUT_LIST = "membit:input:list";
const INPUT_SEEN = "membit:input:seen";
const READ_LIST = "membit:read:list";
const READ_SEEN = "membit:read:seen";
const LOCK_FLUSH = "membit:lock:flush";
const LAST_DATE_KEY = "membit:last_date";

function utcYMD(d = new Date()) {
  return d.toISOString().slice(0, 10);
}

// --- Redis helpers
async function acquireLock(ttlMs = 60000) {
  const token = `t-${Math.random().toString(36).slice(2, 9)}`;
  try {
    const ok = await redis.setnx(LOCK_FLUSH, token);
    if (ok === 1) {
      await redis.pexpire(LOCK_FLUSH, ttlMs);
      return token;
    }
    return null;
  } catch {
    return null;
  }
}
async function releaseLock() {
  try {
    await redis.del(LOCK_FLUSH);
  } catch {}
}

async function popChunk(n = CHUNK_SIZE) {
  const out = [];
  for (let i = 0; i < n; i++) {
    const s = await redis.lpop(INPUT_LIST);
    if (!s) break;
    try {
      out.push(JSON.parse(s));
    } catch {}
  }
  return out;
}

async function readSlice(offset, count) {
  const end = offset + count - 1;
  const arr = await redis.lrange(READ_LIST, offset, end);
  return arr
    .map((s) => {
      try { return JSON.parse(s); } 
      catch { return null; }
    })
    .filter(Boolean);
}

function itemIdOf(it) {
  return it?.rest_id ?? it?.id ?? null;
}

function mergeUniqueByRestId(existingArr, incomingArr) {
  const map = new Map();
  for (const e of existingArr) {
    const id = itemIdOf(e);
    if (id) map.set(String(id), e);
    else map.set(JSON.stringify(e).slice(0, 50) + Math.random(), e);
  }
  for (const it of incomingArr) {
    const id = itemIdOf(it);
    if (!id) {
      it._generated_id = `_noid_${Math.random().toString(36).slice(2, 9)}`;
      map.set(`g_${it._generated_id}`, it);
      continue;
    }
    if (!map.has(String(id))) map.set(String(id), it);
  }
  return Array.from(map.values());
}

// --- Auto-reset Redis per day
async function checkAndResetDaily() {
  const today = utcYMD();
  const lastDate = await redis.get(LAST_DATE_KEY);
  if (lastDate !== today) {
    await redis.del(INPUT_LIST, INPUT_SEEN, READ_LIST, READ_SEEN);
    await redis.set(LAST_DATE_KEY, today);
    console.log("Redis reset for new day", today);
  }
}

// --- MAIN handler
export default async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, X-SECRET");
  if (req.method === "OPTIONS") return res.status(200).end();

  try {
    await checkAndResetDaily();

    // --- GET ---
    if (req.method === "GET") {
      const url = new URL(req.url, `https://${req.headers.host}`);
      const q = Object.fromEntries(url.searchParams.entries());
      const flush = "flush" in q && (q.flush === "1" || q.flush === "true" || q.flush === "");

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
          let toPushAll = [];
          while (true) {
            const chunk = await popChunk(CHUNK_SIZE);
            if (!chunk.length) break;

            const newOnes = [];
            for (const it of chunk) {
              const id = itemIdOf(it);
              if (!id) {
                it._generated_id = `_noid_${Math.random().toString(36).slice(2, 9)}`;
                newOnes.push(it);
              } else {
                const added = await redis.sadd(READ_SEEN, String(id));
                if (added === 1) newOnes.push(it);
              }
            }
            if (newOnes.length) {
              const jsons = newOnes.map((x) => JSON.stringify(x));
              await redis.rpush(READ_LIST, ...jsons);
              toPushAll.push(...newOnes);
            }
          }

          return res.status(200).json({ message: "Flush complete", pushed: toPushAll.length });
        } finally {
          await releaseLock();
        }
      }

      // Read path
      const batchesParam = url.searchParams.get("batches");
      const batchSize = Math.min(
        Math.max(parseInt(url.searchParams.get("batch_size") || String(READ_BATCH_SIZE), 10) || READ_BATCH_SIZE, 1),
        500
      );
      const total = await redis.llen(READ_LIST);

      if (batchesParam) {
        const parts = batchesParam.split(",").map((s) => parseInt(s.trim(), 10)).filter((n) => Number.isFinite(n) && n > 0);
        const result = [];
        for (const idx of parts) {
          const start = (idx - 1) * batchSize;
          const slice = await readSlice(start, batchSize);
          result.push({ idx, count: slice.length, posts: slice });
        }
        return res.status(200).json({ batches: result, total });
      }

      const cursor = Math.max(parseInt(url.searchParams.get("cursor") || "0", 10) || 0, 0);
      const limit = Math.min(Math.max(parseInt(url.searchParams.get("limit") || String(batchSize), 10) || batchSize, 1), 1000);
      const slice = await readSlice(cursor, limit);
      const nextCursor = cursor + slice.length;
      return res.status(200).json({ record: slice, cursor: nextCursor, total });
    }

    // --- POST ---
    if (req.method === "POST") {
      let items = [];
      if (Array.isArray(req.body)) items = req.body;
      else if (req.body && Array.isArray(req.body.posts)) items = req.body.posts;
      else return res.status(400).json({ error: "Body must be array or { posts: [...] }" });

      let accepted = 0, skipped = 0;
      for (const it of items) {
        const id = itemIdOf(it);
        if (!id) {
          await redis.rpush(INPUT_LIST, JSON.stringify(it));
          accepted++;
        } else {
          const added = await redis.sadd(INPUT_SEEN, String(id));
          if (added === 1) {
            await redis.rpush(INPUT_LIST, JSON.stringify(it));
            accepted++;
          } else skipped++;
        }
      }

      const totalInput = await redis.llen(INPUT_LIST);
      return res.status(201).json({ accepted, skipped, total_in_input: totalInput });
    }

    res.setHeader("Allow", "GET, POST, OPTIONS");
    return res.status(405).json({ error: "Method not allowed" });
  } catch (err) {
    console.error("Unhandled", err);
    return res.status(500).json({ error: err?.message ?? "internal" });
  }
}
