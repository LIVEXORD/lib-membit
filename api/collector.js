// api/collector.js
// Minimal Collector: Upstash Redis only + daily reset (safe flush)
// - POST: accept array or { posts: [...] } -> push to input:list (dedupe in input:seen)
// - GET: read paginated from read:list, support ?batches=1,2,3 & ?batch_size=200 OR cursor/limit
// - ?flush=1 : flush staged input -> dedupe (against READ_LIST) -> append to read

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

// --- Auto-reset Redis per day
// NOTE: to preserve user's read archive, we only clear INPUT_LIST & INPUT_SEEN on day change.
// This keeps READ_LIST / READ_SEEN intact so users can still read past items.
async function checkAndResetDaily() {
  const today = utcYMD();
  const lastDate = await redis.get(LAST_DATE_KEY);
  if (lastDate !== today) {
    // clear input queue + input seen only
    await redis.del(INPUT_LIST, INPUT_SEEN);
    await redis.set(LAST_DATE_KEY, today);
    console.log("Redis INPUT reset for new day", today);
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
          // SAFE FLUSH FLOW (read all input, then process, then clear input)
          // 1) read all input items (batching via CHUNK_SIZE to avoid memory spike if necessary)
          const rawInput = await redis.lrange(INPUT_LIST, 0, -1); // strings
          if (!rawInput || rawInput.length === 0) {
            return res.status(200).json({ message: "Flush complete", pushed: 0 });
          }

          // 2) preload read-list ids into a Set for fast lookup
          const rawRead = await redis.lrange(READ_LIST, 0, -1);
          const readIdSet = new Set();
          for (const s of rawRead) {
            try {
              const obj = JSON.parse(s);
              const id = itemIdOf(obj);
              if (id) readIdSet.add(String(id));
            } catch {}
          }

          // 3) process input array in chunks (to avoid huge synchronous loops)
          let toPushAll = [];
          for (let i = 0; i < rawInput.length; i += CHUNK_SIZE) {
            const batch = rawInput.slice(i, i + CHUNK_SIZE);
            const newOnes = [];

            for (const s of batch) {
              try {
                const it = JSON.parse(s);
                const id = itemIdOf(it);

                if (!id) {
                  // no stable id → accept and generate one
                  it._generated_id = `_noid_${Math.random().toString(36).slice(2, 9)}`;
                  newOnes.push(it);
                  continue;
                }

                // If id already exists in READ_LIST (tracked in readIdSet), skip
                if (readIdSet.has(String(id))) {
                  // already in read_list -> skip
                  continue;
                }

                // Not present in read_list -> safe to push
                newOnes.push(it);
                // mark locally so following items in same flush know it's now present
                readIdSet.add(String(id));
              } catch {
                // ignore malformed entries
              }
            }

            if (newOnes.length) {
              // push the batch into READ_LIST and update READ_SEEN
              const jsons = newOnes.map((x) => JSON.stringify(x));
              await redis.rpush(READ_LIST, ...jsons);
              // add ids to READ_SEEN where possible
              for (const it of newOnes) {
                const id = itemIdOf(it);
                if (id) await redis.sadd(READ_SEEN, String(id));
              }
              toPushAll.push(...newOnes);
            }
          }

          // 4) after processing everything, clear input queue (we already consumed rawInput)
          await redis.del(INPUT_LIST);

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
        const parts = batchesParam
          .split(",")
          .map((s) => parseInt(s.trim(), 10))
          .filter((n) => Number.isFinite(n) && n > 0);
        const result = [];
        for (const idx of parts) {
          const start = (idx - 1) * batchSize;
          const slice = await readSlice(start, batchSize);
          result.push({ idx, count: slice.length, posts: slice });
        }
        return res.status(200).json({ batches: result, total });
      }

      const cursor = Math.max(parseInt(url.searchParams.get("cursor") || "0", 10) || 0, 0);
      const limit = Math.min(
        Math.max(parseInt(url.searchParams.get("limit") || String(batchSize), 10) || batchSize, 1),
        1000
      );
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
          // dedupe at input-queue level so same id not enqueued repeatedly
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
