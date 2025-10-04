/*
membit-collector-server.js
Full Express server implementation of your Upstash-only collector.
Features:
- POST /collector : push items to INPUT_LIST with dedupe against READ_SEEN and INPUT_SEEN
- GET /collector  : read from READ_LIST (cursor/limit or batches)
- GET /collector?flush=1 : move items from INPUT_LIST -> READ_LIST with dedupe (requires X-SECRET when FLUSH_SECRET set)
- Daily auto-reset (based on server date UTC Y-M-D)
- Safe bootstrap: if READ_LIST exists but contains non-JSON placeholder, it will be cleared
- Safer flush using RPOPLPUSH -> PROCESSING_LIST pattern (atomic-ish) and requeue/cleanup
- Startup recovery for PROCESSING_LIST

Env variables:
- UPSTASH_REDIS_REST_URL, UPSTASH_REDIS_REST_TOKEN (used by @upstash/redis via Redis.fromEnv())
- FLUSH_SECRET (optional) - required header X-SECRET for flush
- CHUNK_SIZE (default 100) - how many attempts per flush loop
- BATCH_SIZE (default 200) - default read batch when batching
- PORT (default 3000)

Install:
  npm init -y
  npm i express @upstash/redis body-parser morgan
Run:
  node membit-collector-server.js

*/

import express from "express";
import bodyParser from "body-parser";
import morgan from "morgan";
import { Redis } from "@upstash/redis";

const redis = Redis.fromEnv();

const app = express();
app.use(morgan("tiny"));
app.use(bodyParser.json({ limit: "2mb" }));

// --- CONFIG
const FLUSH_SECRET = process.env.FLUSH_SECRET || null;
const CHUNK_SIZE = parseInt(process.env.CHUNK_SIZE || "100", 10);
const READ_BATCH_SIZE = Math.min(
  Math.max(parseInt(process.env.BATCH_SIZE || "200", 10), 1),
  500
);
const LAST_DATE_KEY = "membit:last_date";

// --- KEYS
const INPUT_LIST = "membit:input:list";
const INPUT_SEEN = "membit:input:seen";
const READ_LIST = "membit:read:list";
const READ_SEEN = "membit:read:seen";
const LOCK_FLUSH = "membit:lock:flush";
const PROCESSING_LIST = "membit:processing:list";

// --- helpers
function utcYMD(d = new Date()) {
  return d.toISOString().slice(0, 10);
}
function itemIdOf(it) {
  return it?.rest_id ?? it?.id ?? null;
}

async function acquireLock(ttlMs = 60000) {
  const token = `t-${Math.random().toString(36).slice(2, 9)}`;
  try {
    const ok = await redis.setnx(LOCK_FLUSH, token);
    if (ok === 1) {
      await redis.pexpire(LOCK_FLUSH, ttlMs);
      return token;
    }
    return null;
  } catch (e) {
    console.warn("acquireLock error", e?.message ?? e);
    return null;
  }
}
async function releaseLock() {
  try {
    await redis.del(LOCK_FLUSH);
  } catch (e) {
    console.warn("releaseLock", e?.message ?? e);
  }
}

// --- startup recovery: move any leftover processing items back to input (safe restart)
async function recoverProcessingList() {
  const procLen = Number((await redis.llen(PROCESSING_LIST)) || 0);
  if (procLen === 0) return 0;
  let moved = 0;
  while (true) {
    const itm = await redis.rpop(PROCESSING_LIST);
    if (!itm) break;
    await redis.lpush(INPUT_LIST, itm);
    moved++;
  }
  console.warn(
    `Recovered ${moved} items from processing list back to input list`
  );
  return moved;
}

// --- daily reset
async function checkAndResetDaily() {
  const today = utcYMD();
  try {
    const lastDate = await redis.get(LAST_DATE_KEY);
    if (lastDate !== today) {
      await redis.del(INPUT_LIST, INPUT_SEEN, READ_LIST, READ_SEEN);
      await redis.set(LAST_DATE_KEY, today);
      console.log("Redis reset for new day:", today);
    }
  } catch (e) {
    console.warn("checkAndResetDaily error", e?.message ?? e);
  }
}

// --- validate read list content (detect placeholder)
async function readListLooksValid() {
  const first = await redis.lindex(READ_LIST, 0);
  if (!first) return false;
  try {
    JSON.parse(first);
    return true;
  } catch {
    return false;
  }
}

// --- ensure bootstrap
async function ensureReadStructures() {
  const totalRead = Number((await redis.llen(READ_LIST)) || 0);

  // if read list has content but is not valid JSON, clear it (likely manual placeholder)
  if (totalRead > 0) {
    const valid = await readListLooksValid();
    if (!valid) {
      console.warn(
        "READ_LIST contains non-json placeholder — clearing read:list & read:seen"
      );
      await redis.del(READ_LIST, READ_SEEN);
    }
  }

  const totalAfter = Number((await redis.llen(READ_LIST)) || 0);
  if (totalAfter === 0) {
    const inputs = await redis.lrange(INPUT_LIST, 0, -1);
    if (!inputs || inputs.length === 0) return 0;

    let moved = 0;
    for (const s of inputs) {
      let obj = null;
      try {
        obj = JSON.parse(s);
      } catch {
        obj = { text: s };
      }

      try {
        const id = itemIdOf(obj);
        await redis.rpush(READ_LIST, JSON.stringify(obj));
        if (id) await redis.sadd(READ_SEEN, String(id));
        moved++;
      } catch (e) {
        console.error("Failed pushing to read structures:", e?.message ?? e);
      }
    }

    if (moved > 0) {
      await redis.del(INPUT_LIST, INPUT_SEEN);
    } else {
      console.warn(
        "ensureReadStructures: found input list but moved 0 items — leaving INPUT_LIST intact"
      );
    }

    return moved;
  }
  return -1;
}

// --- flush logic (safe with processing list)
async function flushAll() {
  // first try bootstrap path
  const boot = await ensureReadStructures();
  if (boot >= 0) {
    return { flushed: boot, bootstrap: true };
  }

  let flushed = 0;
  // Use RPOPLPUSH to move items to PROCESSING_LIST so we don't lose items on crash
  while (true) {
    // rpoplpush returns element or null
    const s = await redis.rpoplpush(INPUT_LIST, PROCESSING_LIST);
    if (!s) break; // no more

    let parsed = null;
    try {
      parsed = JSON.parse(s);
    } catch {}

    if (!parsed) {
      // can't parse: push to read as raw object and remove from processing
      const obj = { text: s };
      try {
        await redis.rpush(READ_LIST, JSON.stringify(obj));
        flushed++;
      } catch (e) {
        console.error("Failed pushing raw to READ_LIST", e?.message ?? e);
        // if failed, move back to input to avoid data loss
        await redis.lpush(INPUT_LIST, s);
      }
      // remove the item from processing (first occurrence)
      await redis.lrem(PROCESSING_LIST, 1, s);
      continue;
    }

    const id = itemIdOf(parsed);
    if (!id) {
      try {
        // no id -> push to read list with generated id
        parsed._generated_id = `_noid_${Math.random()
          .toString(36)
          .slice(2, 9)}`;
        await redis.rpush(READ_LIST, JSON.stringify(parsed));
        flushed++;
      } catch (e) {
        console.error(
          "Failed pushing noid parsed item to READ_LIST",
          e?.message ?? e
        );
        await redis.lpush(INPUT_LIST, s);
      }
      await redis.lrem(PROCESSING_LIST, 1, s);
      continue;
    }

    try {
      // dedupe against READ_SEEN set
      const added = await redis.sadd(READ_SEEN, String(id));
      if (added === 1) {
        await redis.rpush(READ_LIST, JSON.stringify(parsed));
        flushed++;
      } else {
        // duplicate, skip
      }
    } catch (e) {
      console.error("Error during dedupe/push", e?.message ?? e);
      // move back to input to prevent data loss
      await redis.lpush(INPUT_LIST, s);
    }

    // remove from processing list
    await redis.lrem(PROCESSING_LIST, 1, s);

    // throttle to avoid long loops in one go (optional)
    if (flushed && flushed % CHUNK_SIZE === 0) {
      // allow other operations
      await new Promise((r) => setTimeout(r, 10));
    }
  }

  return { flushed, bootstrap: false };
}

// --- read helper
async function readSlice(offset, count) {
  const end = offset + count - 1;
  const arr = await redis.lrange(READ_LIST, offset, end);
  // always return JSON objects: parse if possible, otherwise wrap as { text: ... }
  return arr
    .map((s) => {
      if (!s) return null;
      try {
        return JSON.parse(s);
      } catch {
        return { text: s };
      }
    })
    .filter(Boolean);
}

// --- API handlers (single route /collector)
app.options("/collector", (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, X-SECRET");
  res.status(200).end();
});

app.get("/collector", async (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, X-SECRET");

  try {
    await checkAndResetDaily();

    const q = req.query || {};
    // only accept `batch` param for GET. default to 1 if missing/invalid
    const batchParam = q.batch;
    const batchNum = Math.max(parseInt(batchParam || "1", 10) || 1, 1);

    const batchSize = Math.min(
      Math.max(
        parseInt(q.batch_size || String(READ_BATCH_SIZE), 10) ||
          READ_BATCH_SIZE,
        1
      ),
      500
    );
    const total = Number((await redis.llen(READ_LIST)) || 0);
    const batchCount = total > 0 ? Math.ceil(total / batchSize) : 0;
    const batch = Array.from({ length: batchCount }, (_, i) => i + 1);

    if (batchCount === 0) {
      return res
        .status(200)
        .json({ batches: [], total, batch_count: 0, batch: [] });
    }

    if (batchNum > batchCount) {
      return res
        .status(200)
        .json({
          batches: [{ idx: batchNum, count: 0, posts: [] }],
          total,
          batch_count: batchCount,
          batch,
        });
    }

    const start = (batchNum - 1) * batchSize;
    const slice = await readSlice(start, batchSize);
    const result = [{ idx: batchNum, count: slice.length, posts: slice }];

    return res
      .status(200)
      .json({ batches: result, total, batch_count: batchCount, batch });
  } catch (err) {
    console.error("GET /collector unhandled", err);
    return res.status(500).json({ error: err?.message ?? "internal" });
  }
});

// Dedicated flush endpoint (POST) to avoid extra GET params
app.post("/collector/flush", async (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, X-SECRET");
  try {
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
  } catch (err) {
    console.error("POST /collector/flush unhandled", err);
    return res.status(500).json({ error: err?.message ?? "internal" });
  }
});

app.post("/collector", async (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, X-SECRET");

  try {
    await checkAndResetDaily();

    let items = [];
    if (Array.isArray(req.body)) items = req.body;
    else if (req.body && Array.isArray(req.body.posts)) items = req.body.posts;
    else
      return res
        .status(400)
        .json({ error: "Body must be array or { posts: [...] }" });

    let accepted = 0,
      skipped = 0;
    for (const it of items) {
      const id = itemIdOf(it);
      const serialized = JSON.stringify(it);
      if (!id) {
        await redis.rpush(INPUT_LIST, serialized);
        accepted++;
      } else {
        const alreadyRead = await redis.sismember(READ_SEEN, String(id));
        if (alreadyRead) {
          skipped++;
          continue;
        }
        const added = await redis.sadd(INPUT_SEEN, String(id));
        if (added === 1) {
          await redis.rpush(INPUT_LIST, serialized);
          accepted++;
        } else skipped++;
      }
    }

    const totalInput = Number((await redis.llen(INPUT_LIST)) || 0);
    return res
      .status(201)
      .json({ accepted, skipped, total_in_input: totalInput });
  } catch (err) {
    console.error("POST /collector unhandled", err);
    return res.status(500).json({ error: err?.message ?? "internal" });
  }
});

// small health & metrics
app.get("/health", async (req, res) => {
  try {
    const inputLen = Number((await redis.llen(INPUT_LIST)) || 0);
    const readLen = Number((await redis.llen(READ_LIST)) || 0);
    const procLen = Number((await redis.llen(PROCESSING_LIST)) || 0);
    const inputSeen = Number((await redis.scard(INPUT_SEEN)) || 0);
    const readSeen = Number((await redis.scard(READ_SEEN)) || 0);
    res.json({ ok: true, inputLen, readLen, procLen, inputSeen, readSeen });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message ?? e });
  }
});

// startup tasks
const PORT = parseInt(process.env.PORT || "3000", 10);
(async () => {
  try {
    await recoverProcessingList();
    app.listen(PORT, () =>
      console.log(`membit-collector server running on port ${PORT}`)
    );
  } catch (e) {
    console.error("Failed to start server", e);
    process.exit(1);
  }
})();
