// api/collector.js
import { Redis } from "@upstash/redis";

const redis = Redis.fromEnv();

const INPUT_LIST = "membit:input:list";
const INPUT_SEEN = "membit:input:seen";
const READ_LIST = "membit:read:list";
const READ_SEEN = "membit:read:seen";
const LAST_DATE_KEY = "membit:last_date";

function utcYMD(d = new Date()) {
  return d.toISOString().slice(0, 10);
}
function itemId(it) {
  return it?.rest_id ?? it?.id ?? null;
}

// reset harian → hapus semua
async function checkAndResetDaily() {
  const today = utcYMD();
  const lastDate = await redis.get(LAST_DATE_KEY);
  if (lastDate !== today) {
    await redis.del(INPUT_LIST, INPUT_SEEN, READ_LIST, READ_SEEN);
    await redis.set(LAST_DATE_KEY, today);
    console.log("Redis reset for new day", today);
  }
}

export default async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  try {
    await checkAndResetDaily();

    // POST → enqueue
    if (req.method === "POST") {
      let items = [];
      if (Array.isArray(req.body)) items = req.body;
      else if (req.body && Array.isArray(req.body.posts)) items = req.body.posts;
      else return res.status(400).json({ error: "invalid body" });

      let accepted = 0, skipped = 0;
      for (const it of items) {
        const id = itemId(it);
        if (!id) continue;

        // skip kalau udah ada di read_seen
        const inRead = await redis.sismember(READ_SEEN, id);
        if (inRead) { skipped++; continue; }

        // skip kalau udah ada di input_seen
        const added = await redis.sadd(INPUT_SEEN, id);
        if (added === 0) { skipped++; continue; }

        await redis.rpush(INPUT_LIST, JSON.stringify(it));
        accepted++;
      }

      return res.status(201).json({ accepted, skipped });
    }

    // GET ?flush=1 → pindahin ke read
    if (req.method === "GET") {
      const url = new URL(req.url, `https://${req.headers.host}`);
      const flush = url.searchParams.get("flush");

      if (flush) {
        const raw = await redis.lrange(INPUT_LIST, 0, -1);
        let pushed = 0;

        for (const s of raw) {
          try {
            const it = JSON.parse(s);
            const id = itemId(it);
            if (!id) continue;

            const added = await redis.sadd(READ_SEEN, id);
            if (added === 1) {
              await redis.rpush(READ_LIST, JSON.stringify(it));
              pushed++;
            }
          } catch {}
        }

        await redis.del(INPUT_LIST, INPUT_SEEN);
        return res.status(200).json({ flushed: pushed });
      }

      // GET biasa → baca read:list
      const cursor = parseInt(url.searchParams.get("cursor") || "0", 10);
      const limit = Math.min(parseInt(url.searchParams.get("limit") || "200", 10), 500);

      const total = await redis.llen(READ_LIST);
      const slice = await redis.lrange(READ_LIST, cursor, cursor + limit - 1);
      const posts = slice.map(s => { try { return JSON.parse(s); } catch { return null; } }).filter(Boolean);

      return res.status(200).json({ record: posts, cursor: cursor + posts.length, total });
    }

    res.setHeader("Allow", "GET, POST, OPTIONS");
    return res.status(405).json({ error: "method not allowed" });
  } catch (err) {
    console.error("err", err);
    return res.status(500).json({ error: err.message || "internal" });
  }
}