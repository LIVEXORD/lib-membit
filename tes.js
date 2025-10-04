import 'dotenv/config'; // ini yg load .env ke process.env
import { Redis } from "@upstash/redis";

const redis = Redis.fromEnv();
const READ_LIST = "membit:read:list";
const READ_SEEN = "membit:read:seen";

async function rebuildSeen() {
  const arr = await redis.lrange(READ_LIST, 0, -1);
  for (const s of arr) {
    try {
      const obj = JSON.parse(s);
      const id = obj?.rest_id ?? obj?.id;
      if (id) await redis.sadd(READ_SEEN, String(id));
    } catch {}
  }
  console.log("Rebuilt READ_SEEN from READ_LIST, count:", await redis.scard(READ_SEEN));
}

rebuildSeen().catch(console.error);
