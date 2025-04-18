const GIST_ID = process.env.GIST_ID;
const GITHUB_TOKEN = process.env.GITHUB_TOKEN;
const DEV_PUSH_TOKEN = process.env.DEV_PUSH_TOKEN;
const GIST_API = `https://api.github.com/gists/${GIST_ID}`;
const CACHE_DURATION = 10 * 60 * 1000; // 10 minutes

// Initialize global cache if undefined
if (!globalThis._cacheData) {
  globalThis._cacheData = { data: [], lastUpdate: 0, gistSha: "" };
}

async function loadData() {
  const now = Date.now();
  // Return cached if still valid
  if (now - globalThis._cacheData.lastUpdate < CACHE_DURATION && Array.isArray(globalThis._cacheData.data) && globalThis._cacheData.data.length) {
    return { data: globalThis._cacheData.data, sha: globalThis._cacheData.gistSha };
  }
  // Fetch from Gist
  const gistRes = await fetch(GIST_API, { headers: { Authorization: `token ${GITHUB_TOKEN}` } });
  if (!gistRes.ok) throw new Error(`Failed to fetch gist: ${gistRes.status}`);
  const gistJson = await gistRes.json();
  const file = gistJson.files["data.json"];
  if (!file || !file.raw_url) throw new Error("data.json not found or raw_url missing");
  const rawRes = await fetch(file.raw_url);
  if (!rawRes.ok) throw new Error(`Failed to fetch raw content: ${rawRes.status}`);
  const json = await rawRes.json();
  const dataArray = Array.isArray(json) ? json : (Array.isArray(json.record) ? json.record : []);
  // Update cache
  globalThis._cacheData = { data: dataArray, gistSha: file.sha, lastUpdate: now };
  return { data: dataArray, sha: file.sha };
}

async function saveData(newArray, sha) {
  const payload = {
    description: "Update data.json via Vercel",
    files: { "data.json": { content: JSON.stringify({ record: newArray }, null, 2), sha } }
  };
  const res = await fetch(GIST_API, {
    method: "PATCH",
    headers: {
      Authorization: `token ${GITHUB_TOKEN}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify(payload)
  });
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Failed to update gist: ${res.status} - ${err}`);
  }
  // Update cache after successful push
  globalThis._cacheData.data = newArray;
  globalThis._cacheData.gistSha = sha;
  globalThis._cacheData.lastUpdate = Date.now();
}

export default async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  try {
    // 1) Force-push cached data via PUT with token auth
    if (req.method === "PUT") {
      const { token } = req.body;
      if (!token || token !== DEV_PUSH_TOKEN) {
        return res.status(403).json({ error: "Unauthorized" });
      }
      const { data, gistSha } = globalThis._cacheData;
      if (!Array.isArray(data) || data.length === 0 || !gistSha) {
        return res.status(400).json({ error: "No cached data to push" });
      }
      await saveData(data, gistSha);
      return res.status(200).json({ message: "Force-pushed cache to Gist" });
    }

    // 2) Get data
    if (req.method === "GET") {
      const { data } = await loadData();
      // Auto-push if cache expired
      if (Date.now() - globalThis._cacheData.lastUpdate > CACHE_DURATION) {
        await saveData(globalThis._cacheData.data, globalThis._cacheData.gistSha);
      }
      return res.status(200).json({ record: data });
    }

    // 3) Add new items via POST
    if (req.method === "POST") {
      const items = req.body;
      if (!Array.isArray(items) || items.length === 0) {
        return res.status(400).json({ error: "Request body must be a non-empty array" });
      }
      const { data: existing, sha } = await loadData();
      const added = [];
      const skipped = [];
      for (const it of items) {
        const isDup = existing.some(x => x.pet_id === it.pet_id || (x.dna?.dna1id === it.dna?.dna1id && x.dna?.dna2id === it.dna?.dna2id));
        if (isDup) skipped.push(it);
        else added.push(it);
      }
      if (added.length === 0) {
        return res.status(200).json({ message: "No new items", skipped });
      }
      const merged = existing.concat(added);
      // Push immediately if cache expired
      if (Date.now() - globalThis._cacheData.lastUpdate > CACHE_DURATION) {
        await saveData(merged, sha);
        return res.status(201).json({ message: skipped.length ? "Partial added" : "All added", added, skipped });
      }
      // Else cache until next PUT
      globalThis._cacheData.data = merged;
      return res.status(202).json({ message: "Cached until next push", added, skipped });
    }

    res.setHeader("Allow", "GET, POST, PUT, OPTIONS");
    return res.status(405).json({ error: "Method not supported" });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: err.message });
  }
}
