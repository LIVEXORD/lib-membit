const GIST_ID = process.env.GIST_ID;
const GITHUB_TOKEN = process.env.GITHUB_TOKEN;
const GIST_API = `https://api.github.com/gists/${GIST_ID}`;
const CACHE_DURATION = 10 * 60 * 1000; // 10 minutes

// Global cache object
if (!globalThis._cacheData) {
  globalThis._cacheData = {
    data: [],
    lastUpdate: 0,
    gistSha: "",
  };
}

async function loadData() {
  const now = Date.now();
  if (
    now - globalThis._cacheData.lastUpdate < CACHE_DURATION &&
    globalThis._cacheData.data.length > 0
  ) {
    return {
      data: globalThis._cacheData.data,
      sha: globalThis._cacheData.gistSha,
    };
  }

  const gistRes = await fetch(GIST_API, {
    headers: { Authorization: `token ${GITHUB_TOKEN}` },
  });
  if (!gistRes.ok)
    throw new Error(`Failed to fetch gist metadata: ${gistRes.status}`);

  const gistJson = await gistRes.json();
  const file = gistJson.files["data.json"];
  if (!file) throw new Error("data.json not found in gist");
  if (!file.raw_url) throw new Error("raw_url not available for data.json");

  const rawRes = await fetch(file.raw_url);
  if (!rawRes.ok)
    throw new Error(`Failed to fetch raw content: ${rawRes.status}`);

  const json = await rawRes.json();

  let dataArray;
  if (Array.isArray(json)) {
    dataArray = json;
  } else if (Array.isArray(json.record)) {
    dataArray = json.record;
  } else {
    dataArray = [];
  }

  globalThis._cacheData = {
    data: dataArray,
    gistSha: file.sha,
    lastUpdate: now,
  };

  return { data: dataArray, sha: file.sha };
}

async function saveData(newArray, sha) {
  const payload = {
    description: "Update data.json via Vercel",
    files: {
      "data.json": {
        content: JSON.stringify({ record: newArray }, null, 2),
        sha,
      },
    },
  };

  const res = await fetch(GIST_API, {
    method: "PATCH",
    headers: {
      Authorization: `token ${GITHUB_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Failed to update gist: ${res.status} - ${err}`);
  }

  globalThis._cacheData.data = newArray;
  globalThis._cacheData.lastUpdate = Date.now();
  globalThis._cacheData.gistSha = sha;
}

export default async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();

  try {

    if (req.method === "GET") {
      const { data } = await loadData();

      if (Date.now() - globalThis._cacheData.lastUpdate > CACHE_DURATION) {
        await saveData(
          globalThis._cacheData.data,
          globalThis._cacheData.gistSha
        );
      }

      return res.status(200).json({ record: data });
    }

    if (req.method === "POST") {
      const pets = req.body;
      if (!Array.isArray(pets) || pets.length === 0) {
        return res
          .status(400)
          .json({ error: "Request body must be a non-empty array." });
      }

      const { data: allData, sha } = await loadData();
      const added = [];
      const skipped = [];

      for (const pet of pets) {
        const dupId = allData.some((x) => x.pet_id === pet.pet_id);
        const dupDna = allData.some(
          (x) =>
            x.dna?.dna1id === pet.dna?.dna1id &&
            x.dna?.dna2id === pet.dna?.dna2id
        );
        if (dupId || dupDna) skipped.push(pet);
        else added.push(pet);
      }

      if (added.length === 0) {
        return res.status(200).json({ message: "No new items.", skipped });
      }

      const newData = allData.concat(added);

      // Only push if more than CACHE_DURATION since last update
      if (Date.now() - globalThis._cacheData.lastUpdate > CACHE_DURATION) {
        await saveData(newData, sha);
        return res.status(201).json({
          message: skipped.length ? "Some skipped (dupes)" : "All added",
          added,
          skipped,
        });
      } else {
        // Store in memory cache until next push
        globalThis._cacheData.data = newData;
        return res.status(201).json({
          message: "Cached temporarily. Will push soon.",
          added,
          skipped,
        });
      }
    }

    // Handle PATCH request
    if (req.method === "PATCH") {
      const { data } = req.body;

      if (!Array.isArray(data) || data.length === 0) {
        return res
          .status(400)
          .json({ error: "Request body must be a non-empty array of data." });
      }

      const { sha } = globalThis._cacheData;
      await saveData(data, sha);

      return res.status(200).json({
        message: "Data updated successfully.",
        updatedData: data,
      });
    }

    res.setHeader("Allow", "GET, POST, PATCH, OPTIONS");
    return res.status(405).json({ error: "Method not supported." });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: err.message });
  }
}
