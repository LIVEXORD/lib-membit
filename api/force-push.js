// pages/api/force-push.js

const GIST_ID = process.env.GIST_ID;
const GITHUB_TOKEN = process.env.GITHUB_TOKEN;
const GIST_API = `https://api.github.com/gists/${GIST_ID}`;

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

  // Inisialisasi global cache jika belum ada
  if (!globalThis._cacheData) {
    globalThis._cacheData = {
      data: [],
      lastUpdate: 0,
      gistSha: "",
    };
  }
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

  if (req.method === "OPTIONS") {
    return res.status(200).end();
  }

  if (req.method === "POST") {
    const { token } = req.body;

    // Simple security check biar gak sembarang orang bisa pakai
    if (token !== process.env.DEV_PUSH_TOKEN) {
      return res.status(403).json({ error: "Unauthorized" });
    }

    try {
      await saveData(globalThis._cacheData.data, globalThis._cacheData.gistSha);
      return res.status(200).json({ message: "Forced push success" });
    } catch (err) {
      return res.status(500).json({ error: err.message });
    }
  } else {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method Not Allowed" });
  }
}
