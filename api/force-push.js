// pages/api/force-push.js

export default async function handler(req, res) {
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
  