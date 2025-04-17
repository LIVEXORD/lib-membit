import fs from 'fs';
import path from 'path';

// Load accounts.json dari root
function loadAccounts() {
  const filePath = path.join(process.cwd(), 'accounts.json');
  const raw = fs.readFileSync(filePath, 'utf-8');
  return JSON.parse(raw).accounts;
}

export default async function handler(req, res) {
  // CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const accounts = loadAccounts();

  if (req.method === 'POST') {
    const pets = req.body;
    // validasi
    if (!Array.isArray(pets) || pets.length === 0) {
      return res.status(400).json({ error: 'Body must be a non-empty array.' });
    }

    // 1) Ambil & gabungkan data lama dari semua akun
    let allData = [];
    for (const acct of accounts) {
      const url = `https://api.jsonbin.io/v3/b/${acct.binId}/latest`;
      const r = await fetch(url, {
        method: 'GET',
        headers: { 'X-Master-Key': acct.apiKey }
      });
      if (!r.ok) {
        return res.status(500).json({ error: `Fetch failed on ${acct.name}` });
      }
      const { record } = await r.json();
      const list = Array.isArray(record?.record)
        ? record.record
        : (Array.isArray(record) ? record : []);
      allData = allData.concat(list);
    }

    // 2) Filter duplikat & kumpulkan yang baru
    const added = [];
    const skipped = [];
    for (const pet of pets) {
      const dupId = allData.some(x => x.pet_id === pet.pet_id);
      const dupDna = allData.some(x =>
        x.dna?.dna1id === pet.dna.dna1id &&
        x.dna?.dna2id === pet.dna.dna2id
      );
      dupId || dupDna ? skipped.push(pet) : added.push(pet);
    }
    if (added.length === 0) {
      return res.status(409).json({ message: 'No new items.', skipped });
    }

    const newData = allData.concat(added);

    // 3) Push update ke semua akun, kumpulkan hasilnya
    const updateResults = [];
    for (const acct of accounts) {
      const url = `https://api.jsonbin.io/v3/b/${acct.binId}`;
      try {
        const u = await fetch(url, {
          method: 'PUT',
          headers: {
            'X-Master-Key': acct.apiKey,
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({ record: newData })
        });
        if (u.ok) {
          updateResults.push({ name: acct.name, status: 'success' });
        } else {
          const details = await u.text();
          updateResults.push({ name: acct.name, status: 'failed', details });
        }
      } catch (err) {
        updateResults.push({ name: acct.name, status: 'error', message: err.message });
      }
    }

    return res.status(201).json({
      message: skipped.length
        ? 'Some items skipped (dupes), see updateResults'
        : 'All new items added, see updateResults',
      added,
      skipped,
      updateResults
    });
  }
  else if (req.method === 'GET') {
    // Gabungkan data dari semua akun
    let allData = [];
    for (const acct of accounts) {
      const url = `https://api.jsonbin.io/v3/b/${acct.binId}/latest`;
      const r = await fetch(url, {
        method: 'GET',
        headers: { 'X-Master-Key': acct.apiKey }
      });
      if (!r.ok) {
        return res.status(500).json({ error: `Fetch failed on ${acct.name}` });
      }
      const { record } = await r.json();
      const list = Array.isArray(record?.record)
        ? record.record
        : (Array.isArray(record) ? record : []);
      allData = allData.concat(list);
    }
    return res.status(200).json({ record: allData });
  }
  else {
    return res.status(405).json({ error: 'Method not supported.' });
  }
}
