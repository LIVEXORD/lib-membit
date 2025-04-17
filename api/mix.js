import fs from 'fs';
import path from 'path';

// Fungsi untuk load accounts.json dan mengembalikan array accounts
function loadAccounts() {
  const filePath = path.join(process.cwd(), 'accounts.json');
  const raw = fs.readFileSync(filePath, 'utf-8');
  const parsed = JSON.parse(raw);
  return parsed.accounts;    // akses ke properti "accounts"
}

export default async function handler(req, res) {
  // CORS headers (opsional, sesuaikan kebutuhan)
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  const accounts = loadAccounts();

  if (req.method === 'POST') {
    const pets = req.body;
    if (!Array.isArray(pets) || pets.length === 0) {
      return res.status(400).json({ error: 'Request body must be an array.' });
    }

    // Step 1: Ambil dan gabungkan semua data lama
    let allData = [];
    for (const acct of accounts) {
      const url = `https://api.jsonbin.io/v3/b/${acct.binId}/latest`;
      const r = await fetch(url, {
        method: 'GET',
        headers: { 'X-Master-Key': acct.apiKey }
      });
      if (!r.ok) {
        return res.status(500).json({ error: `Failed to fetch from ${acct.name}` });
      }
      const { record } = await r.json();
      // record bisa nested di record.record atau langsung record
      const list = Array.isArray(record?.record)
        ? record.record
        : (Array.isArray(record) ? record : []);
      allData = allData.concat(list);
    }

    // Step 2: Filter duplicates, kumpulkan yang baru
    const added = [];
    const skipped = [];
    for (const pet of pets) {
      const dupId  = allData.some(x => x.pet_id === pet.pet_id);
      const dupDna = allData.some(x =>
        x.dna?.dna1id === pet.dna.dna1id &&
        x.dna?.dna2id === pet.dna.dna2id
      );
      (dupId || dupDna) ? skipped.push(pet) : added.push(pet);
    }
    if (added.length === 0) {
      return res.status(409).json({ message: 'No new items.', skipped });
    }

    const newData = allData.concat(added);

    // Step 3: Push update ke semua akun
    for (const acct of accounts) {
      const url = `https://api.jsonbin.io/v3/b/${acct.binId}`;
      const u = await fetch(url, {
        method: 'PUT',
        headers: {
          'X-Master-Key': acct.apiKey,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ record: newData })
      });
      if (!u.ok) {
        const txt = await u.text();
        return res.status(500).json({
          error: `Failed to update ${acct.name}`,
          details: txt
        });
      }
    }

    return res.status(201).json({
      message: skipped.length ? 'Some skipped (dupes)' : 'All added',
      added,
      skipped
    });
  }
  else if (req.method === 'GET') {
    // Gabungkan semua data dari setiap akun
    let allData = [];
    for (const acct of accounts) {
      const url = `https://api.jsonbin.io/v3/b/${acct.binId}/latest`;
      const r = await fetch(url, {
        method: 'GET',
        headers: { 'X-Master-Key': acct.apiKey }
      });
      if (!r.ok) {
        return res.status(500).json({ error: `Failed to fetch from ${acct.name}` });
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
