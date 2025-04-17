import fs from 'fs';
import path from 'path';

// Load accounts.json dari root
function loadAccounts() {
  const filePath = path.join(process.cwd(), 'accounts.json');
  const raw = fs.readFileSync(filePath, 'utf-8');
  return JSON.parse(raw).accounts;
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
      const list = Array.isArray(record?.record)
        ? record.record
        : (Array.isArray(record) ? record : []);
      allData = allData.concat(list);
    }

    // Step 2: Filter duplicates dan simpan data yang belum ada
    const added = [];
    const skipped = [];
    for (const pet of pets) {
      const dupId  = allData.some(x => x.pet_id === pet.pet_id);
      const dupDna = allData.some(x =>
        x.dna?.dna1id === pet.dna.dna1id &&
        x.dna?.dna2id === pet.dna.dna2id
      );
      if (dupId || dupDna) {
        skipped.push(pet);  // Simpan yang duplikat
      } else {
        added.push(pet);  // Simpan yang baru
      }
    }

    // Jika tidak ada data baru, beri pesan
    if (added.length === 0) {
      return res.status(409).json({ message: 'No new items.', skipped });
    }

    // Step 3: Gabungkan data lama dan yang baru
    const newData = allData.concat(added);

    // Step 4: Update data ke setiap akun, pastikan tidak ada data lebih lama
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

      const responseText = await u.text();  // Dapatkan respons dalam bentuk teks
      console.log('Response from PUT request:', responseText);  // Tambahkan logging untuk mengecek respon

      if (!u.ok) {
        return res.status(500).json({
          error: `Failed to update ${acct.name}`,
          details: responseText
        });
      }
    }

    // Step 5: Kirim response
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
