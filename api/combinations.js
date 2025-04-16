// api/combinations.js

const fs = require('fs').promises;
const path = require('path');

// Lokasi file data kombinasi
const DATA_FILE = path.join(process.cwd(), 'data', 'combinations.json');

/**
 * Membaca kombinasi dari file JSON.
 * Jika file tidak ditemukan, mengembalikan array kosong.
 */
async function readCombinations() {
  try {
    const data = await fs.readFile(DATA_FILE, 'utf8');
    const parsed = JSON.parse(data);
    return parsed.combinations || [];
  } catch (error) {
    // Jika file belum ada, kembalikan array kosong.
    if (error.code === 'ENOENT') return [];
    throw error;
  }
}

/**
 * Menyimpan array kombinasi ke file JSON.
 */
async function saveCombinations(combinations) {
  const data = JSON.stringify({ combinations }, null, 2);
  // Pastikan direktori data sudah ada
  await fs.mkdir(path.dirname(DATA_FILE), { recursive: true });
  await fs.writeFile(DATA_FILE, data, 'utf8');
}

/**
 * Handler untuk API endpoint.
 * Mendukung GET untuk membaca data dan POST untuk menambahkan kombinasi baru.
 */
export default async function handler(req, res) {
  if (req.method === 'GET') {
    try {
      const combinations = await readCombinations();
      return res.status(200).json({ combinations });
    } catch (error) {
      return res.status(500).json({ error: 'Gagal membaca data kombinasi.' });
    }
  } else if (req.method === 'POST') {
    try {
      const { combination } = req.body;
      if (!combination) {
        return res.status(400).json({ error: 'Field "combination" harus diisi.' });
      }

      // Baca data kombinasi yang sudah ada.
      let combinations = await readCombinations();

      // Jika kombinasi sudah ada, kembalikan pesan bahwa data sudah ada.
      if (combinations.includes(combination)) {
        return res.status(200).json({
          message: 'Kombinasi sudah ada.',
          combination,
        });
      }

      // Tambahkan kombinasi baru dan simpan.
      combinations.push(combination);
      await saveCombinations(combinations);
      return res.status(201).json({
        message: 'Kombinasi berhasil ditambahkan.',
        combination,
      });
    } catch (error) {
      return res.status(500).json({ error: 'Gagal menambahkan kombinasi.' });
    }
  } else {
    res.setHeader('Allow', ['GET', 'POST']);
    return res.status(405).json({ error: `Method ${req.method} tidak diizinkan` });
  }
}
