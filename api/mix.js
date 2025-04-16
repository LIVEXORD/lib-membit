const apiKey = '$2a$10$R.tpF1zMrk4inHGeWvZ6VuQjMCAwhQIpPxim6I/kzi8xUh413cE6u';
const binsId = '67ffa1258a456b79668aa4f1';
const apiUrl = `https://api.jsonbin.io/v3/b/${binsId}`;

export default async function handler(req, res) {
  // Endpoint hello untuk memastikan API bekerja
  if (req.method === 'GET' && req.url === '/api/hello') {
    return res.status(200).json({ message: 'Hello, World!' });
  }

  if (req.method === 'POST') {
    const { dna1, dna2, result } = req.body;

    if (!dna1 || !dna2 || !result) {
      return res.status(400).json({ error: 'Field tidak lengkap' });
    }

    try {
      // Mengambil data dari JSONBins
      const response = await fetch(apiUrl, {
        method: 'GET',
        headers: {
          'X-Master-Key': apiKey,
          'Content-Type': 'application/json',
        },
      });

      if (!response.ok) {
        throw new Error(`Gagal mengambil data: ${response.statusText}`);
      }

      const data = await response.json();

      // Mengecek apakah kombinasi sudah ada
      const sudahAda = data.find(
        item => item.dna1 === dna1 && item.dna2 === dna2 && item.result === result
      );

      if (sudahAda) {
        return res.status(200).json({ message: 'Kombinasi sudah ada' });
      }

      // Menambah data baru ke dalam array
      data.push({ dna1, dna2, result });

      // Mengupdate data di JSONBins
      const updateResponse = await fetch(apiUrl, {
        method: 'PUT',
        headers: {
          'X-Master-Key': apiKey,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(data),
      });

      if (updateResponse.ok) {
        return res.status(201).json({ message: 'Kombinasi disimpan', data: { dna1, dna2, result } });
      } else {
        return res.status(500).json({ error: 'Gagal menyimpan data ke JSONBins' });
      }
    } catch (error) {
      console.error('Error:', error);
      return res.status(500).json({ error: error.message });
    }
  } else if (req.method === 'GET') {
    try {
      // Mengambil data dari JSONBins
      const response = await fetch(apiUrl, {
        method: 'GET',
        headers: {
          'X-Master-Key': apiKey,
          'Content-Type': 'application/json',
        },
      });

      if (!response.ok) {
        throw new Error(`Gagal mengambil data: ${response.statusText}`);
      }

      const data = await response.json();
      return res.status(200).json(data);
    } catch (error) {
      console.error('Error:', error);
      return res.status(500).json({ error: error.message });
    }
  }

  res.status(405).json({ error: 'Method tidak didukung' });
}
