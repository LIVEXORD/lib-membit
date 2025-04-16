const apiKey = '$2a$10$R.tpF1zMrk4inHGeWvZ6VuQjMCAwhQIpPxim6I/kzi8xUh413cE6u';
const binsId = '67ffa1258a456b79668aa4f1';
const apiUrl = `https://api.jsonbin.io/v3/b/${binsId}`;

export default async function handler(req, res) {
  if (req.method === 'POST') {
    const pets = req.body; // Menerima array dari data pet

    if (!Array.isArray(pets) || pets.length === 0) {
      return res.status(400).json({ error: 'Request body must be an array of pet data.' });
    }

    // Validasi setiap objek pet dalam array
    for (const pet of pets) {
      const { pet_name, pet_id, pet_class, pet_star, dna } = pet;

      // Validasi field pet
      if (!pet_name || !pet_id || !pet_class || !pet_star || !dna || !dna.dna1id || !dna.dna2id) {
        return res.status(400).json({ error: 'Incomplete fields, ensure all fields are filled properly for every pet.' });
      }
    }

    // Cek data yang sudah ada di JSONBin
    const readResponse = await fetch(apiUrl, {
      method: 'GET',
      headers: {
        'X-Master-Key': apiKey,
        'Content-Type': 'application/json',
      },
    });

    if (!readResponse.ok) {
      return res.status(500).json({ error: 'Failed to fetch existing data.' });
    }

    const existingData = await readResponse.json();

    // Cek apakah kombinasi dna1id dan dna2id sudah ada untuk setiap pet
    const duplicates = existingData.record.some(
        item => item?.dna?.dna1id === pet.dna.dna1id && item?.dna?.dna2id === pet.dna.dna2id
    );      

    if (duplicates.length > 0) {
      return res.status(409).json({ error: 'Some pet combinations already exist.', duplicates });
    }

    // Jika tidak ada duplikasi, tambahkan data pet baru
    const newData = existingData.record.concat(pets);

    // Update data di JSONBin
    const updateResponse = await fetch(apiUrl, {
      method: 'PUT',
      headers: {
        'X-Master-Key': apiKey,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ record: newData }),
    });

    if (updateResponse.ok) {
      return res.status(201).json({ message: 'Data successfully added.', data: pets });
    } else {
      return res.status(500).json({ error: 'Failed to update data to JSONBin.' });
    }

  } else if (req.method === 'GET') {
    // Mengambil data yang ada
    const response = await fetch(apiUrl, {
      method: 'GET',
      headers: {
        'X-Master-Key': apiKey,
        'Content-Type': 'application/json',
      },
    });

    if (response.ok) {
      const data = await response.json();
      return res.status(200).json(data);
    } else {
      return res.status(500).json({ error: 'Failed to fetch data from JSONBin.' });
    }
  }

  return res.status(405).json({ error: 'Method not supported.' });
}
