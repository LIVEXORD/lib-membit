const apiKey = '$2a$10$R.tpF1zMrk4inHGeWvZ6VuQjMCAwhQIpPxim6I/kzi8xUh413cE6u';
const binsId = '67ffa1258a456b79668aa4f1';
const apiUrl = `https://api.jsonbin.io/v3/b/${binsId}`;

export default async function handler(req, res) {
  if (req.method === 'POST') {
    const pets = req.body; // Menerima array dari data pet

    if (!Array.isArray(pets) || pets.length === 0) {
      return res.status(400).json({ error: 'Request body must be an array of pet data.' });
    }

    for (const pet of pets) {
      const { pet_name, pet_id, pet_class, pet_star, dna } = pet;
      if (!pet_name || !pet_id || !pet_class || !pet_star || !dna || !dna.dna1id || !dna.dna2id) {
        return res.status(400).json({ error: 'Incomplete fields, ensure all fields are filled properly for every pet.' });
      }
    }

    // Ambil data dari JSONBin
    const readResponse = await fetch(apiUrl, {
      method: 'GET',
      headers: {
        'X-Master-Key': apiKey,
        'Content-Type': 'application/json'
      }
    });

    if (!readResponse.ok) {
      return res.status(500).json({ error: 'Failed to fetch existing data.' });
    }

    const existingData = await readResponse.json();

    // Tangani nested "record.record"
    const recordList = Array.isArray(existingData.record?.record)
      ? existingData.record.record
      : (Array.isArray(existingData.record) ? existingData.record : []);

    // Cek duplikat berdasarkan pet_id
    for (const newPet of pets) {
      if (recordList.some(item => item.pet_id === newPet.pet_id)) {
        return res.status(400).json({ message: `Pet dengan ID ${newPet.pet_id} sudah ada!` });
      }
    }

    // Cek duplikat kombinasi dna
    const duplicates = pets.filter(newPet =>
      recordList.some(
        item =>
          item?.dna?.dna1id === newPet.dna.dna1id &&
          item?.dna?.dna2id === newPet.dna.dna2id
      )
    );

    if (duplicates.length > 0) {
      return res.status(409).json({ error: 'Some pet combinations already exist.', duplicates });
    }

    // Gabungkan data baru
    const newData = recordList.concat(pets);

    const updateResponse = await fetch(apiUrl, {
      method: 'PUT',
      headers: {
        'X-Master-Key': apiKey,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ record: newData })
    });

    if (updateResponse.ok) {
      return res.status(201).json({ message: 'Data successfully added.', data: pets });
    } else {
      const errorText = await updateResponse.text();
      return res.status(500).json({ error: 'Failed to update data to JSONBin.', details: errorText });
    }

  } else if (req.method === 'GET') {
    const response = await fetch(apiUrl, {
      method: 'GET',
      headers: {
        'X-Master-Key': apiKey,
        'Content-Type': 'application/json'
      }
    });

    if (response.ok) {
      const data = await response.json();

      // Ambil langsung record terdalam untuk menghindari nested
      const recordList = Array.isArray(data.record?.record)
        ? data.record.record
        : (Array.isArray(data.record) ? data.record : []);

      return res.status(200).json({ record: recordList });
    } else {
      return res.status(500).json({ error: 'Failed to fetch data from JSONBin.' });
    }
  }

  return res.status(405).json({ error: 'Method not supported.' });
}
