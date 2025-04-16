import fs from 'fs';
import path from 'path';

const filePath = path.join(process.cwd(), 'data', 'mix.json');

export default async function handler(req, res) {
  if (req.method === 'POST') {
    const { dna1, dna2, result } = req.body;

    if (!dna1 || !dna2 || !result) {
      return res.status(400).json({ error: 'Field tidak lengkap' });
    }

    const rawData = fs.readFileSync(filePath);
    const data = JSON.parse(rawData);

    const sudahAda = data.find(
      item => item.dna1 === dna1 && item.dna2 === dna2 && item.result === result
    );

    if (sudahAda) {
      return res.status(200).json({ message: 'Kombinasi sudah ada' });
    }

    data.push({ dna1, dna2, result });
    fs.writeFileSync(filePath, JSON.stringify(data, null, 2));

    return res.status(201).json({ message: 'Kombinasi disimpan', data: { dna1, dna2, result } });

  } else if (req.method === 'GET') {
    const rawData = fs.readFileSync(filePath);
    const data = JSON.parse(rawData);
    return res.status(200).json(data);
  }

  res.status(405).json({ error: 'Method tidak didukung' });
}
