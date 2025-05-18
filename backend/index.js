import 'dotenv/config';
import { ethers } from 'ethers';
import { Pool } from 'pg';
import express from 'express';
import cors from 'cors';

const app = express();
app.use(cors());

const db = new Pool({ connectionString: process.env.DATABASE_URL });
const provider = new ethers.JsonRpcProvider(process.env.RPC_URL);
const contract = new ethers.Contract(
  process.env.CONTRACT_ADDRESS,
  JSON.parse(process.env.CONTRACT_MARKET_ABI),
  provider
);

// --- Event Listener ---
contract.on('Trade', async (user, outcome, amount, shares, creatorFee, platformFee, event) => {
  const { transactionHash, blockNumber } = event;
  const marketAddress = event.address;
  const timestamp = new Date(); // For demo; ideally fetch block timestamp

  await db.query(
    `INSERT INTO trades (tx_hash, block_number, user_address, market_address, outcome, amount, shares, creator_fee, platform_fee, timestamp)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
    [
      transactionHash,
      blockNumber,
      user,
      marketAddress,
      outcome,
      amount.toString(),
      shares.toString(),
      creatorFee.toString(),
      platformFee.toString(),
      timestamp
    ]
  );
  console.log('Trade indexed:', transactionHash);
});

// --- API Endpoint ---
app.get('/api/user-trades/:address', async (req, res) => {
  const { address } = req.params;
  const { rows } = await db.query(
    'SELECT * FROM trades WHERE user_address = $1 ORDER BY timestamp DESC LIMIT 100',
    [address.toLowerCase()]
  );
  res.json(rows);
});

app.listen(process.env.PORT || 3001, () => {
  console.log('API running on port', process.env.PORT || 3001);
});
