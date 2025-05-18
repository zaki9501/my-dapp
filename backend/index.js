import 'dotenv/config';
import { ethers } from 'ethers';
import { Pool } from 'pg';
import express from 'express';
import cors from 'cors';

const app = express();
app.use(cors());

const db = new Pool({ connectionString: process.env.DATABASE_URL });
const provider = new ethers.WebSocketProvider(process.env.RPC_URL);

const factoryAbi = JSON.parse(process.env.CONTRACT_FACTORY_ABI);
const marketAbi = JSON.parse(process.env.CONTRACT_MARKET_ABI);

const factory = new ethers.Contract(
  process.env.FACTORY_ADDRESS, // Set this in Railway to your MarketFactory address
  factoryAbi,
  provider
);

// --- Helper: Listen to Trade events on a Market contract ---
function listenToMarket(marketAddress) {
  const market = new ethers.Contract(marketAddress, marketAbi, provider);
  market.on('Trade', async (user, outcome, amount, shares, creatorFee, platformFee, event) => {
    const { transactionHash, blockNumber } = event;
    const timestamp = new Date(); // For demo; ideally fetch block timestamp

    await db.query(
      `INSERT INTO trades (tx_hash, block_number, user_address, market_address, outcome, amount, shares, creator_fee, platform_fee, timestamp)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
       ON CONFLICT (tx_hash) DO NOTHING`,
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
    console.log('Trade indexed:', transactionHash, 'on market', marketAddress);
  });
  console.log('Listening for trades on market:', marketAddress);
}

// --- 1. Listen for new markets created ---
factory.on('MarketCreated', (marketAddress, creator, predictionId, event) => {
  console.log('New market created at:', marketAddress);
  listenToMarket(marketAddress);
});

// --- 2. On startup, listen to all existing markets ---
async function listenToExistingMarkets() {
  const count = await factory.marketCount();
  for (let i = 0; i < count; i++) {
    const marketAddress = await factory.markets(i);
    listenToMarket(marketAddress);
  }
}
listenToExistingMarkets();

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
