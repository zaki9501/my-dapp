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
    try {
      // ethers v6: event.log.address, event.log.transactionHash, event.log.blockNumber
      const { transactionHash, blockNumber, address: marketAddress } = event.log;
      // Get block timestamp
      const block = await provider.getBlock(blockNumber);
      const timestamp = new Date(block.timestamp * 1000);

      // Fetch user's Farcaster FID from the contract
      let userFid = null;
      try {
        userFid = await market.userFid(user);
        if (userFid) userFid = userFid.toString();
      } catch (e) {
        console.warn('Could not fetch userFid for', user, e);
      }

      // Debug log
      console.log('Trade event:', {
        transactionHash,
        blockNumber,
        user,
        marketAddress,
        outcome,
        amount,
        shares,
        creatorFee,
        platformFee,
        timestamp,
        userFid
      });

      await db.query(
        `INSERT INTO trades (tx_hash, block_number, user_address, market_address, outcome, amount, shares, creator_fee, platform_fee, timestamp, user_fid)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
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
          timestamp,
          userFid
        ]
      );
      console.log('Trade indexed:', transactionHash, 'on market', marketAddress, 'userFid:', userFid);
    } catch (err) {
      console.error('Error handling Trade event:', err);
    }
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
  try {
    const marketAddresses = await factory.getMarkets();
    for (const marketAddress of marketAddresses) {
      listenToMarket(marketAddress);
    }
  } catch (err) {
    console.error('Error fetching existing markets:', err);
  }
}
listenToExistingMarkets();

// --- API Endpoint ---
app.get('/api/user-trades/:address', async (req, res) => {
  const { address } = req.params;
  try {
    const { rows } = await db.query(
      'SELECT * FROM trades WHERE user_address = $1 ORDER BY timestamp DESC LIMIT 100',
      [address.toLowerCase()]
    );
    // Format values for display (MON)
    const formatted = rows.map(row => ({
      ...row,
      amount_mon: ethers.formatEther(row.amount),
      shares_mon: ethers.formatEther(row.shares),
      creator_fee_mon: ethers.formatEther(row.creator_fee),
      platform_fee_mon: ethers.formatEther(row.platform_fee),
    }));
    res.json(formatted);
  } catch (err) {
    console.error('API error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// --- API Endpoint: Get all trades for a market ---
app.get('/api/market-trades/:marketAddress', async (req, res) => {
  const { marketAddress } = req.params;
  try {
    const { rows } = await db.query(
      'SELECT * FROM trades WHERE market_address = $1 ORDER BY timestamp ASC',
      [marketAddress.toLowerCase()]
    );
    // Format values for display (MON)
    const formatted = rows.map(row => ({
      ...row,
      amount_mon: ethers.formatEther(row.amount),
      shares_mon: ethers.formatEther(row.shares),
      creator_fee_mon: ethers.formatEther(row.creator_fee),
      platform_fee_mon: ethers.formatEther(row.platform_fee),
    }));
    res.json(formatted);
  } catch (err) {
    console.error('API error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.listen(process.env.PORT || 3001, () => {
  console.log('API running on port', process.env.PORT || 3001);
}); 
