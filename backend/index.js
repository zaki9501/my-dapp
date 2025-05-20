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
      const { transactionHash, blockNumber, address: marketAddress } = event.log;
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

      // Fetch predictionId and resolved outcome from the contract
      let predictionId = null;
      let resolvedOutcome = null;
      try {
        predictionId = await market.predictionId();
      } catch (e) {
        console.warn('Could not fetch predictionId for', marketAddress, e);
      }
      try {
        // resolved outcome: 0 or 1 if resolved, null/undefined if not
        resolvedOutcome = await market.winningOutcome();
      } catch (e) {
        // Not resolved yet
        resolvedOutcome = null;
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
        userFid,
        predictionId,
        resolvedOutcome
      });

      await db.query(
        `INSERT INTO trades (tx_hash, block_number, user_address, market_address, outcome, amount, shares, creator_fee, platform_fee, timestamp, user_fid, prediction_id, resolved_outcome)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
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
          userFid,
          predictionId,
          resolvedOutcome
        ]
      );
      console.log('Trade indexed:', transactionHash, 'on market', marketAddress, 'userFid:', userFid, 'predictionId:', predictionId, 'resolvedOutcome:', resolvedOutcome);
      await indexMarketMetadata(marketAddress);
    } catch (err) {
      console.error('Error handling Trade event:', err);
    }
  });
  market.on('MarketResolved', async () => {
    await indexMarketMetadata(marketAddress);
  });
  console.log('Listening for trades and resolution on market:', marketAddress);
}

// --- Helper: Listen to Market events and update markets table ---
async function indexMarketMetadata(marketAddress) {
  const market = new ethers.Contract(marketAddress, marketAbi, provider);
  try {
    const [
      predictionId,
      question,
      description,
      category,
      rule,
      status,
      resolutionDate,
      resolved,
      winningOutcome,
      yesPool,
      noPool,
      volume,
      tradesCount
    ] = await Promise.all([
      market.predictionId(),
      market.question(),
      market.description(),
      market.category(),
      market.rule(),
      market.status(),
      market.resolutionDate(),
      market.resolved(),
      market.winningOutcome().catch(() => null),
      market.yesPool(),
      market.noPool(),
      market.volume(),
      market.tradesCount()
    ]);
    await db.query(
      `INSERT INTO markets (
        market_address, prediction_id, question, description, category, rule, status, resolution_date, resolved, outcome, yes_pool, no_pool, volume, trades_count
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,TO_TIMESTAMP($8),$9,$10,$11,$12,$13,$14)
      ON CONFLICT (market_address) DO UPDATE SET
        prediction_id = EXCLUDED.prediction_id,
        question = EXCLUDED.question,
        description = EXCLUDED.description,
        category = EXCLUDED.category,
        rule = EXCLUDED.rule,
        status = EXCLUDED.status,
        resolution_date = EXCLUDED.resolution_date,
        resolved = EXCLUDED.resolved,
        outcome = EXCLUDED.outcome,
        yes_pool = EXCLUDED.yes_pool,
        no_pool = EXCLUDED.no_pool,
        volume = EXCLUDED.volume,
        trades_count = EXCLUDED.trades_count`,
      [
        marketAddress,
        predictionId,
        question,
        description,
        category,
        rule,
        status,
        Number(resolutionDate),
        resolved,
        winningOutcome,
        ethers.formatEther(yesPool),
        ethers.formatEther(noPool),
        ethers.formatEther(volume),
        Number(tradesCount)
      ]
    );
    console.log('Indexed/updated market metadata for', marketAddress);
  } catch (err) {
    console.error('Error indexing market metadata:', err);
  }
}

// --- Listen to MarketCreated and update markets table ---
factory.on('MarketCreated', async (marketAddress, creator, predictionId, event) => {
  console.log('New market created at:', marketAddress);
  await indexMarketMetadata(marketAddress);
  listenToMarket(marketAddress);
});

// --- Listen to all existing markets on startup ---
async function listenToExistingMarkets() {
  try {
    const marketAddresses = await factory.getMarkets();
    for (const marketAddress of marketAddresses) {
      await indexMarketMetadata(marketAddress);
      listenToMarket(marketAddress);
    }
  } catch (err) {
    console.error('Error fetching existing markets:', err);
  }
}
listenToExistingMarkets();

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

// --- API Endpoint: Get live markets from DB ---
app.get('/api/live-markets', async (req, res) => {
  try {
    const { rows } = await db.query(
      "SELECT * FROM markets WHERE status = 'live' AND resolved = false ORDER BY resolution_date ASC LIMIT 100"
    );
    res.json(rows);
  } catch (err) {
    console.error('API error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

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
      prediction_id: row.prediction_id,
      resolved_outcome: row.resolved_outcome
    }));
    res.json(formatted);
  } catch (err) {
    console.error('API error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// --- API Endpoint: Get resolved markets from DB ---
app.get('/api/resolved-markets', async (req, res) => {
  try {
    const { rows } = await db.query(
      "SELECT * FROM markets WHERE resolved = true ORDER BY resolution_date DESC LIMIT 100"
    );
    // Map and calculate yes_price and no_price
    const formatted = rows.map(m => {
      const yesPool = Number(m.yes_pool);
      const noPool = Number(m.no_pool);
      const totalPool = yesPool + noPool;
      return {
        prediction_id: m.prediction_id,
        market_address: m.market_address,
        question: m.question,
        description: m.description,
        category: m.category,
        rule: m.rule,
        resolution_date: m.resolution_date,
        yes_pool: yesPool,
        no_pool: noPool,
        volume: Number(m.volume),
        trades_count: Number(m.trades_count),
        resolved: m.resolved,
        outcome: m.outcome,
        yes_price: totalPool > 0 ? yesPool / totalPool : 0.5,
        no_price: totalPool > 0 ? noPool / totalPool : 0.5,
      };
    });
    res.json(formatted);
  } catch (err) {
    console.error('API error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Respond to HEAD requests at root
app.head('/', (req, res) => {
  res.status(200).end();
});

// (Optional) Also respond to GET requests at root
app.get('/', (req, res) => {
  res.status(200).send('OK');
});

app.get('/health', (req, res) => {
  res.status(200).send('OK');
});

app.listen(process.env.PORT || 3001, () => {
  console.log('API running on port', process.env.PORT || 3001);
}); 
