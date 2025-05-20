import 'dotenv/config';
import { ethers } from 'ethers';
import { Pool } from 'pg';
import express from 'express';
import cors from 'cors';

// Periodic outbound request to keep Railway service awake
setInterval(() => {
  fetch('https://api.coingecko.com/api/v3/ping').catch(() => {});
}, 5 * 60 * 1000); // every 5 minutes

const app = express();
app.use(cors());

const db = new Pool({ connectionString: process.env.DATABASE_URL });
const provider = new ethers.WebSocketProvider(process.env.RPC_URL);

const factoryAbi = JSON.parse(process.env.CONTRACT_FACTORY_ABI);
const marketAbi = JSON.parse(process.env.CONTRACT_MARKET_ABI);

const factory = new ethers.Contract(
  process.env.FACTORY_ADDRESS,
  factoryAbi,
  provider
);

// --- Multicall3 ABI ---
const multicall3Abi = [
  {
    "inputs": [
      {
        "components": [
          { "internalType": "address", "name": "target", "type": "address" },
          { "internalType": "bytes", "name": "callData", "type": "bytes" }
        ],
        "internalType": "struct Multicall3.Call[]",
        "name": "calls",
        "type": "tuple[]"
      }
    ],
    "name": "aggregate",
    "outputs": [
      { "internalType": "uint256", "name": "blockNumber", "type": "uint256" },
      { "internalType": "bytes[]", "name": "returnData", "type": "bytes[]" }
    ],
    "stateMutability": "view",
    "type": "function"
  }
];

// --- Batch Index Market Metadata using Multicall3 ---
async function batchIndexMarketMetadata(marketAddresses) {
  if (!marketAddresses.length) return;
  const iface = new ethers.Interface(marketAbi);
  const multicall = new ethers.Contract(
    '0xcA11bde05977b3631167028862bE2a173976CA11',
    multicall3Abi,
    provider
  );

  // Prepare all calls for all markets
  const calls = [];
  for (const address of marketAddresses) {
    calls.push(
      { target: address, callData: iface.encodeFunctionData('predictionId', []) },
      { target: address, callData: iface.encodeFunctionData('question', []) },
      { target: address, callData: iface.encodeFunctionData('description', []) },
      { target: address, callData: iface.encodeFunctionData('category', []) },
      { target: address, callData: iface.encodeFunctionData('rule', []) },
      { target: address, callData: iface.encodeFunctionData('status', []) },
      { target: address, callData: iface.encodeFunctionData('resolutionDate', []) },
      { target: address, callData: iface.encodeFunctionData('resolved', []) },
      { target: address, callData: iface.encodeFunctionData('winningOutcome', []) },
      { target: address, callData: iface.encodeFunctionData('yesPool', []) },
      { target: address, callData: iface.encodeFunctionData('noPool', []) },
      { target: address, callData: iface.encodeFunctionData('volume', []) },
      { target: address, callData: iface.encodeFunctionData('tradesCount', []) }
    );
  }

  // Call multicall3
  const [, returnData] = await multicall.aggregate(calls);

  // Parse results and update DB
  for (let i = 0; i < marketAddresses.length; i++) {
    const base = i * 13;
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
      ] = [
        iface.decodeFunctionResult('predictionId', returnData[base])[0],
        iface.decodeFunctionResult('question', returnData[base + 1])[0],
        iface.decodeFunctionResult('description', returnData[base + 2])[0],
        iface.decodeFunctionResult('category', returnData[base + 3])[0],
        iface.decodeFunctionResult('rule', returnData[base + 4])[0],
        iface.decodeFunctionResult('status', returnData[base + 5])[0],
        iface.decodeFunctionResult('resolutionDate', returnData[base + 6])[0],
        iface.decodeFunctionResult('resolved', returnData[base + 7])[0],
        iface.decodeFunctionResult('winningOutcome', returnData[base + 8])[0],
        iface.decodeFunctionResult('yesPool', returnData[base + 9])[0],
        iface.decodeFunctionResult('noPool', returnData[base + 10])[0],
        iface.decodeFunctionResult('volume', returnData[base + 11])[0],
        iface.decodeFunctionResult('tradesCount', returnData[base + 12])[0]
      ];

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
          marketAddresses[i],
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
    } catch (err) {
      console.error('Error parsing multicall result for', marketAddresses[i], err);
    }
  }
}

// --- Single Market Indexing for Events ---
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
    await batchIndexMarketMetadata(marketAddresses);
    for (const marketAddress of marketAddresses) {
      listenToMarket(marketAddress);
    }
  } catch (err) {
    console.error('Error fetching existing markets:', err);
  }
}
listenToExistingMarkets();

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
        resolvedOutcome = await market.winningOutcome();
      } catch (e) {
        resolvedOutcome = null;
      }

      await db.query(
        `INSERT INTO trades (tx_hash, block_number, user_address, market_address, outcome, amount, shares, creator_fee, platform_fee, timestamp, user_fid, prediction_id, resolved_outcome, user_outcome)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
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
          resolvedOutcome,
          outcome
        ]
      );
      await indexMarketMetadata(marketAddress);
    } catch (err) {
      console.error('Error handling Trade event:', err);
    }
  });
  market.on('MarketResolved', async () => {
    await indexMarketMetadata(marketAddress);
    // Update all trades for this market with the new outcome
    const { rows } = await db.query(
      'SELECT outcome FROM markets WHERE market_address = $1',
      [marketAddress]
    );
    if (rows.length > 0) {
      const outcome = rows[0].outcome;
      await db.query(
        'UPDATE trades SET resolved_outcome = $1 WHERE market_address = $2',
        [outcome, marketAddress]
      );
    }
  });
  console.log('Listening for trades and resolution on market:', marketAddress);
}

// --- API Endpoints (unchanged) ---

app.get('/api/market-trades/:marketAddress', async (req, res) => {
  const { marketAddress } = req.params;
  try {
    const { rows } = await db.query(
      'SELECT * FROM trades WHERE market_address = $1 ORDER BY timestamp ASC',
      [marketAddress.toLowerCase()]
    );
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

app.get('/api/user-trades/:address', async (req, res) => {
  const { address } = req.params;
  try {
    const { rows } = await db.query(
      'SELECT * FROM trades WHERE LOWER(user_address) = $1 ORDER BY timestamp DESC LIMIT 100',
      [address.toLowerCase()]
    );
    const formatted = rows.map(row => ({
      ...row,
      amount_mon: ethers.formatEther(row.amount),
      shares_mon: ethers.formatEther(row.shares),
      creator_fee_mon: ethers.formatEther(row.creator_fee),
      platform_fee_mon: ethers.formatEther(row.platform_fee),
      prediction_id: row.prediction_id,
      resolved_outcome: row.resolved_outcome,
      user_outcome: row.user_outcome !== undefined ? row.user_outcome : row.outcome
    }));
    res.json(formatted);
  } catch (err) {
    console.error('API error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/api/resolved-markets', async (req, res) => {
  try {
    const { rows } = await db.query(
      "SELECT * FROM markets WHERE resolved = true ORDER BY resolution_date DESC LIMIT 100"
    );
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

// Health and root endpoints
app.head('/', (req, res) => {
  res.status(200).end();
});
app.get('/', (req, res) => {
  res.status(200).send('OK');
});
app.get('/health', (req, res) => {
  res.status(200).send('OK');
});

app.listen(process.env.PORT || 3001, () => {
  console.log('API running on port', process.env.PORT || 3001);
});
