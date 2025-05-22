import 'dotenv/config';
import { ethers } from 'ethers';
import { Pool } from 'pg';
import express from 'express';
import cors from 'cors';
import fetch from 'node-fetch';

// Initialize Express app
const app = express();
app.use(cors());

// --- Database Pool Setup ---
const db = new Pool({
  connectionString: process.env.DATABASE_URL,
  idleTimeoutMillis: 30000, // Close idle connections after 30 seconds
  connectionTimeoutMillis: 5000, // Timeout connection attempts after 5 seconds
  max: 10, // Max number of connections in the pool
  keepAlive: true, // Enable TCP keep-alive to prevent connection drops
});

// Handle database pool errors and attempt reconnection
db.on('error', (err, client) => {
  console.error('Database pool error:', err.stack);
  client.release();
  setTimeout(async () => {
    try {
      await db.connect();
      console.log('Reconnected to database pool');
    } catch (reconnectErr) {
      console.error('Failed to reconnect to database:', reconnectErr.stack);
    }
  }, 5000);
});

// Function to ensure database connection is alive before queries
async function ensureDbConnection() {
  try {
    await db.query('SELECT 1');
    console.log('Database connection check successful');
  } catch (err) {
    console.error('Database connection check failed:', err.stack);
    throw err;
  }
}

// --- Provider Setup with Reconnection Logic ---
let wsProvider;
let httpProvider;
let factory;
let multicall;

// Function to initialize WebSocketProvider with reconnection
async function initializeWsProvider() {
  try {
    wsProvider = new ethers.WebSocketProvider(process.env.WS_RPC_URL);
    await wsProvider.getBlockNumber();
    console.log('WebSocket provider initialized successfully');
    return true;
  } catch (err) {
    console.error('Failed to initialize WebSocket provider:', err.message);
    wsProvider?.destroy();
    return false;
  }
}

// Function to initialize providers with reconnection loop
async function initializeProviders() {
  httpProvider = new ethers.JsonRpcProvider(process.env.RPC_URL);

  let attempts = 0;
  const maxAttempts = 5;
  while (attempts < maxAttempts) {
    attempts++;
    const success = await initializeWsProvider();
    if (success) break;
    console.log(`Retrying WebSocket provider initialization (attempt ${attempts}/${maxAttempts})...`);
    await new Promise(resolve => setTimeout(resolve, 5000));
  }

  if (!wsProvider) {
    console.error('Failed to initialize WebSocket provider after max attempts, falling back to HTTP provider');
    wsProvider = httpProvider;
  }
}

// Monitor WebSocket provider health and reconnect if needed
async function monitorWsProvider() {
  try {
    await wsProvider.getBlockNumber();
    console.log('WebSocket provider is healthy');
  } catch (err) {
    console.error('WebSocket provider disconnected:', err.message);
    cleanupMarketListeners();
    await initializeProviders();
    reinitializeContracts();
    setupFactoryListeners(); // Reattach factory listeners
    await listenToExistingMarkets();
  }
}

// Run provider health check every 5 minutes
setInterval(monitorWsProvider, 5 * 60 * 1000);

// --- Contract Setup ---
const factoryAbi = JSON.parse(process.env.CONTRACT_FACTORY_ABI);
const marketAbi = JSON.parse(process.env.CONTRACT_MARKET_ABI);

// Multicall3 ABI
const multicall3Abi = [
  {
    inputs: [
      {
        components: [
          { internalType: 'address', name: 'target', type: 'address' },
          { internalType: 'bytes', name: 'callData', type: 'bytes' },
        ],
        internalType: 'struct Multicall3.Call[]',
        name: 'calls',
        type: 'tuple[]',
      },
    ],
    name: 'aggregate',
    outputs: [
      { internalType: 'uint256', name: 'blockNumber', type: 'uint256' },
      { internalType: 'bytes[]', name: 'returnData', type: 'bytes[]' },
    ],
    stateMutability: 'view',
    type: 'function',
  },
];

function reinitializeContracts() {
  factory = new ethers.Contract(process.env.FACTORY_ADDRESS, factoryAbi, wsProvider);
  multicall = new ethers.Contract('0xcA11bde05977b3631167028862bE2a173976CA11', multicall3Abi, httpProvider);
  console.log('Contracts reinitialized');
}

// Function to set up factory event listeners
function setupFactoryListeners() {
  factory.on('MarketCreated', async (marketAddress, creator, predictionId, event) => {
    console.log('New market created at:', marketAddress);
    await indexMarketMetadata(marketAddress);
    listenToMarket(marketAddress);
  });
  console.log('Factory event listeners set up');
}

// Initialize providers and contracts on startup
(async () => {
  try {
    await initializeProviders();
    reinitializeContracts();
    setupFactoryListeners(); // Set up factory listeners after initialization
    await listenToExistingMarkets();
  } catch (err) {
    console.error('Failed to initialize application:', err.message, err.stack);
    process.exit(1); // Exit with failure if initialization fails
  }
})();

// --- Batch Index Market Metadata using Multicall3 ---
async function batchIndexMarketMetadata(marketAddresses) {
  if (!marketAddresses.length) return;
  const iface = new ethers.Interface(marketAbi);

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
      { target: address, callData: iface.encodeFunctionData('tradesCount', []) },
      { target: address, callData: iface.encodeFunctionData('creatorFid', []) }
    );
  }

  try {
    const [, returnData] = await multicall.aggregate(calls);

    for (let i = 0; i < marketAddresses.length; i++) {
      const base = i * 14;
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
          tradesCount,
          creatorFid,
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
          iface.decodeFunctionResult('tradesCount', returnData[base + 12])[0],
          iface.decodeFunctionResult('creatorFid', returnData[base + 13])[0],
        ];

        await ensureDbConnection();
        await db.query(
          `INSERT INTO markets (
            market_address, prediction_id, question, description, category, rule, status, resolution_date, resolved, outcome, yes_pool, no_pool, volume, trades_count, user_fid
          ) VALUES ($1,$2,$3,$4,$5,$6,$7,TO_TIMESTAMP($8),$9,$10,$11,$12,$13,$14,$15)
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
            trades_count = EXCLUDED.trades_count,
            user_fid = EXCLUDED.user_fid`,
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
            Number(tradesCount),
            creatorFid?.toString() || null,
          ]
        );
        console.log(`Batch indexed market ${marketAddresses[i]}: predictionId=${predictionId}`);
      } catch (err) {
        console.error('Error parsing multicall result for', marketAddresses[i], err.message, err.stack);
      }
    }
  } catch (err) {
    console.error('Error in batchIndexMarketMetadata:', err.message, err.stack);
  }
}

// --- Single Market Indexing for Events ---
async function indexMarketMetadata(marketAddress) {
  const contractForReads = new ethers.Contract(marketAddress, marketAbi, httpProvider);
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
      tradesCount,
      creatorFid,
    ] = await Promise.all([
      contractForReads.predictionId(),
      contractForReads.question(),
      contractForReads.description(),
      contractForReads.category(),
      contractForReads.rule(),
      contractForReads.status(),
      contractForReads.resolutionDate(),
      contractForReads.resolved(),
      contractForReads.winningOutcome().catch(() => null),
      contractForReads.yesPool(),
      contractForReads.noPool(),
      contractForReads.volume(),
      contractForReads.tradesCount(),
      contractForReads.creatorFid(),
    ]);
    console.log(`Indexing market ${marketAddress}: predictionId=${predictionId}, resolved=${resolved}`);

    await ensureDbConnection();
    await db.query(
      `INSERT INTO markets (
        market_address, prediction_id, question, description, category, rule, status, resolution_date, resolved, outcome, yes_pool, no_pool, volume, trades_count, user_fid
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,TO_TIMESTAMP($8),$9,$10,$11,$12,$13,$14,$15)
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
        trades_count = EXCLUDED.trades_count,
        user_fid = EXCLUDED.user_fid`,
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
        Number(tradesCount),
        creatorFid?.toString() || null,
      ]
    );
    console.log('Indexed/updated market metadata for', marketAddress);
  } catch (err) {
    console.error(`Error indexing market metadata for ${marketAddress}:`, err.message, err.stack);
  }
}

// --- Manage Market Event Listeners ---
const marketListeners = new Map();

function cleanupMarketListeners() {
  marketListeners.forEach(({ tradeListener, resolvedListener }, marketAddress) => {
    const market = new ethers.Contract(marketAddress, marketAbi, wsProvider);
    market.removeListener('Trade', tradeListener);
    market.removeListener('MarketResolved', resolvedListener);
    console.log('Cleaned up listeners for market:', marketAddress);
  });
  marketListeners.clear();
}

// Periodic cleanup of inactive markets
setInterval(async () => {
  try {
    const { rows } = await db.query(
      'SELECT market_address, resolved FROM markets WHERE resolved = true OR resolution_date < NOW() - INTERVAL \'30 days\''
    );
    for (const { market_address, resolved } of rows) {
      if (marketListeners.has(market_address)) {
        const { tradeListener, resolvedListener } = marketListeners.get(market_address);
        const market = new ethers.Contract(market_address, marketAbi, wsProvider);
        market.removeListener('Trade', tradeListener);
        market.removeListener('MarketResolved', resolvedListener);
        marketListeners.delete(market_address);
        console.log(`Cleaned up listeners for inactive/resolved market: ${market_address}`);
      }
    }
  } catch (err) {
    console.error('Error during periodic market listener cleanup:', err.message, err.stack);
  }
}, 6 * 60 * 60 * 1000); // Run every 6 hours

function listenToMarket(marketAddress) {
  const contractForEvents = new ethers.Contract(marketAddress, marketAbi, wsProvider);
  const contractForReads = new ethers.Contract(marketAddress, marketAbi, httpProvider);

  const tradeListener = async (user, outcome, amount, shares, creatorFee, platformFee, event) => {
    try {
      const { transactionHash, blockNumber, address: marketAddress } = event.log;
      const block = await httpProvider.getBlock(blockNumber);
      const timestamp = new Date(block.timestamp * 1000);

      let userFid = null;
      try {
        userFid = await contractForReads.userFid(user);
        if (userFid) userFid = userFid.toString();
      } catch (e) {
        console.warn('Could not fetch userFid for', user, e);
      }

      let predictionId = null;
      let resolvedOutcome = null;
      try {
        predictionId = await contractForReads.predictionId();
      } catch (e) {
        console.warn('Could not fetch predictionId for', marketAddress, e);
      }
      try {
        resolvedOutcome = await contractForReads.winningOutcome();
      } catch (e) {
        resolvedOutcome = null;
      }

      await ensureDbConnection();
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
          outcome,
        ]
      );
      await indexMarketMetadata(marketAddress);
      console.log(`Processed Trade event for market ${marketAddress}`);
    } catch (err) {
      console.error('Error handling Trade event:', err.message, err.stack);
      monitorWsProvider();
    }
  };

  const resolvedListener = async () => {
    try {
      await indexMarketMetadata(marketAddress);
      const { rows } = await db.query('SELECT resolved, outcome FROM markets WHERE market_address = $1', [marketAddress]);
      if (rows.length > 0) {
        const { resolved, outcome } = rows[0];
        if (resolved) {
          const resolvedOutcomeInt = typeof outcome === 'boolean' ? (outcome ? 1 : 0) : outcome;
          await db.query('UPDATE trades SET resolved_outcome = $1 WHERE market_address = $2', [resolvedOutcomeInt, marketAddress]);
          contractForEvents.removeListener('Trade', tradeListener);
          contractForEvents.removeListener('MarketResolved', resolvedListener);
          marketListeners.delete(marketAddress);
          console.log('Cleaned up listeners for resolved market:', marketAddress);
        }
      }
    } catch (err) {
      console.error('Error handling MarketResolved event:', err.message, err.stack);
      monitorWsProvider();
    }
  };

  contractForEvents.on('Trade', tradeListener);
  contractForEvents.on('MarketResolved', resolvedListener);

  marketListeners.set(marketAddress, { tradeListener, resolvedListener });
  console.log('Listening for trades and resolution on market:', marketAddress);
}

// --- Listen to Existing Markets on Startup ---
async function listenToExistingMarkets() {
  try {
    const marketAddresses = await factory.getMarkets();
    console.log(`Found ${marketAddresses.length} existing markets`);
    await batchIndexMarketMetadata(marketAddresses);
    for (const marketAddress of marketAddresses) {
      listenToMarket(marketAddress);
    }
  } catch (err) {
    console.error('Error fetching existing markets:', err.message, err.stack);
    monitorWsProvider();
  }
}

// --- API Endpoints ---
app.get('/api/market-trades/:marketAddress', async (req, res) => {
  const { marketAddress } = req.params;
  try {
    await ensureDbConnection();
    const { rows } = await db.query(
      'SELECT * FROM trades WHERE market_address = $1 ORDER BY timestamp ASC',
      [marketAddress.toLowerCase()]
    );
    const formatted = rows.map((row) => ({
      ...row,
      amount_mon: ethers.formatEther(row.amount),
      shares_mon: ethers.formatEther(row.shares),
      creator_fee_mon: ethers.formatEther(row.creator_fee),
      platform_fee_mon: ethers.formatEther(row.platform_fee),
    }));
    res.json(formatted);
  } catch (err) {
    console.error('API error /market-trades:', err.message, err.stack);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/api/live-markets', async (req, res) => {
  try {
    await ensureDbConnection();
    const { rows } = await db.query(
      "SELECT * FROM markets WHERE status = 'live' AND resolved = false ORDER BY resolution_date ASC LIMIT 100"
    );
    res.json(rows);
  } catch (err) {
    console.error('API error /live-markets:', err.message, err.stack);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/api/user-trades/:address', async (req, res) => {
  const { address } = req.params;
  try {
    await ensureDbConnection();
    const { rows } = await db.query(
      'SELECT * FROM trades WHERE LOWER(user_address) = $1 ORDER BY timestamp DESC LIMIT 100',
      [address.toLowerCase()]
    );
    const formatted = rows.map((row) => ({
      ...row,
      amount_mon: ethers.formatEther(row.amount),
      shares_mon: ethers.formatEther(row.shares),
      creator_fee_mon: ethers.formatEther(row.creator_fee),
      platform_fee_mon: ethers.formatEther(row.platform_fee),
      prediction_id: row.prediction_id,
      resolved_outcome: row.resolved_outcome,
      user_outcome: row.user_outcome !== undefined ? row.user_outcome : row.outcome,
    }));
    res.json(formatted);
  } catch (err) {
    console.error('API error /user-trades:', err.message, err.stack);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/api/resolved-markets', async (req, res) => {
  try {
    await ensureDbConnection();
    const { rows } = await db.query(
      'SELECT * FROM markets WHERE resolved = true ORDER BY resolution_date DESC LIMIT 100'
    );
    const formatted = rows.map((m) => {
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
    console.error('API error /resolved-markets:', err.message, err.stack);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/api/leaderboard', async (req, res) => {
  try {
    await ensureDbConnection();
    const { rows } = await db.query(`
      SELECT
        user_address,
        COUNT(DISTINCT prediction_id) AS total_predictions,
        COUNT(DISTINCT CASE WHEN user_outcome::int = resolved_outcome::int AND resolved_outcome IS NOT NULL THEN prediction_id END) AS correct_predictions,
        COUNT(*) AS total_trades,
        SUM(CASE WHEN resolved_outcome IS NOT NULL AND (CASE WHEN user_outcome::int = resolved_outcome::int THEN shares::numeric ELSE 0 END) > amount::numeric THEN 1 ELSE 0 END) AS profitable_trades,
        SUM(CASE WHEN resolved_outcome IS NOT NULL THEN (CASE WHEN user_outcome::int = resolved_outcome::int THEN shares::numeric ELSE 0 END) - amount::numeric ELSE 0 END) AS total_pnl,
        SUM(amount::numeric) AS total_volume
      FROM trades
      GROUP BY user_address
      ORDER BY total_pnl DESC
      LIMIT 20
    `);
    res.json(rows);
  } catch (err) {
    console.error('Leaderboard API error:', err.message, err.stack);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/api/activity', async (req, res) => {
  const fids = req.query.fids ? req.query.fids.split(',').map(fid => fid.trim()) : [];
  if (!fids.length) return res.json([]);

  const NEYNAR_API_KEY = process.env.NEYNAR_API_KEY;
  const profileCache = {};

  try {
    await ensureDbConnection();
    const { rows } = await db.query(
      `SELECT t.*, m.question
       FROM trades t
       LEFT JOIN markets m ON t.market_address = m.market_address
       WHERE t.user_fid = ANY($1::int[])
       ORDER BY t.timestamp DESC
       LIMIT 50`,
      [fids]
    );

    const enriched = await Promise.all(rows.map(async (row) => {
      let username = '', avatar = '';
      if (row.user_fid && NEYNAR_API_KEY) {
        const profile = await fetchNeynarProfile(row.user_fid, NEYNAR_API_KEY, profileCache);
        username = profile.username;
        avatar = profile.avatar;
      }
      return {
        id: row.tx_hash,
        username,
        avatar,
        action: 'prediction',
        details: row.question || row.prediction_id || '',
        timestamp: row.timestamp,
        amount: ethers.formatEther(row.amount),
        prediction: row.outcome === 1 || row.outcome === '1' ? 'yes' : 'no',
      };
    }));
    res.json(enriched);
  } catch (err) {
    console.error('API error in /api/activity:', err.message, err.stack);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/api/expired-markets', async (req, res) => {
  try {
    await ensureDbConnection();
    const { rows } = await db.query(
      "SELECT * FROM markets WHERE resolved = false AND resolution_date <= NOW() ORDER BY resolution_date ASC"
    );
    res.json(rows);
  } catch (err) {
    console.error('API error /expired-markets:', err.message, err.stack);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/api/user-portfolio/:address', async (req, res) => {
  const { address } = req.params;
  try {
    await ensureDbConnection();
    const { rows: trades } = await db.query(
      'SELECT * FROM trades WHERE LOWER(user_address) = $1',
      [address.toLowerCase()]
    );
    const { rows: markets } = await db.query('SELECT * FROM markets');

    const sharesByMarket = {};
    for (const trade of trades) {
      if (!sharesByMarket[trade.market_address]) {
        sharesByMarket[trade.market_address] = { yes: 0, no: 0 };
      }
      if (trade.outcome === 1 || trade.outcome === '1') {
        sharesByMarket[trade.market_address].yes += Number(trade.shares_mon);
      } else {
        sharesByMarket[trade.market_address].no += Number(trade.shares_mon);
      }
    }

    const portfolio = markets.map(market => ({
      id: market.prediction_id,
      marketAddress: market.market_address,
      question: market.question,
      description: market.description,
      category: market.category,
      rule: market.rule,
      endDate: market.resolution_date,
      yesPrice: market.yes_pool,
      noPrice: market.no_pool,
      volume: market.volume,
      tradesCount: market.trades_count,
      status: market.status,
      resolved: market.resolved,
      outcome: market.outcome,
      yesPool: market.yes_pool,
      noPool: market.no_pool,
      creator: market.creator,
      yesShares: sharesByMarket[market.market_address]?.yes || 0,
      noShares: sharesByMarket[market.market_address]?.no || 0,
    }));

    res.json(portfolio.filter(m => m.yesShares > 0 || m.noShares > 0));
  } catch (err) {
    console.error('API error /user-portfolio:', err.message, err.stack);
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

// --- Frame endpoint for a prediction market ---
app.get('/frame/:marketId', async (req, res) => {
  const { marketId } = req.params;
  // TODO: Fetch real market data from your DB
  const question = "Will ETH reach $5k by 2025?"; // Replace with real data
  const imageUrl = `https://yourdomain.com/og-image/${marketId}.png`; // Optional: dynamic OG image

  res.set('Content-Type', 'text/html');
  res.send(`
    <!DOCTYPE html>
    <html>
      <head>
        <meta property="og:title" content="Prediction Market" />
        <meta property="og:description" content="${question}" />
        <meta property="og:image" content="${imageUrl}" />
        <meta property="fc:frame" content="vNext" />
        <meta property="fc:frame:image" content="${imageUrl}" />
        <meta property="fc:frame:button:1" content="Vote YES" />
        <meta property="fc:frame:button:2" content="Vote NO" />
        <meta property="fc:frame:post_url" content="https://yourdomain.com/api/frame-action/${marketId}" />
      </head>
      <body>
        <h1>Prediction Market Frame</h1>
      </body>
    </html>
  `);
});

// Start the server
const port = process.env.PORT || 3001;
app.listen(port, () => {
  console.log('API running on port', port);
});

// Global error handlers
process.on('uncaughtException', (err) => {
  console.error('Uncaught Exception:', err.message, err.stack);
});
process.on('unhandledRejection', (err) => {
  console.error('Unhandled Rejection:', err.message, err.stack);
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('Received SIGTERM, shutting down...');
  cleanupMarketListeners();
  await db.end();
  wsProvider?.destroy();
  process.exit(0);
});

// Fetch Neynar profile with timeout
async function fetchNeynarProfile(fid, apiKey, cache) {
  if (cache[fid]) return cache[fid];

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 5000);

  try {
    const res = await fetch(`https://api.neynar.com/v2/farcaster/user?fid=${fid}`, {
      headers: { 'x-api-key': apiKey },
      signal: controller.signal,
    });
    clearTimeout(timeoutId);

    const data = await res.json();
    if (data && data.result && data.result.user) {
      const user = data.result.user;
      const profile = {
        username: user.username || '',
        avatar: user.pfp_url || '',
      };
      cache[fid] = profile;
      return profile;
    }
  } catch (e) {
    clearTimeout(timeoutId);
    console.error(`Error fetching Neynar profile for FID ${fid}:`, e.message);
  }

  cache[fid] = { username: '', avatar: '' };
  return cache[fid];
}
