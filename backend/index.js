import 'dotenv/config';
import { ethers } from 'ethers';
import { Pool } from 'pg';
import express from 'express';
import cors from 'cors';
import fetch from 'node-fetch';
import satori from 'satori';
import svg2img from 'svg2img';
import fs from 'fs';
import { parseWebhookEvent, verifyAppKeyWithNeynar } from "@farcaster/frame-node";

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
      { target: address, callData: iface.encodeFunctionData('creatorFid', []) },
      { target: address, callData: iface.encodeFunctionData('sharesOutstanding', []) },
    );
  }

  try {
    const [, returnData] = await multicall.aggregate(calls);

    for (let i = 0; i < marketAddresses.length; i++) {
      const base = i * 15;
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
          sharesOutstanding,
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
          iface.decodeFunctionResult('sharesOutstanding', returnData[base + 14])[0],
        ];

        await ensureDbConnection();
        await db.query(
          `INSERT INTO markets (
            market_address, prediction_id, question, description, category, rule, status, resolution_date, resolved, outcome, yes_pool, no_pool, volume, trades_count, user_fid, shares_outstanding
          ) VALUES ($1,$2,$3,$4,$5,$6,$7,TO_TIMESTAMP($8),$9,$10,$11,$12,$13,$14,$15,$16)
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
            user_fid = EXCLUDED.user_fid,
            shares_outstanding = EXCLUDED.shares_outstanding`,
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
            sharesOutstanding ? sharesOutstanding.toString() : null,
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
      sharesOutstanding,
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
      contractForReads.sharesOutstanding().catch(() => null),
    ]);
    console.log(`Indexing market ${marketAddress}: predictionId=${predictionId}, resolved=${resolved}`);

    await ensureDbConnection();
    await db.query(
      `INSERT INTO markets (
        market_address, prediction_id, question, description, category, rule, status, resolution_date, resolved, outcome, yes_pool, no_pool, volume, trades_count, user_fid, shares_outstanding
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,TO_TIMESTAMP($8),$9,$10,$11,$12,$13,$14,$15,$16)
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
        user_fid = EXCLUDED.user_fid,
        shares_outstanding = EXCLUDED.shares_outstanding`,
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
        sharesOutstanding ? sharesOutstanding.toString() : null,
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
    const { rows: trades } = await db.query(
      'SELECT * FROM trades WHERE LOWER(user_address) = $1 ORDER BY timestamp DESC LIMIT 100',
      [address.toLowerCase()]
    );

    // Get all unique market addresses from the trades
    const marketAddresses = [...new Set(trades.map(t => t.market_address))];
    // Fetch all relevant markets in one query
    const { rows: markets } = await db.query(
      'SELECT market_address, yes_pool, no_pool, shares_outstanding, outcome FROM markets WHERE market_address = ANY($1)',
      [marketAddresses]
    );
    const marketMap = {};
    for (const m of markets) {
      marketMap[m.market_address] = m;
    }

    // Before mapping trades, build a map of total winning shares per market
    const winningSharesMap = {};
    for (const market of markets) {
      const marketTrades = trades.filter(t => t.market_address === market.market_address);
      const winningOutcome = String(market.outcome) === '1' || market.outcome === true ? 1 : 0;
      const totalWinningShares = marketTrades
        .filter(t => Number(t.outcome) === winningOutcome)
        .reduce((sum, t) => sum + Number(ethers.formatEther(t.shares)), 0);
      winningSharesMap[market.market_address] = BigInt(Math.round(totalWinningShares * 1e18));
    }

    function calculatePnL(trade, market, totalWinningShares) {
      const amount = BigInt(trade.amount);
      const shares = BigInt(trade.shares);
      const creatorFee = BigInt(trade.creator_fee || '0');
      const platformFee = BigInt(trade.platform_fee || '0');
      if (trade.resolved_outcome === null || trade.resolved_outcome === undefined) {
        return null; // Not resolved
      }
      if (!market) return null;
      const totalPool = BigInt(parseFloat(market.yes_pool) * 1e18 + parseFloat(market.no_pool) * 1e18);
      if (trade.user_outcome == trade.resolved_outcome) {
        if (totalWinningShares === 0n) return null;
        const claimable = (shares * totalPool) / totalWinningShares;
        return claimable - amount - creatorFee - platformFee;
      } else {
        return -amount - creatorFee - platformFee;
      }
    }

    const formatted = trades.map((row) => {
      const market = marketMap[row.market_address];
      const pnlWei = calculatePnL(row, market, winningSharesMap[row.market_address] || 0n);
      return {
        ...row,
        amount_mon: ethers.formatEther(row.amount),
        shares_mon: ethers.formatEther(row.shares),
        creator_fee_mon: ethers.formatEther(row.creator_fee),
        platform_fee_mon: ethers.formatEther(row.platform_fee),
        prediction_id: row.prediction_id,
        resolved_outcome: row.resolved_outcome,
        user_outcome: row.user_outcome !== undefined ? row.user_outcome : row.outcome,
        pnl_wei: pnlWei !== null ? pnlWei.toString() : null,
        pnl_mon: pnlWei !== null ? ethers.formatEther(pnlWei) : null,
      };
    });
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
    // Fetch on-chain values for each market
    const formatted = await Promise.all(rows.map(async (m) => {
      const yesPool = Number(m.yes_pool);
      const noPool = Number(m.no_pool);
      const totalPool = yesPool + noPool;
      // Fetch on-chain values
      let winningOutcome = null;
      let sharesOutstanding = null;
      try {
        const contract = new ethers.Contract(m.market_address, marketAbi, httpProvider);
        winningOutcome = await contract.winningOutcome();
        sharesOutstanding = await contract.sharesOutstanding();
      } catch (e) {
        // fallback to DB value if available
        winningOutcome = m.outcome;
        sharesOutstanding = m.shares_outstanding;
      }
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
        winningOutcome: winningOutcome?.toString(),
        sharesOutstanding: (sharesOutstanding ?? m.shares_outstanding)?.toString(),
      };
    }));
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
        MAX(user_fid) AS user_fid,
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

    // 1. Fetch prediction trades
    const { rows: predictionRows } = await db.query(
      `SELECT t.*, m.question
       FROM trades t
       LEFT JOIN markets m ON t.market_address = m.market_address
       WHERE t.user_fid = ANY($1::int[])
       ORDER BY t.timestamp DESC
       LIMIT 50`,
      [fids]
    );

    // 2. Fetch futures trades
    const { rows: futuresRows } = await db.query(
      `SELECT * FROM futures_trades WHERE user_fid = ANY($1::int[]) ORDER BY timestamp DESC LIMIT 50`,
      [fids]
    );

    // 3. Format prediction activities
    const predictionActivities = await Promise.all(predictionRows.map(async (row) => {
      let username = 'Unknown';
      if (row.user_fid && NEYNAR_API_KEY) {
        const profile = await fetchNeynarProfile(row.user_fid, NEYNAR_API_KEY, profileCache);
        if (profile && profile.username && profile.username.trim() !== '') {
          username = profile.username;
        } else if (row.username && row.username.trim() !== '') {
          username = row.username;
        }
      } else if (row.username && row.username.trim() !== '') {
        username = row.username;
      }
      const avatar = row.user_fid
        ? `https://api.dicebear.com/7.x/pixel-art/svg?seed=${row.user_fid}`
        : '/default-avatar.png';
      return {
        id: row.tx_hash,
        username,
        avatar,
        action: 'prediction',
        details: row.question || row.prediction_id || '',
        timestamp: row.timestamp,
        amount: ethers.formatEther(row.amount),
        prediction: row.outcome === 1 || row.outcome === '1' ? 'yes' : 'no',
        user_fid: row.user_fid,
      };
    }));

    // 4. Format futures activities
    const futuresActivities = await Promise.all(futuresRows.map(async (row) => {
      let username = 'Unknown';
      if (row.user_fid && NEYNAR_API_KEY) {
        const profile = await fetchNeynarProfile(row.user_fid, NEYNAR_API_KEY, profileCache);
        if (profile && profile.username && profile.username.trim() !== '') {
          username = profile.username;
        } else if (row.username && row.username.trim() !== '') {
          username = row.username;
        }
      } else if (row.username && row.username.trim() !== '') {
        username = row.username;
      }
      return {
        id: row.tx_hash,
        username,
        avatar: row.user_fid
          ? `https://api.dicebear.com/7.x/pixel-art/svg?seed=${row.user_fid}`
          : '/default-avatar.png',
        action: 'futures',
        details: row.asset,
        asset: row.asset,
        direction: row.direction,
        leverage: Number(row.leverage),
        entry_price: Number(row.entry_price),
        timestamp: row.timestamp,
        amount: Number(row.amount),
        user_fid: row.user_fid,
      };
    }));

    // 5. Merge and sort all activities
    const allActivities = [...predictionActivities, ...futuresActivities].sort(
      (a, b) => new Date(b.timestamp) - new Date(a.timestamp)
    );

    res.json(allActivities.slice(0, 10).map(a => ({
      ...a,
      user_fid: a.user_fid || a.fid,
    })));
  } catch (err) {
    console.error('API error in /api/activity:', err.message, err.stack);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/api/expired-markets', async (req, res) => {
  try {
    const now = new Date();
    const result = await db.query(
      `SELECT * FROM markets WHERE resolution_date < $1 AND resolved = false ORDER BY resolution_date ASC`,
      [now]
    );
    res.json(result.rows);
  } catch (err) {
    console.error('Error fetching expired markets:', err);
    res.status(500).json({ error: 'Failed to fetch expired markets' });
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
app.get('/frame/:id', async (req, res) => {
  const { id } = req.params;
  const ref = req.query.ref;
  // Fetch prediction data from DB
  const prediction = await getPredictionFromDB(id);
  if (!prediction) return res.status(404).send('Not found');
  // Build the post_url with ref if present
  const postUrl = ref
    ? `https://ragenodes.site/predictions?ref=${encodeURIComponent(ref)}`
    : `https://ragenodes.site/predictions`;
  res.send(`
    <!DOCTYPE html>
    <html>
      <head>
        <meta property="fc:frame" content="vNext" />
        <meta property="fc:frame:image" content="${prediction.imageUrl}" />
        <meta property="fc:frame:button:1" content="Trade" />
        <meta property="fc:frame:post_url" content="${postUrl}" />
        <meta property="og:title" content="${prediction.question}" />
        <meta property="og:image" content="${prediction.imageUrl}" />
      </head>
      <body>
        <h1>${prediction.question}</h1>
      </body>
    </html>
  `);
});

app.get('/og-image/:predictionId.png', async (req, res) => {
  const { predictionId } = req.params;
  try {
    // Fetch prediction data from DB
    await ensureDbConnection();
    const { rows } = await db.query('SELECT * FROM markets WHERE prediction_id = $1 LIMIT 1', [predictionId]);
    const market = rows[0];
    if (!market) return res.status(404).send('Prediction not found');

    // Fetch font
    const fontUrl = 'https://rsms.me/inter/font-files/Inter-Regular.woff';
    const fontData = await fetch(fontUrl).then(r => r.arrayBuffer());

    // Fetch logo image and encode as base64
    const logoPath = './public/icon.png';
    const logoBuffer = fs.readFileSync(logoPath);
    const logoBase64 = logoBuffer.toString('base64');

    // Generate SVG
    const svg = await satori(
      {
        type: 'div',
        props: {
          style: {
            width: 1200,
            height: 630,
            background: '#fff',
            display: 'flex',
            flexDirection: 'column',
            justifyContent: 'center',
            alignItems: 'center',
            fontFamily: 'Inter'
          },
          children: [
            {
              type: 'img',
              props: {
                src: `data:image/png;base64,${logoBase64}`,
                style: { width: 100, marginBottom: 20, borderRadius: 10 }
              }
            },
            {
              type: 'div',
              props: {
                style: { color: 'black', fontSize: 48, fontFamily: 'Inter', textAlign: 'center' },
                children: market.question
              }
            }
          ]
        }
      },
      {
        width: 1200,
        height: 630,
        fonts: [
          {
            name: 'Inter',
            data: fontData,
            weight: 400,
            style: 'normal'
          }
        ]
      }
    );

    // Convert SVG to PNG
    svg2img(svg, { width: 1200, height: 630 }, (error, buffer) => {
      if (error) {
        res.status(500).send('Failed to generate image');
      } else {
        res.set('Content-Type', 'image/png');
        res.send(buffer);
      }
    });
  } catch (e) {
    res.status(500).send('Failed to generate OG image');
  }
});

app.post('/api/webhook', express.json(), async (req, res) => {
  try {
    // This will throw if the signature is invalid
    const data = await parseWebhookEvent(req.body, verifyAppKeyWithNeynar);

    // Now data is verified and you can safely process it
    // ... (your logic here, e.g. store tokens, etc.)

    res.status(200).json({ success: true });
  } catch (e) {
    // Signature invalid or other error
    console.error('Invalid webhook signature:', e);
    res.status(400).json({ error: 'Invalid signature' });
  }
});

app.post('/api/log-futures-trade', express.json(), async (req, res) => {
  const { fid, wallet, txHash, asset, direction, size, leverage, entryPrice, amount } = req.body;
  if (!fid || !wallet) {
    return res.status(400).json({ error: 'Missing FID or wallet address' });
  }
  try {
    await ensureDbConnection();
    await db.query(
      `INSERT INTO futures_trades (tx_hash, user_fid, user_address, asset, direction, size, leverage, entry_price, timestamp, amount)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), $9)`,
      [txHash, fid, wallet, asset, direction, size, leverage, entryPrice, amount]
    );

    // After logging the trade for user with FID = friendFid
    const { rows: notifyRows } = await db.query(
      'SELECT fid FROM notify_friends WHERE $1 = ANY(friend_fids)',
      [fid] // friendFid = the FID of the user who just did the activity
    );

    for (const row of notifyRows) {
      // For each user who wants notifications about this friend
      const { rows: tokenRows } = await db.query(
        'SELECT token, url FROM notification_tokens WHERE fid = $1',
        [row.fid]
      );
      if (tokenRows.length) {
        await sendNotification(
          row.fid,
          `friend-activity-${fid}-${Date.now()}`,
          'Friend Activity!',
          `Your friend just made a new trade!`,
          'https://yourapp.com/friends'
        );
      }
    }

    res.json({ success: true });
  } catch (err) {
    console.error('Error logging futures trade:', err.message, err.stack);
    res.status(500).json({ error: 'Failed to log futures trade' });
  }
});

app.post('/api/save-notification-token', express.json(), async (req, res) => {
  const { fid, token, url } = req.body;
  if (!fid || !token || !url) {
    return res.status(400).json({ error: 'Missing fid, token, or url' });
  }
  try {
    await db.query(
      `INSERT INTO notification_tokens (fid, token, url)
       VALUES ($1, $2, $3)
       ON CONFLICT (fid) DO UPDATE SET token = EXCLUDED.token, url = EXCLUDED.url`,
      [fid, token, url]
    );
    res.json({ success: true });
  } catch (err) {
    console.error('Error saving notification token:', err.message, err.stack);
    res.status(500).json({ error: 'Failed to save notification token' });
  }
});

app.post('/api/save-notify-friends', express.json(), async (req, res) => {
  const { fid, notifyFids } = req.body;
  if (!fid || !Array.isArray(notifyFids)) {
    return res.status(400).json({ error: 'Missing fid or notifyFids' });
  }
  try {
    await db.query(
      `INSERT INTO notify_friends (fid, friend_fids)
       VALUES ($1, $2)
       ON CONFLICT (fid) DO UPDATE SET friend_fids = EXCLUDED.friend_fids`,
      [fid, notifyFids]
    );
    res.json({ success: true });
  } catch (err) {
    console.error('Error saving notify friends:', err.message, err.stack);
    res.status(500).json({ error: 'Failed to save notify friends' });
  }
});

app.get('/api/futures-leaderboard', async (req, res) => {
  try {
    await ensureDbConnection();
    const { rows } = await db.query(`
      SELECT
        user_address,
        MAX(user_fid) AS user_fid,
        COUNT(*) AS total_trades,
        SUM(amount::numeric) AS total_volume,
        SUM(size::numeric) AS total_size
      FROM futures_trades
      GROUP BY user_address
      ORDER BY total_volume DESC
      LIMIT 20
    `);
    res.json(rows);
  } catch (err) {
    console.error('Futures Leaderboard API error:', err.message, err.stack);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.post('/api/track-referral', express.json(), async (req, res) => {
  console.log("Received /api/track-referral RAW BODY:", req.body);
  const { referrerFid, referredFid } = req.body;
  if (!referrerFid || !referredFid) return res.status(400).json({ error: 'Missing FID' });
  try {
    await db.query(
      'INSERT INTO referrals (referrer_fid, referred_fid) VALUES ($1, $2) ON CONFLICT DO NOTHING',
      [referrerFid, referredFid]
    );
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: 'Failed to track referral' });
  }
});

app.get('/api/track-referral-test', (req, res) => {
  console.log("Received GET /api/track-referral-test");
  res.json({ ok: true });
});

app.post('/api/test-referral', express.json(), (req, res) => {
  console.log("Received /api/test-referral:", req.body);
  res.json({ ok: true });
});

// Optional: Set a secret in your .env, e.g. REWARD_API_SECRET=your_secret
const REWARD_API_SECRET = process.env.REWARD_API_SECRET;

app.post('/api/reward-referral', express.json(), async (req, res) => {
  console.log("Received POST /api/reward-referral", req.body);

  // Optional: Simple security check
  if (REWARD_API_SECRET && req.headers['x-api-secret'] !== REWARD_API_SECRET) {
    return res.status(403).json({ error: 'Forbidden' });
  }

  const { referrer, referred } = req.body;
  if (!referrer || !referred) {
    return res.status(400).json({ error: 'Missing referrer or referred address' });
  }
  try {
    // Lookup addresses
    const { rows: refRows } = await db.query('SELECT user_address FROM users WHERE fid = $1', [referrer]);
    const { rows: referredRows } = await db.query('SELECT user_address FROM users WHERE fid = $1', [referred]);
    if (!refRows.length || !referredRows.length) {
      return res.status(400).json({ error: 'Could not find user address for one or both FIDs' });
    }
    const referrerAddress = refRows[0].user_address;
    const referredAddress = referredRows[0].user_address;
    // Now call contract.rewardReferral(referrerAddress, referredAddress)
    const tx = await contract.rewardReferral(referrerAddress, referredAddress);
    await tx.wait();
    res.json({ success: true, txHash: tx.hash });
  } catch (err) {
    console.error('Error rewarding referral:', err);
    res.status(500).json({ error: err.message });
  }
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
    // Use the bulk endpoint for consistency
    const res = await fetch(`https://api.neynar.com/v2/farcaster/user/bulk?fids=${fid}`, {
      headers: { 'x-api-key': apiKey },
      signal: controller.signal,
    });
    clearTimeout(timeoutId);

    const data = await res.json();
    if (data && data.users && data.users.length > 0) {
      const user = data.users[0];
      const profile = {
        username: user.username || '',
        avatar: '', // not used, but you could use user.pfp_url if you want
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

app.get('/prediction/:id', async (req, res) => {
  const prediction = await getPredictionFromDB(req.params.id);
  res.send(`
    <!DOCTYPE html>
    <html>
      <head>
        <meta property="fc:frame" content="vNext" />
        <meta property="fc:frame:image" content="${prediction.imageUrl}" />
        <meta property="fc:frame:button:1" content="Trade" />
        <meta property="fc:frame:post_url" content="https://yourapp.com/prediction/${prediction.id}/trade" />
        <!-- ...other meta tags... -->
      </head>
      <body>
        <div id="root"></div>
        <script src="/main.js"></script>
      </body>
    </html>
  `);
});

/**
 * Send a Farcaster Mini App notification to a user.
 * @param {string} fid - The user's Farcaster FID.
 * @param {string} notificationId - Unique notification ID (e.g. "daily-reminder-2024-06-05").
 * @param {string} title - Notification title (max 32 chars).
 * @param {string} body - Notification body (max 128 chars).
 * @param {string} targetUrl - URL to open when user clicks notification (must be on your domain).
 */
async function sendNotification(fid, notificationId, title, body, targetUrl) {
  // 1. Get token and url from DB
  const { rows } = await db.query('SELECT token, url FROM notification_tokens WHERE fid = $1', [fid]);
  if (!rows.length) {
    console.warn(`No notification token found for fid ${fid}`);
    return;
  }

  const { token, url } = rows[0];

  // 2. Send notification
  const payload = {
    notificationId, // e.g. "daily-reminder-2024-06-05"
    title,          // e.g. "New Prediction!"
    body,           // e.g. "A new prediction market is live."
    targetUrl,      // e.g. "https://ragenodes.site/predictions"
    tokens: [token]
  };

  const response = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });

  const result = await response.json();
  console.log('Notification result:', result);
  return result;
}

await sendNotification(
  '123456', // FID of the user
  'new-market-2024-06-05', // notificationId (should be unique per notification type/day)
  'New Prediction!', // title
  'A new prediction market is live. Check it out!', // body
  'https://ragenodes.site/predictions' // targetUrl (where user lands when clicking notification)
);

await db.query(
  `INSERT INTO futures_trades (tx_hash, user_fid, username, asset, direction, size, leverage, entry_price, timestamp, amount)
   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), $9)`,
  [txHash, userFid, username, asset, direction, size, leverage, entryPrice, amount]
);

const { rows: futuresRows } = await db.query(
  `SELECT * FROM futures_trades WHERE user_fid = ANY($1::int[]) ORDER BY timestamp DESC LIMIT 50`,
  [fids]
);

// Example usage in your backend
await sendNotification(
  friendFid, // FID of the user to notify
  'activity-2024-06-06-<unique>', // notificationId (unique per notification)
  'Friend Activity!', // title
  'Your friend just made a new trade!', // body
  'https://ragenodes.site/predictions' // targetUrl
);

console.log('CONTRACT_ADDRESS:', process.env.CONTRACT_ADDRESS);
console.log('OWNER_PRIVATE_KEY:', !!process.env.OWNER_PRIVATE_KEY);
console.log('REFERRAL_REWARD_ABI:', !!process.env.REFERRAL_REWARD_ABI);
console.log('RPC_URL:', process.env.RPC_URL);

const CONTRACT_ADDRESS = process.env.CONTRACT_ADDRESS;
const PRIVATE_KEY = process.env.OWNER_PRIVATE_KEY;
const RPC_URL = process.env.RPC_URL;
const abi = JSON.parse(process.env.REFERRAL_REWARD_ABI);

const provider = new ethers.JsonRpcProvider(RPC_URL);
const ownerWallet = new ethers.Wallet(PRIVATE_KEY, provider);
const contract = new ethers.Contract(CONTRACT_ADDRESS, abi, ownerWallet);

// Endpoint to reward a referral on-chain
app.post('/api/reward-referral', express.json(), async (req, res) => {
  const { referrer, referred } = req.body;
  if (!referrer || !referred) {
    return res.status(400).json({ error: 'Missing referrer or referred address' });
  }
  try {
    // Lookup addresses
    const { rows: refRows } = await db.query('SELECT user_address FROM users WHERE fid = $1', [referrer]);
    const { rows: referredRows } = await db.query('SELECT user_address FROM users WHERE fid = $1', [referred]);
    if (!refRows.length || !referredRows.length) {
      return res.status(400).json({ error: 'Could not find user address for one or both FIDs' });
    }
    const referrerAddress = refRows[0].user_address;
    const referredAddress = referredRows[0].user_address;
    // Now call contract.rewardReferral(referrerAddress, referredAddress)
    const tx = await contract.rewardReferral(referrerAddress, referredAddress);
    await tx.wait();
    res.json({ success: true, txHash: tx.hash });
  } catch (err) {
    console.error('Error rewarding referral:', err);
    res.status(500).json({ error: err.message });
  }
});
