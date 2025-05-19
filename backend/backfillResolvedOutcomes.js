import 'dotenv/config';
import { ethers } from 'ethers';
import { Pool } from 'pg';

const db = new Pool({ connectionString: process.env.DATABASE_URL });
const provider = new ethers.JsonRpcProvider(process.env.RPC_URL);

const marketAbi = JSON.parse(process.env.CONTRACT_MARKET_ABI);

async function backfillResolvedOutcomes() {
  // 1. Find all unique market addresses with trades missing resolved_outcome
  const { rows: markets } = await db.query(
    "SELECT DISTINCT market_address FROM trades WHERE resolved_outcome IS NULL"
  );

  for (const { market_address } of markets) {
    try {
      const market = new ethers.Contract(market_address, marketAbi, provider);
      const resolved = await market.resolved();
      if (!resolved) {
        console.log(`Market ${market_address} not resolved yet, skipping.`);
        continue;
      }
      const outcome = await market.outcome();
      // 2. Update all trades for this market with the resolved outcome
      await db.query(
        "UPDATE trades SET resolved_outcome = $1 WHERE market_address = $2 AND resolved_outcome IS NULL",
        [Number(outcome), market_address]
      );
      console.log(`Updated trades for market ${market_address} with outcome ${Number(outcome)}`);
    } catch (err) {
      console.error(`Error processing market ${market_address}:`, err);
    }
  }

  await db.end();
  console.log('Backfill complete!');
}

backfillResolvedOutcomes();
