CREATE TABLE IF NOT EXISTS trades (
    id SERIAL PRIMARY KEY,
    tx_hash VARCHAR(66) NOT NULL,
    block_number INTEGER NOT NULL,
    user_address VARCHAR(42) NOT NULL,
    market_address VARCHAR(42) NOT NULL,
    outcome INTEGER NOT NULL, -- 0 = NO, 1 = YES
    amount NUMERIC(78, 0) NOT NULL, -- in wei
    shares NUMERIC(78, 0) NOT NULL,
    creator_fee NUMERIC(78, 0) NOT NULL,
    platform_fee NUMERIC(78, 0) NOT NULL,
    timestamp TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_trades_user ON trades(user_address);
CREATE INDEX IF NOT EXISTS idx_trades_market ON trades(market_address);
