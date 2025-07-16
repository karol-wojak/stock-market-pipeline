-- Create stock prices table
CREATE TABLE stock_prices (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    open DECIMAL(10,2),
    high DECIMAL(10,2),
    low DECIMAL(10,2),
    close DECIMAL(10,2),
    volume BIGINT
);

-- Create index for faster queries
CREATE INDEX idx_stock_symbol_time ON stock_prices(symbol, timestamp DESC);