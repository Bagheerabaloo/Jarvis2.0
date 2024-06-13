CREATE TABLE info_exchange_trading (
    id SERIAL PRIMARY KEY,  -- Unique identifier for each record
    ticker_id INTEGER REFERENCES ticker(id),  -- Foreign key to ticker table
    date TIMESTAMP,  -- Timestamp to track when the data was recorded
    symbol VARCHAR(10),  -- Stock symbol
    exchange_name VARCHAR(50),  -- Exchange name
    full_exchange_name VARCHAR(50),  -- Full exchange name
    instrument_type VARCHAR(20),  -- Instrument type
    first_trade_date BIGINT,  -- First trade date
    regular_market_time BIGINT,  -- Regular market time
    has_pre_post_market_data BOOLEAN,  -- Pre and post market data availability
    gmt_offset INTEGER,  -- GMT offset
    exchange_timezone_name VARCHAR(50)  -- Exchange timezone name
);
