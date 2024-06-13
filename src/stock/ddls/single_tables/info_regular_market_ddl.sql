CREATE TABLE info_regular_market (
    id SERIAL PRIMARY KEY,  -- Unique identifier for each record
    ticker_id INTEGER REFERENCES ticker(id),  -- Foreign key to ticker table
    date TIMESTAMP,  -- Timestamp to track when the data was recorded
    regular_market_price NUMERIC(10, 4),  -- Regular market price
    fifty_two_week_high NUMERIC(10, 4),  -- Fifty-two week high
    fifty_two_week_low NUMERIC(10, 4),  -- Fifty-two week low
    regular_market_day_high NUMERIC(10, 4),  -- Regular market day high
    regular_market_day_low NUMERIC(10, 4),  -- Regular market day low
    regular_market_volume BIGINT,  -- Regular market volume
    chart_previous_close NUMERIC(10, 4),  -- Chart previous close
    scale INTEGER,  -- Scale
    price_hint INTEGER,  -- Price hint
    data_granularity VARCHAR(10),  -- Data granularity
    range VARCHAR(20)  -- Range
);
