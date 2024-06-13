CREATE TABLE info_general_stock (
    id SERIAL PRIMARY KEY,  -- Unique identifier for each record
    ticker_id INTEGER REFERENCES ticker(id),  -- Foreign key to ticker table
    date TIMESTAMP,  -- Timestamp to track when the data was recorded
    isin VARCHAR(20),  -- ISIN
    currency VARCHAR(10),  -- Currency used for stock values
    day_high NUMERIC(10, 4),  -- Day's high price
    day_low NUMERIC(10, 4),  -- Day's low price
    exchange VARCHAR(10),  -- Stock exchange
    fifty_day_average NUMERIC(10, 4),  -- Fifty day average price
    last_price NUMERIC(10, 4),  -- Last traded price
    last_volume BIGINT,  -- Last traded volume
    market_cap BIGINT,  -- Market capitalization
    open NUMERIC(10, 4),  -- Opening price
    previous_close NUMERIC(10, 4),  -- Previous closing price
    quote_type VARCHAR(20),  -- Quote type
    regular_market_previous_close NUMERIC(10, 4),  -- Regular market previous close
    shares BIGINT,  -- Shares outstanding
    ten_day_average_volume BIGINT,  -- Ten day average volume
    three_month_average_volume BIGINT,  -- Three month average volume
    timezone VARCHAR(10),  -- Timezone
    two_hundred_day_average NUMERIC(10, 4),  -- Two hundred day average price
    year_change NUMERIC(10, 4),  -- Yearly change
    year_high NUMERIC(10, 4),  -- Yearly high
    year_low NUMERIC(10, 4)  -- Yearly low
);