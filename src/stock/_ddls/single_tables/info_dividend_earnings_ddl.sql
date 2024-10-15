CREATE TABLE info_dividend_earnings (
    id SERIAL PRIMARY KEY,  -- Unique identifier for each record
    ticker_id INTEGER REFERENCES ticker(id),  -- Foreign key to ticker table
    date TIMESTAMP,  -- Timestamp to track when the data was recorded
    dividend_date DATE,  -- Dividend date
    earnings_high NUMERIC(10, 4),  -- Earnings high estimate
    earnings_low NUMERIC(10, 4),  -- Earnings low estimate
    earnings_average NUMERIC(10, 4),  -- Earnings average estimate
    revenue_high BIGINT,  -- Revenue high estimate
    revenue_low BIGINT,  -- Revenue low estimate
    revenue_average BIGINT  -- Revenue average estimate
);
