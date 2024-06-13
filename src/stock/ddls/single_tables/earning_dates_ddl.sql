-- Table to store earnings dates for each ticker
CREATE TABLE earnings_dates (
    id SERIAL PRIMARY KEY,  -- Unique identifier for each earnings date
    ticker_id INTEGER REFERENCES ticker(id),  -- Foreign key to ticker table
    date DATE,  -- Date of the earnings report
    earnings_period VARCHAR(20),  -- Earnings period
    eps_estimate NUMERIC,  -- EPS estimate
    reported_eps NUMERIC,  -- Reported EPS
    surprise_percent NUMERIC  -- Earnings surprise percentage
);