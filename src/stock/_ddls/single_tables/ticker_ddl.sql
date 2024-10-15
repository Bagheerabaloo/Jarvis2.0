-- Table to store basic information about each ticker
CREATE TABLE ticker (
    id SERIAL PRIMARY KEY,  -- Unique identifier for each ticker
    symbol VARCHAR(10) UNIQUE NOT NULL,  -- Ticker symbol (e.g., AAPL)
    company_name TEXT,  -- Full company name
    business_summary TEXT  -- Business summary description
);