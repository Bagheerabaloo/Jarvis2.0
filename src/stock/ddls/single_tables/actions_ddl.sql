-- Table to store actions related to each ticker, with historical tracking
CREATE TABLE actions (
    ticker_id INTEGER REFERENCES ticker(id),  -- Foreign key to ticker table
    date TIMESTAMP,  -- Date of the action
    dividends NUMERIC,  -- Dividend amount
    stock_splits NUMERIC,  -- Stock split ratio
    PRIMARY KEY (ticker_id, date)  -- Composite primary key to ensure uniqueness
);