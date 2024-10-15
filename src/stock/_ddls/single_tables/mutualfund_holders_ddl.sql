CREATE TABLE mutualfund_holders (
    id SERIAL PRIMARY KEY,  -- Unique identifier for each mutual fund holder record
    ticker_id INTEGER REFERENCES ticker(id),  -- Foreign key to ticker table
    date_reported TIMESTAMP,  -- Date when the information was reported
    holder VARCHAR(255),  -- Name of the mutual fund holder
    pct_held NUMERIC,  -- Percentage of shares held
    shares BIGINT,  -- Number of shares held
    value NUMERIC  -- Value of the shares held
);
