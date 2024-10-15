CREATE TABLE major_holders (
    id SERIAL PRIMARY KEY,  -- Unique identifier for each major holder record
    ticker_id INTEGER REFERENCES ticker(id),  -- Foreign key to ticker table
    date TIMESTAMP,  -- Date when the information was recorded
    insiders_percent_held NUMERIC,  -- Percentage of shares held by insiders
    institutions_percent_held NUMERIC,  -- Percentage of shares held by institutions
    institutions_float_percent_held NUMERIC,  -- Percentage of float shares held by institutions
    institutions_count BIGINT  -- Number of institutions holding shares
);
