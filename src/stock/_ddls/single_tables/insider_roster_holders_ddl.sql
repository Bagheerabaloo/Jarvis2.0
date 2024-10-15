CREATE TABLE insider_roster_holders (
    id SERIAL PRIMARY KEY,  -- Unique identifier for each insider roster record
    ticker_id INTEGER REFERENCES ticker(id),  -- Foreign key to ticker table
    date TIMESTAMP,  -- Date when the information was recorded
    name VARCHAR(255),  -- Name of the insider
    position VARCHAR(255),  -- Position of the insider
    url TEXT,  -- URL with more information about the insider
    most_recent_transaction VARCHAR(255),  -- Most recent transaction type
    latest_transaction_date TIMESTAMP,  -- Date of the latest transaction
    shares_owned_directly BIGINT,  -- Number of shares owned directly
    position_direct_date TIMESTAMP,  -- Date of direct position
    shares_owned_indirectly BIGINT,  -- Number of shares owned indirectly
    position_indirect_date TIMESTAMP  -- Date of indirect position
);
