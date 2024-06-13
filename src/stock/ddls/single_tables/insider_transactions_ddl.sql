CREATE TABLE insider_transactions (
    id SERIAL PRIMARY KEY,  -- Unique identifier for each insider transaction record
    ticker_id INTEGER REFERENCES ticker(id),  -- Foreign key to ticker table
    shares BIGINT,  -- Number of shares involved in the transaction
    value NUMERIC,  -- Value of the transaction
    url TEXT,  -- URL with more information about the transaction
    text TEXT,  -- Description of the transaction
    insider VARCHAR(255),  -- Name of the insider
    position VARCHAR(255),  -- Position of the insider
    transaction_type VARCHAR(255),  -- Type of transaction
    start_date TIMESTAMP,  -- Start date of the transaction
    ownership VARCHAR(1)  -- Ownership type (D for Direct, I for Indirect)
);
