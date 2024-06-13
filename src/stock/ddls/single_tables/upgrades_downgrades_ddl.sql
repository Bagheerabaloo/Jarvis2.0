CREATE TABLE upgrades_downgrades (
    id SERIAL PRIMARY KEY,  -- Unique identifier for each record
    ticker_id INTEGER REFERENCES ticker(id),  -- Foreign key to ticker table
    date TIMESTAMP,  -- Date when the upgrade/downgrade occurred
    firm VARCHAR(255),  -- Name of the firm issuing the upgrade/downgrade
    to_grade VARCHAR(50),  -- New grade assigned by the firm
    from_grade VARCHAR(50),  -- Previous grade assigned by the firm
    action VARCHAR(50)  -- Action taken (e.g., "main", "reit", "up", "init")
);
