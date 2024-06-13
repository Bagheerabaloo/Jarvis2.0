CREATE TABLE recommendations (
    id SERIAL PRIMARY KEY,  -- Unique identifier for each record
    ticker_id INTEGER REFERENCES ticker(id),  -- Foreign key to ticker table
    date TIMESTAMP,  -- Date when the information was recorded
    period VARCHAR(10),  -- Period (e.g., "0m", "-1m")
    strong_buy INTEGER,  -- Number of strong buy recommendations
    buy INTEGER,  -- Number of buy recommendations
    hold INTEGER,  -- Number of hold recommendations
    sell INTEGER,  -- Number of sell recommendations
    strong_sell INTEGER  -- Number of strong sell recommendations
);
