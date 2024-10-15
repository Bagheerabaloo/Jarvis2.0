-- Table to store static company address information
CREATE TABLE info_company_address (
    id SERIAL PRIMARY KEY,  -- Unique identifier for each record
    ticker_id INTEGER REFERENCES ticker(id),  -- Foreign key to link with the ticker table
    address1 VARCHAR(100),  -- Address line 1
    city VARCHAR(50),  -- City
    state VARCHAR(50),  -- State
    zip VARCHAR(20),  -- ZIP code
    country VARCHAR(50),  -- Country
    phone VARCHAR(20),  -- Phone number
    website VARCHAR(100)  -- Website URL
);
