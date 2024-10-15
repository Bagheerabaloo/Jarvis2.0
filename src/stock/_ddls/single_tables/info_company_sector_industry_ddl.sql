-- Table to store historical changes in sector and industry for each company
CREATE TABLE info_company_sector_industry_history (
    id SERIAL PRIMARY KEY,  -- Unique identifier for each record
    ticker_id INTEGER REFERENCES ticker(id),  -- Foreign key to link with the ticker table
    sector VARCHAR(100),  -- Sector of the company
    industry VARCHAR(100),  -- Industry of the company
    start_date DATE,  -- Start date of the sector and industry validity
    end_date DATE  -- End date of the sector and industry validity (optional, NULL if currently valid)
);