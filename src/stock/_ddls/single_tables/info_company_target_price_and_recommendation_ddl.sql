-- Table to store static target price and recommendation information for each company
CREATE TABLE info_company_target_price_and_recommendation (
    id SERIAL PRIMARY KEY,  -- Unique identifier for each record
    ticker_id INTEGER REFERENCES ticker(id),  -- Foreign key to link with the ticker table
    date TIMESTAMP,  -- Timestamp to track when the data was recorded

    -- Target Price and Recommendation Information
    current_price NUMERIC(10, 4),  -- Current price of the stock
    target_high_price NUMERIC(10, 4),  -- Target high price
    target_low_price NUMERIC(10, 4),  -- Target low price
    target_mean_price NUMERIC(10, 4),  -- Target mean price
    target_median_price NUMERIC(10, 4),  -- Target median price
    recommendation_mean NUMERIC(10, 4),  -- Mean recommendation value
    recommendation_key VARCHAR(20),  -- Recommendation key (e.g., "buy", "hold")
    number_of_analyst_opinions INTEGER  -- Number of analyst opinions
);