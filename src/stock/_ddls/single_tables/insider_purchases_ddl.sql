CREATE TABLE insider_purchases (
    id SERIAL PRIMARY KEY,  -- Unique identifier for each insider purchase record
    ticker_id INTEGER REFERENCES ticker(id),  -- Foreign key to ticker table
    date TIMESTAMP,  -- Date when the information was recorded
    purchases_shares BIGINT,  -- Number of shares purchased in the last 6 months
    purchases_transactions INTEGER,  -- Number of purchase transactions
    sales_shares BIGINT,  -- Number of shares sold in the last 6 months
    sales_transactions INTEGER,  -- Number of sale transactions
    net_shares_purchased_sold BIGINT,  -- Net shares purchased (sold)
    net_shares_purchased_sold_transactions INTEGER,  -- Number of net shares purchased (sold) transactions
    total_insider_shares_held BIGINT,  -- Total insider shares held
    percent_net_shares_purchased_sold NUMERIC,  -- Percentage of net shares purchased (sold)
    percent_buy_shares NUMERIC,  -- Percentage of buy shares
    percent_sell_shares NUMERIC  -- Percentage of sell shares
);
