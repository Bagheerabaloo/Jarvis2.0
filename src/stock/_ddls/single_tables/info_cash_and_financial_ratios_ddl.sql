CREATE TABLE info_cash_and_financial_ratios (
    id SERIAL PRIMARY KEY,  -- Unique identifier for each record
    ticker_id INTEGER REFERENCES ticker(id),  -- Foreign key to ticker table
    date TIMESTAMP,  -- Timestamp to track when the data was recorded
    total_cash BIGINT,  -- Total cash
    total_cash_per_share NUMERIC(10, 4),  -- Total cash per share
    ebitda BIGINT,  -- EBITDA
    total_debt BIGINT,  -- Total debt
    quick_ratio NUMERIC(10, 4),  -- Quick ratio
    current_ratio NUMERIC(10, 4),  -- Current ratio
    total_revenue BIGINT,  -- Total revenue
    debt_to_equity NUMERIC(10, 4),  -- Debt to equity ratio
    revenue_per_share NUMERIC(10, 4),  -- Revenue per share
    return_on_assets NUMERIC(10, 4),  -- Return on assets
    return_on_equity NUMERIC(10, 4),  -- Return on equity
    free_cashflow BIGINT,  -- Free cash flow
    operating_cashflow BIGINT,  -- Operating cash flow
    earnings_growth NUMERIC(10, 4),  -- Earnings growth
    revenue_growth NUMERIC(10, 4),  -- Revenue growth
    gross_margins NUMERIC(10, 4),  -- Gross margins
    ebitda_margins NUMERIC(10, 4),  -- EBITDA margins
    operating_margins NUMERIC(10, 4),  -- Operating margins
    trailing_peg_ratio NUMERIC(10, 4)  -- Trailing PEG ratio
);
