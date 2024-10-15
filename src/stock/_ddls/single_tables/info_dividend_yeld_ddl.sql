CREATE TABLE info_dividend_yield
(
    id                               SERIAL PRIMARY KEY,             -- Unique identifier for each record
    ticker_id                        INTEGER REFERENCES ticker (id), -- Foreign key to ticker table
    date                             TIMESTAMP,                      -- Timestamp to track when the data was recorded

    -- Dividend and Yield Information
    regular_market_open              NUMERIC(10, 4),                 -- Regular market open
    dividend_rate                    NUMERIC(10, 4),                 -- Dividend rate
    dividend_yield                   NUMERIC(10, 4),                 -- Dividend yield
    ex_dividend_date                 DATE,                           -- Ex-dividend date
    payout_ratio                     NUMERIC(10, 4),                 -- Payout ratio
    five_year_avg_dividend_yield     NUMERIC(10, 4),                 -- Five year average dividend yield
    beta                             NUMERIC(10, 4),                 -- Beta
    trailing_pe                      NUMERIC(10, 4),                 -- Trailing PE ratio
    forward_pe                       NUMERIC(10, 4),                 -- Forward PE ratio
    volume                           BIGINT,                         -- Volume
    average_volume                   BIGINT,                         -- Average volume
    average_volume_10days            BIGINT,                         -- Average volume over 10 days
    average_daily_volume_10day       BIGINT,                         -- Average daily volume over 10 days
    bid                              NUMERIC(10, 4),                 -- Bid price
    ask                              NUMERIC(10, 4),                 -- Ask price
    bid_size                         INTEGER,                        -- Bid size
    ask_size                         INTEGER,                        -- Ask size
    price_to_sales_trailing_12months NUMERIC(10, 4),                 -- Price to sales ratio trailing 12 months
    trailing_annual_dividend_rate    NUMERIC(10, 4),                 -- Trailing annual dividend rate
    trailing_annual_dividend_yield   NUMERIC(10, 4),                 -- Trailing annual dividend yield
    enterprise_value                 BIGINT,                         -- Enterprise value
    profit_margins                   NUMERIC(10, 4),                 -- Profit margins
    float_shares                     BIGINT,                         -- Float shares
    shares_outstanding               BIGINT,                         -- Shares outstanding
    shares_short                     BIGINT,                         -- Shares short
    shares_short_prior_month         BIGINT,                         -- Shares short prior month
    shares_short_previous_month_date DATE,                           -- Shares short previous month date
    date_short_interest              DATE,                           -- Date short interest
    shares_percent_shares_out        NUMERIC(10, 4),                 -- Shares percent shares out
    held_percent_insiders            NUMERIC(10, 4),                 -- Held percent insiders
    held_percent_institutions        NUMERIC(10, 4),                 -- Held percent institutions
    short_ratio                      NUMERIC(10, 4),                 -- Short ratio
    short_percent_of_float           NUMERIC(10, 4),                 -- Short percent of float
    implied_shares_outstanding       BIGINT,                         -- Implied shares outstanding
    book_value                       NUMERIC(10, 4),                 -- Book value
    price_to_book                    NUMERIC(10, 4),                 -- Price to book
    last_fiscal_year_end             DATE,                           -- Last fiscal year end
    next_fiscal_year_end             DATE,                           -- Next fiscal year end
    most_recent_quarter              DATE,                           -- Most recent quarter
    earnings_quarterly_growth        NUMERIC(10, 4),                 -- Earnings quarterly growth
    net_income_to_common             BIGINT,                         -- Net income to common
    trailing_eps                     NUMERIC(10, 4),                 -- Trailing EPS
    forward_eps                      NUMERIC(10, 4),                 -- Forward EPS
    peg_ratio                        NUMERIC(10, 4),                 -- PEG ratio
    last_split_factor                VARCHAR(10),                    -- Last split factor
    last_split_date                  DATE,                           -- Last split date
    enterprise_to_revenue            NUMERIC(10, 4),                 -- Enterprise to revenue
    enterprise_to_ebitda             NUMERIC(10, 4),                 -- Enterprise to EBITDA
    fifty_two_week_change            NUMERIC(10, 4),                 -- Fifty-two week change
    sp_fifty_two_week_change         NUMERIC(10, 4),                 -- S&P fifty-two week change
    last_dividend_value              NUMERIC(10, 4),                 -- Last dividend value
    last_dividend_date               DATE                            -- Last dividend date
);
