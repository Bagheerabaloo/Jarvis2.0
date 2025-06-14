-- Table to store basic information about each ticker
CREATE TABLE ticker (
    id SERIAL PRIMARY KEY,  -- Unique identifier for each ticker
    symbol VARCHAR(10) UNIQUE NOT NULL,  -- Ticker symbol (e.g., AAPL)
    company_name TEXT,  -- Full company name
    business_summary TEXT  -- Business summary description
);
-- Table to store actions related to each ticker, with historical tracking
CREATE TABLE actions (
    id SERIAL PRIMARY KEY,  -- Unique identifier for each action record
    ticker_id INTEGER REFERENCES ticker(id),  -- Foreign key to ticker table
    date TIMESTAMP,  -- Date of the action
    dividends NUMERIC,  -- Dividend amount
    stock_splits NUMERIC  -- Stock split ratio
);
CREATE TABLE balance_sheet (
    id SERIAL PRIMARY KEY,  -- Unique identifier for each balance sheet record
    ticker_id INTEGER REFERENCES ticker(id),  -- Foreign key to ticker table
    date TIMESTAMP,  -- Date when the information was recorded
    period_type VARCHAR(10),  -- Period type (e.g., "Q1", "Q2", "Q3", "Q4") or annual
    treasury_shares_number BIGINT,  -- Number of treasury shares
    ordinary_shares_number BIGINT,  -- Number of ordinary shares
    share_issued BIGINT,  -- Number of shares issued
    net_debt BIGINT,  -- Net debt
    total_debt BIGINT,  -- Total debt
    tangible_book_value BIGINT,  -- Tangible book value
    invested_capital BIGINT,  -- Invested capital
    working_capital BIGINT,  -- Working capital
    net_tangible_assets BIGINT,  -- Net tangible assets
    common_stock_equity BIGINT,  -- Common stock equity
    total_capitalization BIGINT,  -- Total capitalization
    total_equity_gross_minority_interest BIGINT,  -- Total equity including gross minority interest
    stockholders_equity BIGINT,  -- Stockholders' equity
    gains_losses_not_affecting_retained_earnings BIGINT,  -- Gains/losses not affecting retained earnings
    other_equity_adjustments BIGINT,  -- Other equity adjustments
    retained_earnings BIGINT,  -- Retained earnings
    capital_stock BIGINT,  -- Capital stock
    common_stock BIGINT,  -- Common stock
    total_liabilities_net_minority_interest BIGINT,  -- Total liabilities net of minority interest
    total_non_current_liabilities_net_minority_interest BIGINT,  -- Total non-current liabilities net of minority interest
    other_non_current_liabilities BIGINT,  -- Other non-current liabilities
    trade_and_other_payables_non_current BIGINT,  -- Trade and other payables (non-current)
    long_term_debt_and_capital_lease_obligation BIGINT,  -- Long-term debt and capital lease obligation
    long_term_debt BIGINT,  -- Long-term debt
    current_liabilities BIGINT,  -- Current liabilities
    other_current_liabilities BIGINT,  -- Other current liabilities
    current_deferred_liabilities BIGINT,  -- Current deferred liabilities
    current_deferred_revenue BIGINT,  -- Current deferred revenue
    current_debt_and_capital_lease_obligation BIGINT,  -- Current debt and capital lease obligation
    current_debt BIGINT,  -- Current debt
    other_current_borrowings BIGINT,  -- Other current borrowings
    commercial_paper BIGINT,  -- Commercial paper
    payables_and_accrued_expenses BIGINT,  -- Payables and accrued expenses
    payables BIGINT,  -- Payables
    accounts_payable BIGINT,  -- Accounts payable
    total_assets BIGINT,  -- Total assets
    total_non_current_assets BIGINT,  -- Total non-current assets
    other_non_current_assets BIGINT,  -- Other non-current assets
    non_current_deferred_assets BIGINT,  -- Non-current deferred assets
    non_current_deferred_taxes_assets BIGINT,  -- Non-current deferred taxes assets
    investments_and_advances BIGINT,  -- Investments and advances
    other_investments BIGINT,  -- Other investments
    investment_in_financial_assets BIGINT,  -- Investment in financial assets
    available_for_sale_securities BIGINT,  -- Available for sale securities
    net_ppe BIGINT,  -- Net PPE (Property, Plant, and Equipment)
    accumulated_depreciation BIGINT,  -- Accumulated depreciation
    gross_ppe BIGINT,  -- Gross PPE
    leases BIGINT,  -- Leases
    machinery_furniture_equipment BIGINT,  -- Machinery, furniture, equipment
    land_and_improvements BIGINT,  -- Land and improvements
    properties BIGINT,  -- Properties
    current_assets BIGINT,  -- Current assets
    other_current_assets BIGINT,  -- Other current assets
    inventory BIGINT,  -- Inventory
    finished_goods BIGINT,  -- Finished goods
    raw_materials BIGINT,  -- Raw materials
    receivables BIGINT,  -- Receivables
    other_receivables BIGINT,  -- Other receivables
    accounts_receivable BIGINT,  -- Accounts receivable
    cash_cash_equivalents_and_short_term_investments BIGINT,  -- Cash, cash equivalents, and short-term investments
    other_short_term_investments BIGINT,  -- Other short-term investments
    cash_and_cash_equivalents BIGINT,  -- Cash and cash equivalents
    cash_equivalents BIGINT,  -- Cash equivalents
    cash_financial BIGINT  -- Cash financial
);

CREATE TABLE cash_flow (
    id SERIAL PRIMARY KEY,  -- Unique identifier for each cash flow record
    ticker_id INTEGER REFERENCES ticker(id),  -- Foreign key to ticker table
    date TIMESTAMP,  -- Date when the information was recorded
    period_type VARCHAR(10),  -- Period type (e.g., "FY", "Q1", "Q2", "Q3", "Q4") or annual
    free_cash_flow NUMERIC,  -- Free cash flow
    repurchase_of_capital_stock NUMERIC,  -- Repurchase of capital stock
    repayment_of_debt NUMERIC,  -- Repayment of debt
    issuance_of_debt NUMERIC,  -- Issuance of debt
    issuance_of_capital_stock NUMERIC,  -- Issuance of capital stock
    capital_expenditure NUMERIC,  -- Capital expenditure
    interest_paid_supplemental_data NUMERIC,  -- Interest paid (supplemental data)
    income_tax_paid_supplemental_data NUMERIC,  -- Income tax paid (supplemental data)
    end_cash_position NUMERIC,  -- End cash position
    beginning_cash_position NUMERIC,  -- Beginning cash position
    changes_in_cash NUMERIC,  -- Changes in cash
    financing_cash_flow NUMERIC,  -- Financing cash flow
    cash_flow_from_continuing_financing_activities NUMERIC,  -- Cash flow from continuing financing activities
    net_other_financing_charges NUMERIC,  -- Net other financing charges
    cash_dividends_paid NUMERIC,  -- Cash dividends paid
    common_stock_dividend_paid NUMERIC,  -- Common stock dividend paid
    net_common_stock_issuance NUMERIC,  -- Net common stock issuance
    common_stock_payments NUMERIC,  -- Common stock payments
    common_stock_issuance NUMERIC,  -- Common stock issuance
    net_issuance_payments_of_debt NUMERIC,  -- Net issuance/payments of debt
    net_short_term_debt_issuance NUMERIC,  -- Net short term debt issuance
    net_long_term_debt_issuance NUMERIC,  -- Net long term debt issuance
    long_term_debt_payments NUMERIC,  -- Long term debt payments
    long_term_debt_issuance NUMERIC,  -- Long term debt issuance
    investing_cash_flow NUMERIC,  -- Investing cash flow
    cash_flow_from_continuing_investing_activities NUMERIC,  -- Cash flow from continuing investing activities
    net_other_investing_changes NUMERIC,  -- Net other investing changes
    net_investment_purchase_and_sale NUMERIC,  -- Net investment purchase and sale
    sale_of_investment NUMERIC,  -- Sale of investment
    purchase_of_investment NUMERIC,  -- Purchase of investment
    net_business_purchase_and_sale NUMERIC,  -- Net business purchase and sale
    purchase_of_business NUMERIC,  -- Purchase of business
    net_ppe_purchase_and_sale NUMERIC,  -- Net PPE purchase and sale
    purchase_of_ppe NUMERIC,  -- Purchase of PPE
    operating_cash_flow NUMERIC,  -- Operating cash flow
    cash_flow_from_continuing_operating_activities NUMERIC,  -- Cash flow from continuing operating activities
    change_in_working_capital NUMERIC,  -- Change in working capital
    change_in_other_working_capital NUMERIC,  -- Change in other working capital
    change_in_other_current_liabilities NUMERIC,  -- Change in other current liabilities
    change_in_other_current_assets NUMERIC,  -- Change in other current assets
    change_in_payables_and_accrued_expense NUMERIC,  -- Change in payables and accrued expense
    change_in_payable NUMERIC,  -- Change in payable
    change_in_account_payable NUMERIC,  -- Change in account payable
    change_in_inventory NUMERIC,  -- Change in inventory
    change_in_receivables NUMERIC,  -- Change in receivables
    changes_in_account_receivables NUMERIC,  -- Changes in account receivables
    other_non_cash_items NUMERIC,  -- Other non-cash items
    stock_based_compensation NUMERIC,  -- Stock based compensation
    deferred_tax NUMERIC,  -- Deferred tax
    deferred_income_tax NUMERIC,  -- Deferred income tax
    depreciation_amortization_depletion NUMERIC,  -- Depreciation, amortization, depletion
    depreciation_and_amortization NUMERIC,  -- Depreciation and amortization
    net_income_from_continuing_operations NUMERIC  -- Net income from continuing operations
);

-- Table to store earnings dates for each ticker
CREATE TABLE earnings_dates (
    id SERIAL PRIMARY KEY,  -- Unique identifier for each earnings date
    ticker_id INTEGER REFERENCES ticker(id),  -- Foreign key to ticker table
    date DATE,  -- Date of the earnings report
    earnings_period VARCHAR(20),  -- Earnings period
    eps_estimate NUMERIC,  -- EPS estimate
    reported_eps NUMERIC,  -- Reported EPS
    surprise_percent NUMERIC  -- Earnings surprise percentage
);
CREATE TABLE financials (
    id SERIAL PRIMARY KEY,  -- Unique identifier for each record
    ticker_id INTEGER REFERENCES ticker(id),  -- Foreign key to ticker table
    date TIMESTAMP,  -- Date when the information was recorded
    period_type VARCHAR(10),  -- Period type (e.g., "FY", "Q1")
    tax_effect_of_unusual_items NUMERIC,  -- Tax effect of unusual items
    tax_rate_for_calcs NUMERIC,  -- Tax rate for calculations
    normalized_ebitda NUMERIC,  -- Normalized EBITDA
    net_income_from_continuing_operation_net_minority_interest NUMERIC,  -- Net income from continuing operations, net of minority interest
    reconciled_depreciation NUMERIC,  -- Reconciled depreciation
    reconciled_cost_of_revenue NUMERIC,  -- Reconciled cost of revenue
    ebitda NUMERIC,  -- Earnings before interest, taxes, depreciation, and amortization
    ebit NUMERIC,  -- Earnings before interest and taxes
    net_interest_income NUMERIC,  -- Net interest income
    interest_expense NUMERIC,  -- Interest expense
    interest_income NUMERIC,  -- Interest income
    normalized_income NUMERIC,  -- Normalized income
    net_income_from_continuing_and_discontinued_operation NUMERIC,  -- Net income from continuing and discontinued operations
    total_expenses NUMERIC,  -- Total expenses
    total_operating_income_as_reported NUMERIC,  -- Total operating income as reported
    diluted_average_shares NUMERIC,  -- Diluted average shares
    basic_average_shares NUMERIC,  -- Basic average shares
    diluted_eps NUMERIC,  -- Diluted earnings per share
    basic_eps NUMERIC,  -- Basic earnings per share
    diluted_ni_availto_com_stockholders NUMERIC,  -- Diluted net income available to common stockholders
    net_income_common_stockholders NUMERIC,  -- Net income available to common stockholders
    net_income NUMERIC,  -- Net income
    net_income_including_noncontrolling_interests NUMERIC,  -- Net income including noncontrolling interests
    net_income_continuous_operations NUMERIC,  -- Net income from continuous operations
    tax_provision NUMERIC,  -- Tax provision
    pretax_income NUMERIC,  -- Pretax income
    other_income_expense NUMERIC,  -- Other income/expense
    other_non_operating_income_expenses NUMERIC,  -- Other non-operating income/expenses
    net_non_operating_interest_income_expense NUMERIC,  -- Net non-operating interest income/expense
    interest_expense_non_operating NUMERIC,  -- Interest expense (non-operating)
    interest_income_non_operating NUMERIC,  -- Interest income (non-operating)
    operating_income NUMERIC,  -- Operating income
    operating_expense NUMERIC,  -- Operating expense
    research_and_development NUMERIC,  -- Research and development
    selling_general_and_administration NUMERIC,  -- Selling, general, and administration
    gross_profit NUMERIC,  -- Gross profit
    cost_of_revenue NUMERIC,  -- Cost of revenue
    total_revenue NUMERIC,  -- Total revenue
    operating_revenue NUMERIC  -- Operating revenue
);
-- Table to store static additional information about each company
CREATE TABLE info_additional (
    id SERIAL PRIMARY KEY,  -- Unique identifier for each record
    ticker_id INTEGER REFERENCES ticker(id),  -- Foreign key to link with the ticker table
    underlying_symbol VARCHAR(10),  -- Underlying symbol
    short_name VARCHAR(50),  -- Short name
    long_name VARCHAR(100),  -- Long name
    first_trade_date_epoch_utc BIGINT,  -- First trade date in epoch UTC
    time_zone_full_name VARCHAR(50),  -- Full name of the time zone
    time_zone_short_name VARCHAR(10),  -- Short name of the time zone
    uuid UUID,  -- UUID for the record
    message_board_id VARCHAR(50),  -- Message board ID
    gmt_offset_milliseconds BIGINT,  -- GMT offset in milliseconds

    -- IR Information
    ir_website VARCHAR(100),  -- IR website
    max_age INTEGER,  -- Max age

    -- Company Description and Employees
    full_time_employees INTEGER  -- Full-time employees
);

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

-- Table to store historical changes in sector and industry for each company
CREATE TABLE info_company_sector_industry_history (
    id SERIAL PRIMARY KEY,  -- Unique identifier for each record
    ticker_id INTEGER REFERENCES ticker(id),  -- Foreign key to link with the ticker table
    sector VARCHAR(100),  -- Sector of the company
    industry VARCHAR(100),  -- Industry of the company
    start_date DATE,  -- Start date of the sector and industry validity
    end_date DATE  -- End date of the sector and industry validity (optional, NULL if currently valid)
);
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
CREATE TABLE info_dividend_earnings (
    id SERIAL PRIMARY KEY,  -- Unique identifier for each record
    ticker_id INTEGER REFERENCES ticker(id),  -- Foreign key to ticker table
    date TIMESTAMP,  -- Timestamp to track when the data was recorded
    dividend_date DATE,  -- Dividend date
    earnings_high NUMERIC(10, 4),  -- Earnings high estimate
    earnings_low NUMERIC(10, 4),  -- Earnings low estimate
    earnings_average NUMERIC(10, 4),  -- Earnings average estimate
    revenue_high BIGINT,  -- Revenue high estimate
    revenue_low BIGINT,  -- Revenue low estimate
    revenue_average BIGINT  -- Revenue average estimate
);

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

CREATE TABLE info_exchange_trading (
    id SERIAL PRIMARY KEY,  -- Unique identifier for each record
    ticker_id INTEGER REFERENCES ticker(id),  -- Foreign key to ticker table
    date TIMESTAMP,  -- Timestamp to track when the data was recorded
    symbol VARCHAR(10),  -- Stock symbol
    exchange_name VARCHAR(50),  -- Exchange name
    full_exchange_name VARCHAR(50),  -- Full exchange name
    instrument_type VARCHAR(20),  -- Instrument type
    first_trade_date BIGINT,  -- First trade date
    regular_market_time BIGINT,  -- Regular market time
    has_pre_post_market_data BOOLEAN,  -- Pre and post market data availability
    gmt_offset INTEGER,  -- GMT offset
    exchange_timezone_name VARCHAR(50)  -- Exchange timezone name
);

CREATE TABLE info_general_stock (
    id SERIAL PRIMARY KEY,  -- Unique identifier for each record
    ticker_id INTEGER REFERENCES ticker(id),  -- Foreign key to ticker table
    date TIMESTAMP,  -- Timestamp to track when the data was recorded
    isin VARCHAR(20),  -- ISIN
    currency VARCHAR(10),  -- Currency used for stock values
    day_high NUMERIC(10, 4),  -- Day's high price
    day_low NUMERIC(10, 4),  -- Day's low price
    exchange VARCHAR(10),  -- Stock exchange
    fifty_day_average NUMERIC(10, 4),  -- Fifty day average price
    last_price NUMERIC(10, 4),  -- Last traded price
    last_volume BIGINT,  -- Last traded volume
    market_cap BIGINT,  -- Market capitalization
    open NUMERIC(10, 4),  -- Opening price
    previous_close NUMERIC(10, 4),  -- Previous closing price
    quote_type VARCHAR(20),  -- Quote type
    regular_market_previous_close NUMERIC(10, 4),  -- Regular market previous close
    shares BIGINT,  -- Shares outstanding
    ten_day_average_volume BIGINT,  -- Ten day average volume
    three_month_average_volume BIGINT,  -- Three month average volume
    timezone VARCHAR(10),  -- Timezone
    two_hundred_day_average NUMERIC(10, 4),  -- Two hundred day average price
    year_change NUMERIC(10, 4),  -- Yearly change
    year_high NUMERIC(10, 4),  -- Yearly high
    year_low NUMERIC(10, 4)  -- Yearly low
);
CREATE TABLE info_governance (
    id SERIAL PRIMARY KEY,  -- Unique identifier for each record
    ticker_id INTEGER REFERENCES ticker(id),  -- Foreign key to ticker table
    date TIMESTAMP,  -- Timestamp to track when the data was recorded
    audit_risk INTEGER,  -- Audit risk
    board_risk INTEGER,  -- Board risk
    compensation_risk INTEGER,  -- Compensation risk
    shareholder_rights_risk INTEGER,  -- Shareholder rights risk
    overall_risk INTEGER,  -- Overall risk
    governance_epoch_date DATE,  -- Governance epoch date
    compensation_as_of_epoch_date DATE  -- Compensation as of epoch date
);

CREATE TABLE info_regular_market (
    id SERIAL PRIMARY KEY,  -- Unique identifier for each record
    ticker_id INTEGER REFERENCES ticker(id),  -- Foreign key to ticker table
    date TIMESTAMP,  -- Timestamp to track when the data was recorded
    regular_market_price NUMERIC(10, 4),  -- Regular market price
    fifty_two_week_high NUMERIC(10, 4),  -- Fifty-two week high
    fifty_two_week_low NUMERIC(10, 4),  -- Fifty-two week low
    regular_market_day_high NUMERIC(10, 4),  -- Regular market day high
    regular_market_day_low NUMERIC(10, 4),  -- Regular market day low
    regular_market_volume BIGINT,  -- Regular market volume
    chart_previous_close NUMERIC(10, 4),  -- Chart previous close
    scale INTEGER,  -- Scale
    price_hint INTEGER,  -- Price hint
    data_granularity VARCHAR(10),  -- Data granularity
    range VARCHAR(20)  -- Range
);

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

CREATE TABLE institutional_holders (
    id SERIAL PRIMARY KEY,  -- Unique identifier for each institutional holder record
    ticker_id INTEGER REFERENCES ticker(id),  -- Foreign key to ticker table
    date_reported TIMESTAMP,  -- Date when the information was reported
    holder VARCHAR(255),  -- Name of the institutional holder
    pct_held NUMERIC,  -- Percentage of shares held
    shares BIGINT,  -- Number of shares held
    value NUMERIC  -- Value of the shares held
);

CREATE TABLE major_holders (
    id SERIAL PRIMARY KEY,  -- Unique identifier for each major holder record
    ticker_id INTEGER REFERENCES ticker(id),  -- Foreign key to ticker table
    date TIMESTAMP,  -- Date when the information was recorded
    insiders_percent_held NUMERIC,  -- Percentage of shares held by insiders
    institutions_percent_held NUMERIC,  -- Percentage of shares held by institutions
    institutions_float_percent_held NUMERIC,  -- Percentage of float shares held by institutions
    institutions_count BIGINT  -- Number of institutions holding shares
);

CREATE TABLE mutualfund_holders (
    id SERIAL PRIMARY KEY,  -- Unique identifier for each mutual fund holder record
    ticker_id INTEGER REFERENCES ticker(id),  -- Foreign key to ticker table
    date_reported TIMESTAMP,  -- Date when the information was reported
    holder VARCHAR(255),  -- Name of the mutual fund holder
    pct_held NUMERIC,  -- Percentage of shares held
    shares BIGINT,  -- Number of shares held
    value NUMERIC  -- Value of the shares held
);

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

CREATE TABLE upgrades_downgrades (
    id SERIAL PRIMARY KEY,  -- Unique identifier for each record
    ticker_id INTEGER REFERENCES ticker(id),  -- Foreign key to ticker table
    date TIMESTAMP,  -- Date when the upgrade/downgrade occurred
    firm VARCHAR(255),  -- Name of the firm issuing the upgrade/downgrade
    to_grade VARCHAR(50),  -- New grade assigned by the firm
    from_grade VARCHAR(50),  -- Previous grade assigned by the firm
    action VARCHAR(50)  -- Action taken (e.g., "main", "reit", "up", "init")
);

