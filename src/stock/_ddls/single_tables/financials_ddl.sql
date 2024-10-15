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