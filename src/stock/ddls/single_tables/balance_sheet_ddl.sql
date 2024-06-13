CREATE TABLE balance_sheet (
    ticker_id INTEGER REFERENCES ticker(id),  -- Foreign key to ticker table
    date DATE,  -- Date when the information was recorded
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
    cash_financial BIGINT,  -- Cash financial
    PRIMARY KEY (ticker_id, date)  -- Composite primary key to ensure uniqueness
);
