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
