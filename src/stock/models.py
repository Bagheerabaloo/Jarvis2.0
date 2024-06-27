from sqlalchemy import Column, Integer, String, Text, Boolean, Numeric, ForeignKey, Index, UniqueConstraint
from sqlalchemy import Float, TIMESTAMP, BigInteger, Date, DateTime, UUID as SA_UUID
from sqlalchemy.orm import relationship
from src.stock.database import Base


class Ticker(Base):
    __tablename__ = 'ticker'

    id = Column(Integer, primary_key=True, autoincrement=True)  # Unique identifier for each ticker
    symbol = Column(String(10), unique=True, nullable=False)  # Ticker symbol (e.g., AAPL)
    company_name = Column(Text)  # Full company name
    business_summary = Column(Text)  # Business summary description

    # Relationships
    actions = relationship("Action", back_populates="ticker", cascade="all, delete-orphan")
    balance_sheet = relationship("BalanceSheet", back_populates="ticker", cascade="all, delete-orphan")
    calendar = relationship("Calendar", back_populates="ticker", cascade="all, delete-orphan")
    cash_flow = relationship("CashFlow", back_populates="ticker", cascade="all, delete-orphan")
    earnings_dates = relationship("EarningsDates", back_populates="ticker", cascade="all, delete-orphan")
    financials = relationship("Financials", back_populates="ticker", cascade="all, delete-orphan")
    info_cash_and_financial_ratios = relationship("InfoCashAndFinancialRatios", back_populates="ticker", cascade="all, delete-orphan")
    info_company_address = relationship("InfoCompanyAddress", back_populates="ticker", cascade="all, delete-orphan")
    info_sector_industry_history = relationship("InfoSectorIndustryHistory", back_populates="ticker", cascade="all, delete-orphan")
    info_target_price_and_recommendation = relationship("InfoTargetPriceAndRecommendation", back_populates="ticker", cascade="all, delete-orphan")
    info_market_and_financial_metrics = relationship("InfoMarketAndFinancialMetrics", back_populates="ticker", cascade="all, delete-orphan")
    info_general_stock = relationship("InfoGeneralStock", back_populates="ticker", cascade="all, delete-orphan")
    info_governance = relationship("InfoGovernance", back_populates="ticker", cascade="all, delete-orphan")
    info_trading_session = relationship("InfoTradingSession", back_populates="ticker", cascade="all, delete-orphan")
    insider_purchases = relationship("InsiderPurchases", back_populates="ticker", cascade="all, delete-orphan")
    insider_roster_holders = relationship("InsiderRosterHolders", back_populates="ticker", cascade="all, delete-orphan")
    insider_transactions = relationship("InsiderTransactions", back_populates="ticker", cascade="all, delete-orphan")
    institutional_holders = relationship("InstitutionalHolders", back_populates="ticker", cascade="all, delete-orphan")
    major_holders = relationship("MajorHolders", back_populates="ticker", cascade="all, delete-orphan")
    mutualfund_holders = relationship("MutualFundHolders", back_populates="ticker", cascade="all, delete-orphan")
    recommendations = relationship("Recommendations", back_populates="ticker", cascade="all, delete-orphan")
    upgrades_downgrades = relationship("UpgradesDowngrades", back_populates="ticker", cascade="all, delete-orphan")

    # __ relationships with the candle data tables __
    candle_data_day = relationship("CandleDataDay", back_populates="ticker", cascade="all, delete-orphan")
    candle_data_week = relationship("CandleDataWeek", back_populates="ticker", cascade="all, delete-orphan")
    candle_data_month = relationship("CandleDataMonth", back_populates="ticker", cascade="all, delete-orphan")
    candle_data_1_hour = relationship("CandleData1Hour", back_populates="ticker", cascade="all, delete-orphan")
    candle_data_5_minutes = relationship("CandleData5Minutes", back_populates="ticker", cascade="all, delete-orphan")
    candle_data_1_minute = relationship("CandleData1Minute", back_populates="ticker", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Ticker(id={self.id}, symbol={self.symbol}, company_name={self.company_name})>"


class BalanceSheet(Base):
    __tablename__ = 'balance_sheet'

    ticker_id = Column(Integer, ForeignKey('ticker.id'), primary_key=True)  # Foreign key reference to ticker table
    date = Column(Date, primary_key=True)  # Composite primary key to ensure uniqueness
    period_type = Column(String(10), primary_key=True)  # Period type (e.g., "Q1", "Q2", "Q3", "Q4") or annual

    treasury_shares_number = Column(Numeric)  # Number of treasury shares
    ordinary_shares_number = Column(Numeric)  # Number of ordinary shares
    share_issued = Column(Numeric)  # Number of shares issued
    net_debt = Column(Numeric)  # Net debt
    total_debt = Column(Numeric)  # Total debt
    tangible_book_value = Column(Numeric)  # Tangible book value
    invested_capital = Column(Numeric)  # Invested capital
    working_capital = Column(Numeric)  # Working capital
    net_tangible_assets = Column(Numeric)  # Net tangible assets
    common_stock_equity = Column(Numeric)  # Common stock equity
    total_capitalization = Column(Numeric)  # Total capitalization
    total_equity_gross_minority_interest = Column(Numeric)  # Total equity including gross minority interest
    stockholders_equity = Column(Numeric)  # Stockholders' equity
    gains_losses_not_affecting_retained_earnings = Column(Numeric)  # Gains/losses not affecting retained earnings
    other_equity_adjustments = Column(Numeric)  # Other equity adjustments
    retained_earnings = Column(Numeric)  # Retained earnings
    capital_stock = Column(Numeric)  # Capital stock
    common_stock = Column(Numeric)  # Common stock
    total_liabilities_net_minority_interest = Column(Numeric)  # Total liabilities net of minority interest
    total_non_current_liabilities_net_minority_interest = Column(Numeric)  # Total non-current liabilities net of minority interest
    other_non_current_liabilities = Column(Numeric)  # Other non-current liabilities
    trade_and_other_payables_non_current = Column(Numeric)  # Trade and other payables (non-current)
    long_term_debt_and_capital_lease_obligation = Column(Numeric)  # Long-term debt and capital lease obligation
    long_term_debt = Column(Numeric)  # Long-term debt
    current_liabilities = Column(Numeric)  # Current liabilities
    other_current_liabilities = Column(Numeric)  # Other current liabilities
    current_deferred_liabilities = Column(Numeric)  # Current deferred liabilities
    current_deferred_revenue = Column(Numeric)  # Current deferred revenue
    current_debt_and_capital_lease_obligation = Column(Numeric)  # Current debt and capital lease obligation
    current_debt = Column(Numeric)  # Current debt
    other_current_borrowings = Column(Numeric)  # Other current borrowings
    commercial_paper = Column(Numeric)  # Commercial paper
    payables_and_accrued_expenses = Column(Numeric)  # Payables and accrued expenses
    payables = Column(Numeric)  # Payables
    accounts_payable = Column(Numeric)  # Accounts payable
    total_assets = Column(Numeric)  # Total assets
    total_non_current_assets = Column(Numeric)  # Total non-current assets
    other_non_current_assets = Column(Numeric)  # Other non-current assets
    non_current_deferred_assets = Column(Numeric)  # Non-current deferred assets
    non_current_deferred_taxes_assets = Column(Numeric)  # Non-current deferred taxes assets
    investments_and_advances = Column(Numeric)  # Investments and advances
    other_investments = Column(Numeric)  # Other investments
    investment_in_financial_assets = Column(Numeric)  # Investment in financial assets
    available_for_sale_securities = Column(Numeric)  # Available for sale securities
    net_ppe = Column(Numeric)  # Net PPE (Property, Plant, and Equipment)
    accumulated_depreciation = Column(Numeric)  # Accumulated depreciation
    gross_ppe = Column(Numeric)  # Gross PPE
    leases = Column(Numeric)  # Leases
    machinery_furniture_equipment = Column(Numeric)  # Machinery, furniture, equipment
    land_and_improvements = Column(Numeric)  # Land and improvements
    properties = Column(Numeric)  # Properties
    current_assets = Column(Numeric)  # Current assets
    other_current_assets = Column(Numeric)  # Other current assets
    inventory = Column(Numeric)  # Inventory
    # finished_goods = Column(Numeric)  # Finished goods
    # raw_materials = Column(Numeric)  # Raw materials
    receivables = Column(Numeric)  # Receivables
    other_receivables = Column(Numeric)  # Other receivables
    accounts_receivable = Column(Numeric)  # Accounts receivable
    cash_cash_equivalents_and_short_term_investments = Column(Numeric)  # Cash, cash equivalents, and short-term investments
    other_short_term_investments = Column(Numeric)  # Other short-term investments
    cash_and_cash_equivalents = Column(Numeric)  # Cash and cash equivalents
    cash_equivalents = Column(Numeric)  # Cash equivalents
    cash_financial = Column(Numeric)  # Cash financial

    # Relationship to access the parent Ticker object
    ticker = relationship("Ticker", back_populates="balance_sheet")

    def __repr__(self):
        return f"<BalanceSheet(ticker_id={self.ticker_id}, date={self.date}, period_type={self.period_type})>"


class CashFlow(Base):
    __tablename__ = 'cash_flow'

    ticker_id = Column(Integer, ForeignKey('ticker.id'), primary_key=True)  # Foreign key reference to ticker table
    date = Column(Date, primary_key=True)  # Date when the information was recorded
    period_type = Column(String(10), primary_key=True)  # Period type (e.g., "FY", "Q1", "Q2", "Q3", "Q4") or annual

    free_cash_flow = Column(Numeric)  # Free cash flow
    repurchase_of_capital_stock = Column(Numeric)  # Repurchase of capital stock
    repayment_of_debt = Column(Numeric)  # Repayment of debt
    issuance_of_debt = Column(Numeric)  # Issuance of debt
    issuance_of_capital_stock = Column(Numeric)  # Issuance of capital stock
    capital_expenditure = Column(Numeric)  # Capital expenditure
    interest_paid_supplemental_data = Column(Numeric)  # Interest paid (supplemental data)
    income_tax_paid_supplemental_data = Column(Numeric)  # Income tax paid (supplemental data)
    end_cash_position = Column(Numeric)  # End cash position
    beginning_cash_position = Column(Numeric)  # Beginning cash position
    changes_in_cash = Column(Numeric)  # Changes in cash
    financing_cash_flow = Column(Numeric)  # Financing cash flow
    cash_flow_from_continuing_financing_activities = Column(Numeric)  # Cash flow from continuing financing activities
    net_other_financing_charges = Column(Numeric)  # Net other financing charges
    cash_dividends_paid = Column(Numeric)  # Cash dividends paid
    common_stock_dividend_paid = Column(Numeric)  # Common stock dividend paid
    net_common_stock_issuance = Column(Numeric)  # Net common stock issuance
    common_stock_payments = Column(Numeric)  # Common stock payments
    common_stock_issuance = Column(Numeric)  # Common stock issuance
    net_issuance_payments_of_debt = Column(Numeric)  # Net issuance/payments of debt
    net_short_term_debt_issuance = Column(Numeric)  # Net short term debt issuance
    net_long_term_debt_issuance = Column(Numeric)  # Net long term debt issuance
    long_term_debt_payments = Column(Numeric)  # Long term debt payments
    long_term_debt_issuance = Column(Numeric)  # Long term debt issuance
    investing_cash_flow = Column(Numeric)  # Investing cash flow
    cash_flow_from_continuing_investing_activities = Column(Numeric)  # Cash flow from continuing investing activities
    net_other_investing_changes = Column(Numeric)  # Net other investing changes
    net_investment_purchase_and_sale = Column(Numeric)  # Net investment purchase and sale
    sale_of_investment = Column(Numeric)  # Sale of investment
    purchase_of_investment = Column(Numeric)  # Purchase of investment
    net_business_purchase_and_sale = Column(Numeric)  # Net business purchase and sale
    purchase_of_business = Column(Numeric)  # Purchase of business
    net_ppe_purchase_and_sale = Column(Numeric)  # Net PPE purchase and sale
    purchase_of_ppe = Column(Numeric)  # Purchase of PPE
    operating_cash_flow = Column(Numeric)  # Operating cash flow
    cash_flow_from_continuing_operating_activities = Column(Numeric)  # Cash flow from continuing operating activities
    change_in_working_capital = Column(Numeric)  # Change in working capital
    change_in_other_working_capital = Column(Numeric)  # Change in other working capital
    change_in_other_current_liabilities = Column(Numeric)  # Change in other current liabilities
    change_in_other_current_assets = Column(Numeric)  # Change in other current assets
    change_in_payables_and_accrued_expense = Column(Numeric)  # Change in payables and accrued expense
    change_in_payable = Column(Numeric)  # Change in payable
    change_in_account_payable = Column(Numeric)  # Change in account payable
    change_in_inventory = Column(Numeric)  # Change in inventory
    change_in_receivables = Column(Numeric)  # Change in receivables
    changes_in_account_receivables = Column(Numeric)  # Changes in account receivables
    other_non_cash_items = Column(Numeric)  # Other non-cash items
    stock_based_compensation = Column(Numeric)  # Stock based compensation
    deferred_tax = Column(Numeric)  # Deferred tax
    deferred_income_tax = Column(Numeric)  # Deferred income tax
    depreciation_amortization_depletion = Column(Numeric)  # Depreciation, amortization, depletion
    depreciation_and_amortization = Column(Numeric)  # Depreciation and amortization
    net_income_from_continuing_operations = Column(Numeric)  # Net income from continuing operations

    # Relationship to access the parent Ticker object
    ticker = relationship("Ticker", back_populates="cash_flow")

    def __repr__(self):
        return f"<CashFlow(ticker_id={self.ticker_id}, date={self.date}, period_type={self.period_type})>"


class Financials(Base):
    __tablename__ = 'financials'

    ticker_id = Column(Integer, ForeignKey('ticker.id'), nullable=False, primary_key=True)  # Foreign key reference to ticker table
    date = Column(Date, nullable=False, primary_key=True)  # Date when the information was recorded
    period_type = Column(String(10), nullable=False, primary_key=True)  # Period type (e.g., "FY", "Q1")

    tax_effect_of_unusual_items = Column(Numeric)  # Tax effect of unusual items
    tax_rate_for_calcs = Column(Numeric)  # Tax rate for calculations
    normalized_ebitda = Column(Numeric)  # Normalized EBITDA
    net_income_from_continuing_operation_net_minority_interest = Column(Numeric)  # Net income from continuing operations, net of minority interest
    reconciled_depreciation = Column(Numeric)  # Reconciled depreciation
    reconciled_cost_of_revenue = Column(Numeric)  # Reconciled cost of revenue
    ebitda = Column(Numeric)  # Earnings before interest, taxes, depreciation, and amortization
    ebit = Column(Numeric)  # Earnings before interest and taxes
    net_interest_income = Column(Numeric)  # Net interest income
    interest_expense = Column(Numeric)  # Interest expense
    interest_income = Column(Numeric)  # Interest income
    normalized_income = Column(Numeric)  # Normalized income
    net_income_from_continuing_and_discontinued_operation = Column(Numeric)  # Net income from continuing and discontinued operations
    total_expenses = Column(Numeric)  # Total expenses
    total_operating_income_as_reported = Column(Numeric)  # Total operating income as reported
    diluted_average_shares = Column(Numeric)  # Diluted average shares
    basic_average_shares = Column(Numeric)  # Basic average shares
    diluted_eps = Column(Numeric)  # Diluted earnings per share
    basic_eps = Column(Numeric)  # Basic earnings per share
    diluted_net_income_available_to_common_stockholders = Column(Numeric)  # Diluted net income available to common stockholders
    net_income_common_stockholders = Column(Numeric)  # Net income available to common stockholders
    net_income = Column(Numeric)  # Net income
    net_income_including_non_controlling_interests = Column(Numeric)  # Net income including non controlling interests
    net_income_continuous_operations = Column(Numeric)  # Net income from continuous operations
    tax_provision = Column(Numeric)  # Tax provision
    pretax_income = Column(Numeric)  # Pretax income
    other_income_expense = Column(Numeric)  # Other income/expense
    other_non_operating_income_expenses = Column(Numeric)  # Other non-operating income/expenses
    net_non_operating_interest_income_expense = Column(Numeric)  # Net non-operating interest income/expense
    interest_expense_non_operating = Column(Numeric)  # Interest expense (non-operating)
    interest_income_non_operating = Column(Numeric)  # Interest income (non-operating)
    operating_income = Column(Numeric)  # Operating income
    operating_expense = Column(Numeric)  # Operating expense
    research_and_development = Column(Numeric)  # Research and development
    selling_general_and_administration = Column(Numeric)  # Selling, general, and administration
    gross_profit = Column(Numeric)  # Gross profit
    cost_of_revenue = Column(Numeric)  # Cost of revenue
    total_revenue = Column(Numeric)  # Total revenue
    operating_revenue = Column(Numeric)  # Operating revenue

    ticker = relationship("Ticker", back_populates="financials")

    def __repr__(self):
        return f"<Financials(ticker_id={self.ticker_id}, date={self.date}, period_type={self.period_type})>"


class Action(Base):
    __tablename__ = 'actions'

    ticker_id = Column(Integer, ForeignKey('ticker.id'), nullable=False, primary_key=True)  # Foreign key to ticker table
    date = Column(Date, nullable=False, primary_key=True)                                   # Date of the action
    last_update = Column(DateTime, nullable=False, primary_key=True)                        # Timestamp to track when the data was recorded

    dividends = Column(Numeric, nullable=True)                                              # Dividend amount
    stock_splits = Column(Numeric, nullable=True)                                           # Stock split ratio

    # Relationship to access the parent Ticker object
    ticker = relationship("Ticker", back_populates="actions")

    def __repr__(self):
        return f"<Action(ticker_id={self.ticker_id}, date={self.date})>"


class Calendar(Base):
    __tablename__ = 'calendar'

    ticker_id = Column(Integer, ForeignKey('ticker.id'), nullable=False, primary_key=True)  # Foreign key reference to ticker table
    last_update = Column(DateTime, nullable=False, primary_key=True)  # Timestamp to track when the data was recorded

    # Dividend and Earnings Information
    dividend_date = Column(Date, nullable=True)  # Dividend date
    ex_dividend_date = Column(Date, nullable=True)  # Ex-dividend date
    # TODO: Add Earnings Date
    earnings_high = Column(Numeric(10, 4), nullable=True)  # Earnings high estimate
    earnings_low = Column(Numeric(10, 4), nullable=True)  # Earnings low estimate
    earnings_average = Column(Numeric(10, 4), nullable=True)  # Earnings average estimate
    revenue_high = Column(BigInteger, nullable=True)  # Revenue high estimate
    revenue_low = Column(BigInteger, nullable=True)  # Revenue low estimate
    revenue_average = Column(BigInteger, nullable=True)  # Revenue average estimate

    ticker = relationship("Ticker", back_populates="calendar")

    def __repr__(self):
        return f"<InfoDividendEarnings(ticker_id={self.ticker_id}, date={self.date})>"


class EarningsDates(Base):
    __tablename__ = 'earnings_dates'

    ticker_id = Column(Integer, ForeignKey('ticker.id'), primary_key=True)  # Foreign key reference to ticker table
    date = Column(Date, primary_key=True)                                   # Date of the earnings report
    last_update = Column(DateTime, nullable=False, primary_key=True)        # Timestamp to track when the data was recorded

    earnings_period = Column(String(20), nullable=True)                                    # Earnings period
    eps_estimate = Column(Numeric, nullable=True)                                          # EPS estimate
    reported_eps = Column(Numeric, nullable=True)                                          # Reported EPS
    surprise_percent = Column(Numeric, nullable=True)                                      # Earnings surprise percentage

    ticker = relationship("Ticker", back_populates="earnings_dates")

    def __repr__(self):
        return f"<EarningsDates(ticker_id={self.ticker_id}, date={self.date}, earnings_period={self.earnings_period})>"


class InfoCompanyAddress(Base):
    __tablename__ = 'info_company_address'

    ticker_id = Column(Integer, ForeignKey('ticker.id'), nullable=False, primary_key=True)  # Foreign key reference to ticker table
    last_update = Column(DateTime, nullable=False, primary_key=True)  # Timestamp to track when the data was recorded
    
    address1 = Column(String(200))                  # Address line 1
    city = Column(String(50))                       # City
    state = Column(String(50))                      # State
    zip = Column(String(20))                        # ZIP code
    country = Column(String(50))                    # Country
    phone = Column(String(20))                      # Phone number
    website = Column(String(200))                   # Website URL
    ir_website = Column(String(200))                # IR website

    ticker = relationship("Ticker", back_populates="info_company_address")

    def __repr__(self):
        return f"<InfoCompanyAddress(ticker_id={self.ticker_id})>"


class InfoSectorIndustryHistory(Base):
    __tablename__ = 'info_sector_industry_history'

    ticker_id = Column(Integer, ForeignKey('ticker.id'), nullable=False, primary_key=True)  # Foreign key reference to ticker table
    last_update = Column(Date, nullable=False, primary_key=True)                            # Timestamp to track when the data was recorded

    sector = Column(String(100), nullable=False)                                            # Sector of the company
    industry = Column(String(100), nullable=False)                                          # Industry of the company

    ticker = relationship("Ticker", back_populates="info_sector_industry_history")

    def __repr__(self):
        return f"<InfoSectorIndustryHistory(ticker_id={self.ticker_id}, sector={self.sector}, industry={self.industry})>"


class InfoTargetPriceAndRecommendation(Base):
    __tablename__ = 'info_target_price_and_recommendation'

    ticker_id = Column(Integer, ForeignKey('ticker.id'), nullable=False, primary_key=True)  # Foreign key reference to ticker table
    last_update = Column(DateTime, nullable=False, primary_key=True)  # Timestamp to track when the data was recorded

    # Target Price and Recommendation Information
    target_high_price = Column(Numeric(10, 4), nullable=True)  # Target high price
    target_low_price = Column(Numeric(10, 4), nullable=True)  # Target low price
    target_mean_price = Column(Numeric(10, 4), nullable=True)  # Target mean price
    target_median_price = Column(Numeric(10, 4), nullable=True)  # Target median price
    recommendation_mean = Column(Numeric(10, 4), nullable=True)  # Mean recommendation value
    recommendation_key = Column(String(20), nullable=True)  # Recommendation key (e.g., "buy", "hold")
    number_of_analyst_opinions = Column(Integer, nullable=True)  # Number of analyst opinions

    ticker = relationship("Ticker", back_populates="info_target_price_and_recommendation")

    def __repr__(self):
        return f"<InfoTargetPriceAndRecommendation(ticker_id={self.ticker_id}, date={self.date})>"


class InfoGovernance(Base):
    __tablename__ = 'info_governance'

    ticker_id = Column(Integer, ForeignKey('ticker.id'), nullable=False)  # Foreign key to ticker table
    last_update = Column(DateTime, nullable=False, primary_key=True)  # Timestamp to track when the data was recorded

    audit_risk = Column(Integer, nullable=True)  # Audit risk
    board_risk = Column(Integer, nullable=True)  # Board risk
    compensation_risk = Column(Integer, nullable=True)  # Compensation risk
    shareholder_rights_risk = Column(Integer, nullable=True)  # Shareholder rights risk
    overall_risk = Column(Integer, nullable=True)  # Overall risk
    governance_epoch_date = Column(Integer, nullable=True)  # Governance epoch date
    compensation_as_of_epoch_date = Column(Integer, nullable=True)  # Compensation as of epoch date

    # Define relationship to the Ticker class
    ticker = relationship("Ticker", back_populates="info_governance")

    def __repr__(self):
        return f"<InfoGovernance(ticker_id={self.ticker_id}, date={self.date})>"


class InfoCashAndFinancialRatios(Base):
    __tablename__ = 'info_cash_and_financial_ratios'

    ticker_id = Column(Integer, ForeignKey('ticker.id'), nullable=False, primary_key=True)  # Foreign key reference to ticker table
    last_update = Column(DateTime, nullable=False, primary_key=True)  # Timestamp to track when the data was recorded

    # __ Cash and Cash Flow Metrics __
    total_cash = Column(BigInteger)                                 # Total cash
    total_cash_per_share = Column(Numeric(10, 4))        # Total cash per share: precision 8, scale 4
    free_cashflow = Column(BigInteger)                              # Free cash flow
    operating_cashflow = Column(BigInteger)                         # Operating cash flow

    # __ Profitability Metrics __
    ebitda = Column(BigInteger)                                     # EBITDA
    total_revenue = Column(BigInteger)                              # Total revenue
    revenue_per_share = Column(Numeric(9, 3))           # Revenue per share: precision 7, scale 3
    gross_margins = Column(Numeric(11, 10))               # Gross margins: precision 6, scale 5
    ebitda_margins = Column(Numeric(11, 10))              # EBITDA margins: precision 6, scale 5
    operating_margins = Column(Numeric(11, 10))           # Operating margins: precision 6, scale 5

    # __ Growth Metrics __
    earnings_growth = Column(Numeric(6, 3))             # Earnings growth: precision 6, scale 3
    revenue_growth = Column(Numeric(6, 3))              # Revenue growth: precision 6, scale 3

    # __ Leverage and Liquidity Ratios __
    total_debt = Column(BigInteger)                                 # Total debt
    debt_to_equity = Column(Numeric(7, 3))              # Debt to equity ratio: precision 7, scale 3
    quick_ratio = Column(Numeric(5, 3))                 # Quick ratio: precision 5, scale 3
    current_ratio = Column(Numeric(5, 3))               # Current ratio: precision 5, scale 3

    # __ Return Metrics __
    return_on_assets = Column(Numeric(13, 10))            # Return on assets: precision 7, scale 5
    return_on_equity = Column(Numeric(13, 10))            # Return on equity: precision 7, scale 4

    # __ Valuation Metrics __
    trailing_peg_ratio = Column(Numeric(6, 4))          # Trailing PEG ratio: precision 6, scale 4

    ticker = relationship("Ticker", back_populates="info_cash_and_financial_ratios")

    def __repr__(self):
        return f"<InfoCashAndFinancialRatios(ticker_id={self.ticker_id}, date={self.date})>"


class InfoMarketAndFinancialMetrics(Base):
    __tablename__ = 'info_market_and_financial_metrics'

    ticker_id = Column(Integer, ForeignKey('ticker.id'), nullable=False, primary_key=True)  # Foreign key to ticker table
    last_update = Column(DateTime, nullable=False, primary_key=True)                        # Timestamp to track when the data was recorded

    # __ static data __
    dividend_rate = Column(Numeric(10, 4), nullable=True)                       # Dividend rate
    dividend_yield = Column(Numeric(13, 12), nullable=True)                      # Dividend yield
    ex_dividend_date = Column(Integer, nullable=True)                                       # Ex-dividend date
    payout_ratio = Column(Numeric(11, 9), nullable=True)                        # Payout ratio
    five_year_avg_dividend_yield = Column(Numeric(10, 4), nullable=True)        # Five year average dividend yield
    trailing_annual_dividend_rate = Column(Numeric(10, 4), nullable=True)       # Trailing annual dividend rate
    trailing_annual_dividend_yield = Column(Numeric(13, 12), nullable=True)     # Trailing annual dividend yield
    last_dividend_value = Column(Numeric(10, 4), nullable=True)                 # Last dividend value
    last_dividend_date = Column(Integer, nullable=True)                                     # Last dividend date

    # __ Volume and Trading Data __
    average_volume = Column(BigInteger, nullable=True)                                      # Average volume
    average_volume_10days = Column(BigInteger, nullable=True)                               # Average volume over 10 days
    average_daily_volume_10day = Column(BigInteger, nullable=True)                          # Average daily volume over 10 days

    # __ Valuation and Market Capitalization __
    enterprise_value = Column(BigInteger, nullable=True)                                    # Enterprise value
    book_value = Column(Numeric(10, 4), nullable=True)                          # Book value
    enterprise_to_revenue = Column(Numeric(10, 4), nullable=True)               # Enterprise to revenue
    enterprise_to_ebitda = Column(Numeric(10, 4), nullable=True)                # Enterprise to EBITDA

    # __ Short Interest and Ownership __
    shares_short = Column(BigInteger, nullable=True)                                        # Shares short
    shares_short_prior_month = Column(BigInteger, nullable=True)                            # Shares short prior month
    shares_short_previous_month_date = Column(Integer, nullable=True)                       # Shares short previous month date
    date_short_interest = Column(Integer, nullable=True)                                    # Date short interest
    shares_percent_shares_out = Column(Numeric(13, 12), nullable=True)          # Shares percent shares out
    held_percent_insiders = Column(Numeric(13, 12), nullable=True)              # Held percent insiders
    held_percent_institutions = Column(Numeric(13, 12), nullable=True)          # Held percent institutions
    short_ratio = Column(Numeric(10, 4), nullable=True)                         # Short ratio
    short_percent_of_float = Column(Numeric(13, 12), nullable=True)             # Short percent of float
    implied_shares_outstanding = Column(BigInteger, nullable=True)                          # Implied shares outstanding
    float_shares = Column(BigInteger, nullable=True)                                        # Float shares
    shares_outstanding = Column(BigInteger, nullable=True)                                  # Shares outstanding

    # __ Earnings and Growth Metrics __
    earnings_quarterly_growth = Column(Numeric(10, 4), nullable=True)           # Earnings quarterly growth
    net_income_to_common = Column(BigInteger, nullable=True)                                # Net income to common
    trailing_eps = Column(Numeric(10, 4), nullable=True)                        # Trailing EPS
    forward_eps = Column(Numeric(10, 4), nullable=True)                         # Forward EPS
    peg_ratio = Column(Numeric(10, 4), nullable=True)                           # PEG ratio

    # __ Stock Splits and Adjustments __
    last_split_factor = Column(String(20), nullable=True)                                   # Last split factor
    last_split_date = Column(Integer, nullable=True)                                        # Last split date

    # __ Risk Metrics __
    beta = Column(Numeric(10, 4), nullable=True)                                # Beta

    # __ Other Financial Ratios __
    profit_margins = Column(Numeric(11, 9), nullable=True)                      # Profit margins
    fifty_two_week_change = Column(Numeric(11, 10), nullable=True)               # Fifty-two week change
    sp_fifty_two_week_change = Column(Numeric(11, 10), nullable=True)            # S&P fifty-two week change
    last_fiscal_year_end = Column(Integer, nullable=True)                                   # Last fiscal year-end
    next_fiscal_year_end = Column(Integer, nullable=True)                                   # Next fiscal year-end
    most_recent_quarter = Column(Integer, nullable=True)                                    # Most recent quarter

    # Define relationship to the Ticker class
    ticker = relationship("Ticker", back_populates="info_market_and_financial_metrics")

    def __repr__(self):
        return f"<InfoDividendYield(ticker_id={self.ticker_id}, date={self.date}>"


class InfoGeneralStock(Base):
    __tablename__ = 'info_general_stock'

    ticker_id = Column(Integer, ForeignKey('ticker.id'), nullable=False)  # Foreign key to ticker table
    last_update = Column(DateTime, nullable=False, primary_key=True)  # Timestamp to track when the data was recorded

    # *** from info ***
    isin = Column(String(20), nullable=True)                                        # ISIN   # TODO consider to remove it due to HTTPS request failures
    currency = Column(String(10), nullable=True)                                    # Currency used for stock values
    symbol = Column(String(10), nullable=False)                                     # Stock symbol
    exchange = Column(String(10), nullable=True)                                    # Stock exchange
    quote_type = Column(String(20), nullable=True)                                  # Quote type
    underlying_symbol = Column(String(10))                                          # Underlying symbol
    short_name = Column(String(50))                                                 # Short name
    long_name = Column(String(100))                                                 # Long name
    first_trade_date_epoch_utc = Column(BigInteger)                                 # First trade date in epoch UTC
    time_zone_full_name = Column(String(50))                                        # Full name of the time zone
    time_zone_short_name = Column(String(10))                                       # Short name of the time zone
    uuid = Column(SA_UUID)                                                          # UUID for the record
    message_board_id = Column(String(50))                                           # Message board ID
    gmt_offset_milliseconds = Column(BigInteger)                                    # GMT offset in milliseconds
    price_hint = Column(Integer, nullable=True)                                     # Price hint
    max_age = Column(Integer)                                                       # Max age

    # Company Description and Employees
    full_time_employees = Column(Integer)                                           # Full-time employees

    # *** from history_metadata *** static information
    full_exchange_name = Column(String(50), nullable=True)                          # Full exchange name
    instrument_type = Column(String(20), nullable=True)                             # Instrument type
    has_pre_post_market_data = Column(Boolean, nullable=True)                       # Pre and post market data availability
    gmt_offset = Column(Integer, nullable=True)                                     # GMT offset
    chart_previous_close = Column(Numeric(10, 4), nullable=True)      # Chart previous close
    data_granularity = Column(String(10), nullable=True)                            # Data granularity

    # Define relationship to the Ticker class
    ticker = relationship("Ticker", back_populates="info_general_stock")

    def __repr__(self):
        return f"<InfoGeneralStock(ticker_id={self.ticker_id}, date={self.date})>"


class InfoTradingSession(Base):
    __tablename__ = 'info_trading_session'

    ticker_id = Column(Integer, ForeignKey('ticker.id'), nullable=False)    # Foreign key to ticker table
    last_update = Column(DateTime, nullable=False, primary_key=True)        # Timestamp to track when the data was recorded

    # *** from history metadata ***
    regular_market_time = Column(BigInteger, nullable=True)                       # Regular market time      - this changes during trading session
    regular_market_price = Column(Numeric(10, 4), nullable=True)     # Regular market price     - this changes during trading session
    fifty_two_week_high = Column(Numeric(10, 4), nullable=True)      # 52-week high             - this changes during trading session
    fifty_two_week_low = Column(Numeric(10, 4), nullable=True)       # 52-week low              - this changes during trading session
    regular_market_day_high = Column(Numeric(10, 4), nullable=True)  # Regular market day high  - this changes during trading session
    regular_market_day_low = Column(Numeric(10, 4), nullable=True)   # Regular market day low   - this changes during trading session
    regular_market_volume = Column(BigInteger, nullable=True)                      # Regular market volume    - this changes during trading session

    # *** from basic_info ***
    last_price = Column(Numeric(19, 15), nullable=True)         # Last traded price (more precise than current price)   - this changes during trading session
    last_volume = Column(BigInteger, nullable=True)                         # Last traded volume                                    - this changes during trading session

    # *** from basic info *** should be dynamic but are static
    ten_day_average_volume = Column(BigInteger, nullable=True)  # Ten day average volume
    three_month_average_volume = Column(BigInteger, nullable=True)  # Three-month average volume
    year_change = Column(Numeric(19, 18), nullable=True)  # Yearly change
    year_high = Column(Numeric(19, 15), nullable=True)  # Yearly high       - this info is not updated for more than one trading day
    year_low = Column(Numeric(19, 15), nullable=True)  # Yearly low        - this info is not updated for more than one trading day

    # *** from info ***
    current_price = Column(Numeric(10, 4), nullable=True)  # Current price of the stock
    open = Column(Numeric(10, 4), nullable=True)                           # Opening price                  - this changes when trading session starts
    previous_close = Column(Numeric(10, 4), nullable=True)                 # Previous closing price         - this changes when trading session starts
    regular_market_previous_close = Column(Numeric(10, 4), nullable=True)  # Regular market previous close  - this changes when trading session starts
    day_high = Column(Numeric(10, 4), nullable=True)                        # Day's high price              - this changes during trading session
    day_low = Column(Numeric(10, 4), nullable=True)                         # Day's low price               - this changes during trading session
    market_cap = Column(BigInteger, nullable=True)                                        # Market capitalization       - this changes during trading session
    regular_market_open = Column(Numeric(10, 4), nullable=True)  # Regular market open
    trailing_pe = Column(Numeric(11, 7), nullable=True)  # Trailing PE ratio
    forward_pe = Column(Numeric(11, 7), nullable=True)  # Forward PE ratio
    volume = Column(BigInteger, nullable=True)  # Volume
    bid = Column(Numeric(10, 4), nullable=True)  # Bid price
    ask = Column(Numeric(10, 4), nullable=True)  # Ask price
    bid_size = Column(Integer, nullable=True)  # Bid size
    ask_size = Column(Integer, nullable=True)  # Ask size
    price_to_sales_trailing_12months = Column(Numeric(12, 8), nullable=True)  # Price to sales ratio trailing 12 months
    price_to_book = Column(Numeric(14, 10), nullable=True)  # Price to book

    # *** from info *** should be dynamic but for now is static
    fifty_day_average = Column(Numeric(11, 6), nullable=True)         # 50-day average price     - this is fixed to last close (yesterday)
    two_hundred_day_average = Column(Numeric(10, 5), nullable=True)   # 200-day average price    - this is fixed to last close (yesterday)

    # Define relationship to the Ticker class
    ticker = relationship("Ticker", back_populates="info_trading_session")

    def __repr__(self):
        return f"<InfoExchangeTrading(ticker_id={self.ticker_id}, date={self.date})>"


class InsiderPurchases(Base):
    __tablename__ = 'insider_purchases'

    ticker_id = Column(Integer, ForeignKey('ticker.id'), nullable=False)  # Foreign key to ticker table
    last_update = Column(DateTime, nullable=False, primary_key=True)  # Timestamp to track when the data was recorded

    purchases_shares = Column(BigInteger, nullable=True)  # Number of shares purchased in the last 6 months
    purchases_transactions = Column(Integer, nullable=True)  # Number of purchase transactions
    sales_shares = Column(BigInteger, nullable=True)  # Number of shares sold in the last 6 months
    sales_transactions = Column(Integer, nullable=True)  # Number of sale transactions
    net_shares_purchased_sold = Column(BigInteger, nullable=True)  # Net shares purchased (sold)
    net_shares_purchased_sold_transactions = Column(Integer, nullable=True)  # Number of net shares purchased (sold) transactions
    total_insider_shares_held = Column(BigInteger, nullable=True)  # Total insider shares held
    percent_net_shares_purchased_sold = Column(Numeric, nullable=True)  # Percentage of net shares purchased (sold)
    percent_buy_shares = Column(Numeric, nullable=True)  # Percentage of buy shares
    percent_sell_shares = Column(Numeric, nullable=True)  # Percentage of sell shares

    # Define relationship to the Ticker class
    ticker = relationship("Ticker", back_populates="insider_purchases")

    def __repr__(self):
        return f"<InsiderPurchases(ticker_id={self.ticker_id}, date={self.date})>"


class InsiderRosterHolders(Base):
    __tablename__ = 'insider_roster_holders'

    ticker_id = Column(Integer, ForeignKey('ticker.id'), nullable=False, primary_key=True)  # Foreign key to ticker table
    last_update = Column(DateTime, nullable=False, primary_key=True)  # Timestamp to track when the data was recorded
    name = Column(String(255), nullable=False, primary_key=True)  # Name of the insider

    position = Column(String(255), nullable=True)  # Position of the insider
    url = Column(Text, nullable=True)  # URL with more information about the insider
    most_recent_transaction = Column(String(255), nullable=True)  # Most recent transaction type
    latest_transaction_date = Column(DateTime, nullable=True)  # Date of the latest transaction
    shares_owned_directly = Column(BigInteger, nullable=True)  # Number of shares owned directly
    position_direct_date = Column(DateTime, nullable=True)  # Date of direct position
    shares_owned_indirectly = Column(BigInteger, nullable=True)  # Number of shares owned indirectly
    position_indirect_date = Column(Numeric(17, 5), nullable=True)  # Date of indirect position

    # Define relationship to the Ticker class
    ticker = relationship("Ticker", back_populates="insider_roster_holders")

    def __repr__(self):
        return f"<InsiderRosterHolders(ticker_id={self.ticker_id}, date={self.date})>"


class InsiderTransactions(Base):
    __tablename__ = 'insider_transactions'

    ticker_id = Column(Integer, ForeignKey('ticker.id'), nullable=False, primary_key=True)  # Foreign key to ticker table
    insider = Column(String(255), nullable=False, primary_key=True)                          # Name of the insider
    start_date = Column(Date, nullable=False, primary_key=True)                              # Start date of the transaction
    last_update = Column(DateTime, nullable=False, primary_key=True)                        # Timestamp to track when the data was recorded
    shares = Column(BigInteger, nullable=False, primary_key=True)                            # Number of shares involved in the transaction
    value = Column(Numeric, nullable=True, primary_key=True)                                # Value of the transaction

    url = Column(Text, nullable=True)                                                       # URL with more information about the transaction
    text = Column(Text, nullable=True)                                                      # Description of the transaction
    position = Column(String(255), nullable=True)                                           # Position of the insider
    transaction = Column(String(255), nullable=True)                                        # Type of transaction
    ownership = Column(String(3), nullable=True)                                            # Ownership type (D for Direct, I for Indirect)

    # Define relationship to the Ticker class
    ticker = relationship("Ticker", back_populates="insider_transactions")

    def __repr__(self):
        return f"<InsiderTransactions(ticker_id={self.ticker_id}, start_date={self.start_date})>"


class InstitutionalHolders(Base):
    __tablename__ = 'institutional_holders'

    ticker_id = Column(Integer, ForeignKey('ticker.id'), nullable=False, primary_key=True)  # Foreign key to ticker table
    date_reported = Column(Date, nullable=False, primary_key=True)  # Date when the information was reported
    holder = Column(String(255), nullable=False, primary_key=True)  # Name of the institutional holder
    last_update = Column(DateTime, nullable=False, primary_key=True)  # Timestamp to track when the data was recorded

    pct_held = Column(Numeric, nullable=False)  # Percentage of shares held
    shares = Column(BigInteger, nullable=False)  # Number of shares held
    value = Column(Numeric, nullable=False)  # Value of the shares held

    # Define relationship to the Ticker class
    ticker = relationship("Ticker", back_populates="institutional_holders")

    def __repr__(self):
        return f"<InstitutionalHolders(ticker_id={self.ticker_id}, date_reported={self.date_reported})>"


class MajorHolders(Base):
    __tablename__ = 'major_holders'

    ticker_id = Column(Integer, ForeignKey('ticker.id'), nullable=False)  # Foreign key to ticker table
    last_update = Column(DateTime, nullable=False, primary_key=True)  # Timestamp to track when the data was recorded

    insiders_percent_held = Column(Numeric, nullable=False)  # Percentage of shares held by insiders
    institutions_percent_held = Column(Numeric, nullable=False)  # Percentage of shares held by institutions
    institutions_float_percent_held = Column(Numeric, nullable=False)  # Percentage of float shares held by institutions
    institutions_count = Column(BigInteger, nullable=False)  # Number of institutions holding shares

    # Relationship to Ticker class
    ticker = relationship("Ticker", back_populates="major_holders")

    def __repr__(self):
        return f"<MajorHolders(ticker_id={self.ticker_id}, date={self.date})>"


class MutualFundHolders(Base):
    __tablename__ = 'mutualfund_holders'

    ticker_id = Column(Integer, ForeignKey('ticker.id'), nullable=False, primary_key=True)  # Foreign key to ticker table
    date_reported = Column(Date, nullable=False, primary_key=True)  # Date when the information was reported
    holder = Column(String(255), nullable=False, primary_key=True)  # Name of the mutual fund holder
    last_update = Column(DateTime, nullable=False, primary_key=True)  # Timestamp to track when the data was recorded

    pct_held = Column(Numeric, nullable=False)  # Percentage of shares held
    shares = Column(BigInteger, nullable=False)  # Number of shares held
    value = Column(Numeric, nullable=False)  # Value of the shares held

    # Relationship to Ticker class
    ticker = relationship("Ticker", back_populates="mutualfund_holders")

    def __repr__(self):
        return f"<MutualFundHolders(ticker_id={self.ticker_id}, date_reported={self.date_reported})>"


class Recommendations(Base):
    __tablename__ = 'recommendations'

    ticker_id = Column(Integer, ForeignKey('ticker.id'), nullable=False, primary_key=True)  # Foreign key to ticker table
    last_update = Column(DateTime, nullable=False, primary_key=True)  # Timestamp to track when the data was recorded
    period = Column(String(10), nullable=False, primary_key=True)  # Period (e.g., "0m", "-1m")  # TODO: primary key?

    strong_buy = Column(Integer, nullable=False)  # Number of strong buy recommendations
    buy = Column(Integer, nullable=False)  # Number of buy recommendations
    hold = Column(Integer, nullable=False)  # Number of hold recommendations
    sell = Column(Integer, nullable=False)  # Number of sell recommendations
    strong_sell = Column(Integer, nullable=False)  # Number of strong sell recommendations

    # Relationship to Ticker class
    ticker = relationship("Ticker", back_populates="recommendations")

    def __repr__(self):
        return f"<Recommendations(id={self.id}, ticker_id={self.ticker_id}, date={self.date})>"


class UpgradesDowngrades(Base):
    __tablename__ = 'upgrades_downgrades'

    ticker_id = Column(Integer, ForeignKey('ticker.id'), nullable=False, primary_key=True)  # Foreign key to ticker table
    date = Column(Date, nullable=False, primary_key=True)  # Date when the upgrade/downgrade occurred
    firm = Column(String(255), nullable=False, primary_key=True)  # Name of the firm issuing the upgrade/downgrade
    last_update = Column(DateTime, nullable=False, primary_key=True)  # Timestamp to track when the data was recorded

    to_grade = Column(String(50), nullable=False)  # New grade assigned by the firm
    from_grade = Column(String(50))  # Previous grade assigned by the firm
    action = Column(String(50))  # Action taken (e.g., "main", "reit", "up", "init")

    # Relationship to Ticker class
    ticker = relationship("Ticker", back_populates="upgrades_downgrades")

    def __repr__(self):
        return f"<UpgradesDowngrades(ticker_id={self.ticker_id}, date={self.date})>"


class CandleDataMonth(Base):
    __tablename__ = 'candle_data_month'

    # Unique identifier for each candle
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Foreign key to the ticker table, indexed for fast queries
    ticker_id = Column(Integer, ForeignKey('ticker.id'), nullable=False, index=True)

    # Date of the candlestick without timezone, indexed for fast queries
    date = Column(Date, nullable=False, index=True)

    # Details of the candlestick
    open = Column(Float, nullable=False)                         # Opening price
    high = Column(Float, nullable=False)                         # Highest price
    low = Column(Float, nullable=False)                          # Lowest price
    close = Column(Float, nullable=False)                        # Closing price
    adj_close = Column(Float, nullable=True)                     # Adjusted closing price
    volume = Column(Float, nullable=True)

    # Timestamp to track when the data was recorded
    last_update = Column(DateTime, nullable=True)

    # Relationship to the Ticker class
    ticker = relationship("Ticker", back_populates="candle_data_month")

    # Define unique constraint and indexes to improve query performance
    __table_args__ = (
        UniqueConstraint('ticker_id', 'date', name='uix_candle_data_month_ticker_date'),
        Index('ix_candle_data_month_ticker_id_date', 'ticker_id', 'date'),  # Combined index on ticker_id and date
    )

    def __repr__(self):
        return (f"<CandleDataMonth(id={self.id}, ticker_id={self.ticker_id}, date={self.date}, open={self.open}, "
                f"high={self.high}, low={self.low}, close={self.close}, adj_close={self.adj_close}, "
                f"volume={self.volume}, last_update={self.last_update})>")


class CandleDataWeek(Base):
    __tablename__ = 'candle_data_week'

    # Unique identifier for each candle
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Foreign key to the ticker table, indexed for fast queries
    ticker_id = Column(Integer, ForeignKey('ticker.id'), nullable=False, index=True)

    # Date of the candlestick without timezone, indexed for fast queries
    date = Column(Date, nullable=False, index=True)

    # Details of the candlestick
    open = Column(Float, nullable=False)                         # Opening price
    high = Column(Float, nullable=False)                         # Highest price
    low = Column(Float, nullable=False)                          # Lowest price
    close = Column(Float, nullable=False)                        # Closing price
    adj_close = Column(Float, nullable=True)                     # Adjusted closing price
    volume = Column(Float, nullable=True)

    # Timestamp to track when the data was recorded
    last_update = Column(DateTime, nullable=True)

    # Relationship to the Ticker class
    ticker = relationship("Ticker", back_populates="candle_data_week")

    # Define unique constraint and indexes to improve query performance
    __table_args__ = (
        UniqueConstraint('ticker_id', 'date', name='uix_candle_data_week_ticker_date'),
        Index('ix_candle_data_week_ticker_id_date', 'ticker_id', 'date'),  # Combined index on ticker_id and date
    )

    def __repr__(self):
        return (f"<CandleDataWeek(id={self.id}, ticker_id={self.ticker_id}, date={self.date}, open={self.open}, "
                f"high={self.high}, low={self.low}, close={self.close}, adj_close={self.adj_close}, "
                f"volume={self.volume}, last_update={self.last_update})>")


class CandleDataDay(Base):
    __tablename__ = 'candle_data_day'

    # Unique identifier for each candle
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Foreign key to the ticker table, indexed for fast queries
    ticker_id = Column(Integer, ForeignKey('ticker.id'), nullable=False, index=True)

    # Date of the candlestick without timezone, indexed for fast queries
    date = Column(Date, nullable=False, index=True)

    # Details of the candlestick
    open = Column(Float, nullable=False)                         # Opening price
    high = Column(Float, nullable=False)                         # Highest price
    low = Column(Float, nullable=False)                          # Lowest price
    close = Column(Float, nullable=False)                        # Closing price
    adj_close = Column(Float, nullable=True)                     # Adjusted closing price
    volume = Column(Float, nullable=True)                        # Volume traded

    # Timestamp to track when the data was recorded
    last_update = Column(DateTime, nullable=True)

    # __ relationship to the Ticker class __
    ticker = relationship("Ticker", back_populates="candle_data_day")

    # __ relationship with CandleAnalysisDay __
    analysis = relationship("CandleAnalysisDay", back_populates="candle_data_day", uselist=False)  # One-to-one relationship

    # Define unique constraint and indexes to improve query performance
    __table_args__ = (
        UniqueConstraint('ticker_id', 'date', name='uix_candle_data_day_ticker_date'),
        Index('ix_candle_data_day_ticker_id_date', 'ticker_id', 'date'),  # Combined index on ticker_id and date
    )

    def __repr__(self):
        return (f"<CandleDataDay(id={self.id}, ticker_id={self.ticker_id}, date={self.date}, open={self.open}, "
                f"high={self.high}, low={self.low}, close={self.close}, adj_close={self.adj_close}, "
                f"volume={self.volume}, last_update={self.last_update})>")


class CandleData1Hour(Base):
    __tablename__ = 'candle_data_1_hour'

    # Unique identifier for each candle
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Foreign key to the ticker table, indexed for fast queries
    ticker_id = Column(Integer, ForeignKey('ticker.id'), nullable=False, index=True)

    # Date and time of the candlestick with timezone, indexed for fast queries
    date = Column(DateTime(timezone=True), nullable=False, index=True)

    # Time zone information
    time_zone = Column(String(50), nullable=True)               # Time zone information

    # Details of the candlestick
    open = Column(Float, nullable=False)                        # Opening price
    high = Column(Float, nullable=False)                        # Highest price
    low = Column(Float, nullable=False)                         # Lowest price
    close = Column(Float, nullable=False)                       # Closing price
    adj_close = Column(Float, nullable=True)                    # Adjusted closing price
    volume = Column(Float, nullable=True)                       # Volume traded

    # Timestamp to track when the data was recorded
    last_update = Column(DateTime, nullable=False)

    # Relationship to the Ticker class
    ticker = relationship("Ticker", back_populates="candle_data_1_hour")

    # Define unique constraint and indexes to improve query performance
    __table_args__ = (
        UniqueConstraint('ticker_id', 'date', name='uix_candle_data_1_hour_ticker_date'),
        Index('ix_candle_data_1_hour_ticker_id_date', 'ticker_id', 'date'),  # Combined index on ticker_id and date
    )

    def __repr__(self):
        return (f"<CandleData1Hour(id={self.id}, ticker_id={self.ticker_id}, date={self.date}, open={self.open}, "
                f"high={self.high}, low={self.low}, close={self.close}, adj_close={self.adj_close}, "
                f"volume={self.volume}, last_update={self.last_update}, time_zone={self.time_zone})>")


class CandleData5Minutes(Base):
    __tablename__ = 'candle_data_5_minutes'

    # Unique identifier for each candle
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Foreign key to the ticker table, indexed for fast queries
    ticker_id = Column(Integer, ForeignKey('ticker.id'), nullable=False, index=True)

    # Date and time of the candlestick with timezone, indexed for fast queries
    date = Column(DateTime(timezone=True), nullable=False, index=True)

    # Time zone information
    time_zone = Column(String(50), nullable=True)

    # Details of the candlestick
    open = Column(Float, nullable=False)                         # Opening price
    high = Column(Float, nullable=False)                         # Highest price
    low = Column(Float, nullable=False)                          # Lowest price
    close = Column(Float, nullable=False)                        # Closing price
    adj_close = Column(Float, nullable=True)                     # Adjusted closing price
    volume = Column(Float, nullable=True)

    # Timestamp to track when the data was recorded
    last_update = Column(DateTime, nullable=False)

    # Relationship to the Ticker class
    ticker = relationship("Ticker", back_populates="candle_data_5_minutes")

    # Define unique constraint and indexes to improve query performance
    __table_args__ = (
        UniqueConstraint('ticker_id', 'date', name='uix_candle_data_5_minutes_ticker_date'),
        Index('ix_candle_data_5_minutes_ticker_id_date', 'ticker_id', 'date'),  # Combined index on ticker_id and date
    )

    def __repr__(self):
        return (f"<CandleData5Minutes(id={self.id}, ticker_id={self.ticker_id}, date={self.date}, open={self.open}, "
                f"high={self.high}, low={self.low}, close={self.close}, adj_close={self.adj_close}, "
                f"volume={self.volume}, last_update={self.last_update}, time_zone={self.time_zone})>")


class CandleData1Minute(Base):
    __tablename__ = 'candle_data_1_minute'

    # Unique identifier for each candle
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Foreign key to the ticker table, indexed for fast queries
    ticker_id = Column(Integer, ForeignKey('ticker.id'), nullable=False, index=True)

    # Date and time of the candlestick with timezone, indexed for fast queries
    date = Column(DateTime(timezone=True), nullable=False, index=True)

    # Time zone information
    time_zone = Column(String(50), nullable=True)

    # Details of the candlestick
    open = Column(Float, nullable=False)                         # Opening price
    high = Column(Float, nullable=False)                         # Highest price
    low = Column(Float, nullable=False)                          # Lowest price
    close = Column(Float, nullable=False)                        # Closing price
    adj_close = Column(Float, nullable=True)                     # Adjusted closing price
    volume = Column(Float, nullable=True)

    # Timestamp to track when the data was recorded
    last_update = Column(DateTime, nullable=False)

    # Relationship to the Ticker class
    ticker = relationship("Ticker", back_populates="candle_data_1_minute")

    # Define unique constraint and indexes to improve query performance
    __table_args__ = (
        UniqueConstraint('ticker_id', 'date', name='uix_candle_data_1_minute_ticker_date'),
        Index('ix_candle_data_1_minute_ticker_id_date', 'ticker_id', 'date'),  # Combined index on ticker_id and date
    )

    def __repr__(self):
        return (f"<CandleData1Minute(id={self.id}, ticker_id={self.ticker_id}, date={self.date}, open={self.open}, "
                f"high={self.high}, low={self.low}, close={self.close}, adj_close={self.adj_close}, "
                f"volume={self.volume}, last_update={self.last_update}, time_zone={self.time_zone})>")


# Candle Analysis Tables
class CandleAnalysisDay(Base):
    __tablename__ = 'candle_analysis_day'

    candle_data_day_id = Column(Integer, ForeignKey('candle_data_day.id'), primary_key=True, index=True)  # Foreign key to CandleDataDay

    prev_close = Column(Float, nullable=True)       # Previous close price
    tr = Column(Float, nullable=True)               # True Range
    atr = Column(Float, nullable=True)              # Average True Range
    atr_percent = Column(Float, nullable=True)      # Average True Range as a percentage

    bullish = Column(Boolean, nullable=True)        # Bullish indicator
    body_delta = Column(Float, nullable=True)       # Difference between open and close prices
    shadow_delta = Column(Float, nullable=True)     # Difference between high and low prices

    percent_body = Column(Float, nullable=True)             # Percentage of body relative to the total candle
    percent_upper_shadow = Column(Float, nullable=True)     # Percentage of upper shadow
    percent_lower_shadow = Column(Float, nullable=True)     # Percentage of lower shadow

    long_body = Column(Boolean, nullable=True)          # Indicator if the candle has a long body
    shadow_imbalance = Column(Float, nullable=True)     # Imbalance between upper and lower shadows
    shaven_head = Column(Boolean, nullable=True)        # Indicator if the candle has no upper shadow
    shaven_bottom = Column(Boolean, nullable=True)      # Indicator if the candle has no lower shadow

    doji = Column(Boolean, nullable=True)                       # Indicator if the candle is a doji
    spinning_top = Column(Boolean, nullable=True)               # Indicator if the candle is a spinning top
    umbrella_line = Column(Boolean, nullable=True)              # Indicator if the candle is an umbrella line
    umbrella_line_inverted = Column(Boolean, nullable=True)     # Indicator if the candle is an inverted umbrella line

    mid_body = Column(Float, nullable=True)             # Midpoint of the body of the candle
    body_atr_percent = Column(Float, nullable=True)     # Body as a percentage of ATR
    body2atr_percent = Column(Float, nullable=True)     # Double body as a percentage of ATR
    long_atr_candle = Column(Boolean, nullable=True)    # Indicator if the candle is long relative to ATR

    body2atr2_shadow_imbalance_ratio = Column(Float, nullable=True)     # Ratio of double body to shadow imbalance
    long_candle_light = Column(Boolean, nullable=True)                  # Indicator if the candle is long and light
    long_candle_bullish_light = Column(Boolean, nullable=True)          # Indicator if the candle is long, bullish, and light
    long_candle_bearish_light = Column(Boolean, nullable=True)          # Indicator if the candle is long, bearish, and light

    long_candle = Column(Boolean, nullable=True)                # Indicator if the candle is long
    long_candle_bullish = Column(Boolean, nullable=True)        # Indicator if the candle is long and bullish
    long_candle_bearish = Column(Boolean, nullable=True)        # Indicator if the candle is long and bearish

    engulfing_bullish = Column(Boolean, nullable=True)  # Bullish engulfing pattern indicator
    engulfing_bearish = Column(Boolean, nullable=True)  # Bearish engulfing pattern indicator

    dark_cloud_cover = Column(Boolean, nullable=True)           # Dark cloud cover pattern indicator
    dark_cloud_cover_light = Column(Boolean, nullable=True)     # Light version of dark cloud cover pattern indicator
    piercing_pattern = Column(Boolean, nullable=True)           # Piercing pattern indicator
    piercing_pattern_light = Column(Boolean, nullable=True)     # Light version of piercing pattern indicator
    on_neck_pattern = Column(Boolean, nullable=True)            # On neck pattern indicator
    in_neck_pattern = Column(Boolean, nullable=True)            # In neck pattern indicator
    thrusting_pattern = Column(Boolean, nullable=True)          # Thrusting pattern indicator

    star = Column(Boolean, nullable=True)               # Star pattern indicator
    evening_star = Column(Boolean, nullable=True)       # Evening star pattern indicator
    morning_star = Column(Boolean, nullable=True)       # Morning star pattern indicator

    ma50 = Column(Float, nullable=True)                     # 50-day moving average
    ma100 = Column(Float, nullable=True)                    # 100-day moving average
    ma200 = Column(Float, nullable=True)                    # 200-day moving average
    ma200_distance_percent = Column(Float, nullable=True)   # Distance from 200-day moving average in percentage

    rsi = Column(Float, nullable=True)      # Relative Strength Index

    # __ relationship to CandleDataDay
    candle_data_day = relationship("CandleDataDay", back_populates="analysis")  # One-to-one relationship

    def __repr__(self):
        return f"<CandleAnalysisDay(candle_data_day_id={self.candle_data_day_id})>"
