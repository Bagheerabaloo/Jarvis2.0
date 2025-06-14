import numpy as np
import pandas as pd
import yfinance as yf
from datetime import date
from typing import Optional
from datetime import datetime, date
from dataclasses import dataclass, field

from sqlalchemy.sql import and_

from src.common.tools.library import safe_execute
from src.stock.src.TickerServiceBase import Ticker, TickerServiceBase
from src.stock.src.db.models import Action, BalanceSheet, Calendar, CashFlow, Financials, EarningsDates, TickerStatus
from src.stock.src.db.models import InfoCashAndFinancialRatios, InfoCompanyAddress, InfoSectorIndustryHistory, InfoTradingSession
from src.stock.src.db.models import InfoTargetPriceAndRecommendation, InfoMarketAndFinancialMetrics, InfoGeneralStock, InfoGovernance
from src.stock.src.db.models import InsiderPurchases, InsiderRosterHolders, InsiderTransactions, InstitutionalHolders, MajorHolders, MutualFundHolders, Recommendations
from src.stock.src.db.models import UpgradesDowngrades
from src.stock.src.CandleService import CandleService
from src.stock.src.CandleDataInterval import CandleDataInterval

from logger_setup import LOGGER

pd.set_option('future.no_silent_downcasting', True)


@dataclass
class TickerService(TickerServiceBase):
    candle_service: CandleService = field(default=None, init=False)
    stock: yf.Ticker = field(default=None, init=False)
    info: dict = field(default=None, init=False)

    def set_stock(self, stock: yf.Ticker):
        """
        Set the stock attribute to the provided yf.Ticker object.

        :param stock: yf.Ticker object representing the stock.
        """
        self.stock = stock

    def set_info(self, info):
        """
        Set the info attribute to the provided dictionary.

        :param info: Dictionary containing stock information.
        """
        self.info = info

    """ Handle the insertion or update of a Ticker record in the database. """
    def handle_ticker(self, info: dict, error: Optional[str], status: Optional[str]) -> bool:

        if info is None or not info:
            existing_ticker = self.get_existing_ticker()
            self.initialize_ticker(ticker=existing_ticker)  # initialize Ticker attribute
            self.handle_ticker_status(status, error)
            return False

        company_name = info.get('longName', None)
        business_summary = info.get('longBusinessSummary', None)
        ticker_data = {"symbol": self.symbol, "company_name": company_name, "business_summary": business_summary}
        try:
            # __ check if the ticker already exists __
            existing_ticker: Optional[Ticker] = self.session.query(Ticker).filter_by(symbol=ticker_data['symbol']).first()

            # __ update the existing ticker if it exists __
            if existing_ticker:
                return self.update_existing_ticker(existing_ticker=existing_ticker, ticker_data=ticker_data)

            # __ create a new instance of the Ticker class if it does not exist __
            self.create_new_ticker(ticker_data)
            return True
        except RuntimeError as e:
                LOGGER.error(f"{e}")
        except Exception as e:
            self.session.rollback()
            LOGGER.error(f"{self.symbol} - Error updating or inserting ticker: {e}")

        return False

    def get_existing_ticker(self) -> Optional[Ticker]:
        """
        Retrieve the existing Ticker object for the current symbol.

        :return: Ticker object if it exists, None otherwise.
        """
        return self.session.query(Ticker).filter_by(symbol=self.symbol).first()

    def update_existing_ticker(self, existing_ticker: Ticker, ticker_data: dict) -> bool:
        """
        Update an existing ticker with new data.

        :param existing_ticker: The existing Ticker object.
        :param ticker_data: Dictionary containing the new ticker data.
        :return: True if the record was updated, False otherwise.
        """
        # Track changes
        updated_fields = {}

        # Update the existing record and track changes
        if existing_ticker.company_name != ticker_data['company_name']:
            updated_fields['company_name'] = (existing_ticker.company_name, ticker_data['company_name'])
            existing_ticker.company_name = ticker_data['company_name']

        if existing_ticker.business_summary != ticker_data['business_summary']:
            updated_fields['business_summary'] = (existing_ticker.business_summary, ticker_data['business_summary'])
            existing_ticker.business_summary = ticker_data['business_summary']

        self.initialize_ticker(ticker=existing_ticker)  # initialize Ticker attribute

        # Only commit and print updates if there are changes
        if updated_fields:
            self.commit()  # Commit the changes
            LOGGER.info(f"{ticker_data['symbol']} - {'Ticker'.rjust(50)} - UPDATED successfully.")
            for field_, (old_value, new_value) in updated_fields.items():
                LOGGER.debug(f" - {field_}: {old_value} -> {new_value}")

        return True

    def final_update_ticker(self) -> bool:
        # __ check if the ticker already exists __
        existing_ticker: Optional[Ticker] = self.session.query(Ticker).filter_by(symbol=self.symbol).first()

        # __ update the existing ticker if it exists __
        if not existing_ticker:
            return False

        existing_ticker.last_update = datetime.now()
        self.commit()  # Commit the changes
        return True

    def create_new_ticker(self, ticker_data: dict) -> None:
        """
        Create a new Ticker object with the provided data.

        :param ticker_data: Dictionary containing the ticker data.
        """
        new_ticker = Ticker(**ticker_data)
        self.session.add(new_ticker)
        self.commit()
        LOGGER.info(f"{ticker_data['symbol']} - {'Ticker'.rjust(50)} - ADDED successfully.")
        self.initialize_ticker(ticker=new_ticker)  # initialize Ticker attribute

    def initialize_ticker(self, ticker: Ticker) -> None:
        """
        Initialize the Ticker object for the symbol.

        :param ticker: The Ticker object for the symbol.
        """
        self.ticker = ticker
        self.initialize_candle_service()  # initialize CandleService

    def initialize_candle_service(self):
        self.candle_service = CandleService(session=self.session, symbol=self.ticker.symbol, commit_enable=self.commit_enable)  # initialize CandleService
        self.candle_service.initialize_ticker(ticker=self.ticker)  # initialize CandleService ticker attribute

    """Handle of all other information"""

    def handle_balance_sheet(self, period_type: str) -> None:
        """
        Handle the insertion of balance sheet data into the database.

        :param period_type: The period type of the data (e.g., 'annual' or 'quarterly').
        """

        # switch period_type:
        match period_type:
            case 'annual':
                balance_sheet = safe_execute(None, lambda: getattr(self.stock, "balance_sheet"))
            case 'quarterly':
                balance_sheet = safe_execute(None, lambda: getattr(self.stock, "quarterly_balance_sheet"))
            case _:
                LOGGER.error(f"{self.ticker.symbol} - Invalid period_type: {period_type}. Expected 'annual' or 'quarterly'.")
                return None

        # __ if balance_sheet is empty, return __
        if balance_sheet is None or (isinstance(balance_sheet, pd.DataFrame) and balance_sheet.empty):
            LOGGER.warning(f"{self.ticker.symbol} - {'Balance Sheet'.rjust(50)} - no data to insert")
            return None

        # Transpose the DataFrame to have dates as rows
        balance_sheet = balance_sheet.T

        # Give "Date" name to index
        balance_sheet.index.name = "Date"

        # Reset the index to include the date as a column
        balance_sheet.reset_index(inplace=True)

        # Normalize column names and handle any renaming needed (matching the SQLAlchemy model attributes)
        balance_sheet.columns = [col.replace(' ', '_').lower() for col in balance_sheet.columns]

        for _, row in balance_sheet.iterrows():
            date_ = row['date']

            # Check if a record with the same ticker_id, date, and period_type already exists
            existing_record = self.session.query(BalanceSheet).filter(
                and_(
                    BalanceSheet.ticker_id == self.ticker.id,
                    BalanceSheet.date == date_,
                    BalanceSheet.period_type == period_type
                )
            ).first()

            if not existing_record:
                # Create a new BalanceSheet object
                new_balance_sheet = BalanceSheet(
                    ticker_id=self.ticker.id,
                    date=date_,
                    period_type=period_type,
                    treasury_shares_number=row.get('treasury_shares_number', None),
                    ordinary_shares_number=row.get('ordinary_shares_number', None),
                    share_issued=row.get('share_issued', None),
                    net_debt=row.get('net_debt', None),
                    total_debt=row.get('total_debt', None),
                    tangible_book_value=row.get('tangible_book_value', None),
                    invested_capital=row.get('invested_capital', None),
                    working_capital=row.get('working_capital', None),
                    net_tangible_assets=row.get('net_tangible_assets', None),
                    common_stock_equity=row.get('common_stock_equity', None),
                    total_capitalization=row.get('total_capitalization', None),
                    total_equity_gross_minority_interest=row.get('total_equity_gross_minority_interest', None),
                    stockholders_equity=row.get('stockholders_equity', None),
                    gains_losses_not_affecting_retained_earnings=row.get('gains_losses_not_affecting_retained_earnings', None),
                    other_equity_adjustments=row.get('other_equity_adjustments', None),
                    retained_earnings=row.get('retained_earnings', None),
                    capital_stock=row.get('capital_stock', None),
                    common_stock=row.get('common_stock', None),
                    total_liabilities_net_minority_interest=row.get('total_liabilities_net_minority_interest', None),
                    total_non_current_liabilities_net_minority_interest=row.get('total_non_current_liabilities_net_minority_interest', None),
                    other_non_current_liabilities=row.get('other_non_current_liabilities', None),
                    trade_and_other_payables_non_current=row.get('tradeand_other_payables_non_current', None),
                    long_term_debt_and_capital_lease_obligation=row.get('long_term_debt_and_capital_lease_obligation', None),
                    long_term_debt=row.get('long_term_debt', None),
                    current_liabilities=row.get('current_liabilities', None),
                    other_current_liabilities=row.get('other_current_liabilities', None),
                    current_deferred_liabilities=row.get('current_deferred_liabilities', None),
                    current_deferred_revenue=row.get('current_deferred_revenue', None),
                    current_debt_and_capital_lease_obligation=row.get('current_debt_and_capital_lease_obligation', None),
                    current_debt=row.get('current_debt', None),
                    other_current_borrowings=row.get('other_current_borrowings', None),
                    commercial_paper=row.get('commercial_paper', None),
                    payables_and_accrued_expenses=row.get('payables_and_accrued_expenses', None),
                    payables=row.get('payables', None),
                    accounts_payable=row.get('accounts_payable', None),
                    total_assets=row.get('total_assets', None),
                    total_non_current_assets=row.get('total_non_current_assets', None),
                    other_non_current_assets=row.get('other_non_current_assets', None),
                    non_current_deferred_assets=row.get('non_current_deferred_assets', None),
                    non_current_deferred_taxes_assets=row.get('non_current_deferred_taxes_assets', None),
                    investments_and_advances=row.get('investments_and_advances', None),
                    other_investments=row.get('other_investments', None),
                    investment_in_financial_assets=row.get('investmentin_financial_assets', None),
                    available_for_sale_securities=row.get('available_for_sale_securities', None),
                    net_ppe=row.get('net_ppe', None),
                    accumulated_depreciation=row.get('accumulated_depreciation', None),
                    gross_ppe=row.get('gross_ppe', None),
                    leases=row.get('leases', None),
                    machinery_furniture_equipment=row.get('machinery_furniture_equipment', None),
                    land_and_improvements=row.get('land_and_improvements', None),
                    properties=row.get('properties', None),
                    current_assets=row.get('current_assets', None),
                    other_current_assets=row.get('other_current_assets', None),
                    inventory=row.get('inventory', None),
                    receivables=row.get('receivables', None),
                    other_receivables=row.get('other_receivables', None),
                    accounts_receivable=row.get('accounts_receivable', None),
                    cash_cash_equivalents_and_short_term_investments=row.get('cash_cash_equivalents_and_short_term_investments', None),
                    other_short_term_investments=row.get('other_short_term_investments', None),
                    cash_and_cash_equivalents=row.get('cash_and_cash_equivalents', None),
                    cash_equivalents=row.get('cash_equivalents', None),
                    cash_financial=row.get('cash_financial', None)
                )

                try:
                    # Add the object to the session
                    self.session.add(new_balance_sheet)
                    # Commit the session to the database
                    self.commit()
                    LOGGER.info(f"{self.ticker.symbol} - {'Balance Sheet'.rjust(50)} - inserted balance sheet for date {date_} ({period_type})")

                except Exception as e:
                    # Rollback the transaction in case of an error
                    self.session.rollback()
                    LOGGER.error(f"Error occurred: {e}")

    def handle_cash_flow(self, period_type: str) -> None:
        """
        Handle the insertion of cash flow data into the database.

        :param period_type: The period type of the data (e.g., 'annual' or 'quarterly').
        """

        # switch period_type:
        match period_type:
            case 'annual':
                cash_flow = safe_execute(None, lambda: getattr(self.stock, "cashflow"))
            case 'quarterly':
                cash_flow = safe_execute(None, lambda: getattr(self.stock, "quarterly_cashflow"))
            case _:
                LOGGER.error(f"{self.ticker.symbol} - Invalid period_type: {period_type}. Expected 'annual' or 'quarterly'.")
                return None

        # __ if cash_flow is empty, return __
        if cash_flow is None or (isinstance(cash_flow, pd.DataFrame) and cash_flow.empty):
            LOGGER.warning(f"{self.ticker.symbol} - {'Cash Flow'.rjust(50)} - no data to insert")
            return None

        # Transpose the DataFrame to have dates as rows
        cash_flow = cash_flow.T

        # Give "Date" name to index
        cash_flow.index.name = "Date"

        # Reset the index to include the date as a column
        cash_flow.reset_index(inplace=True)

        # Normalize column names and handle any renaming needed (matching the SQLAlchemy model attributes)
        cash_flow.columns = [col.replace(' ', '_').lower() for col in cash_flow.columns]

        for _, row in cash_flow.iterrows():
            date_ = row['date']

            # Check if a record with the same ticker_id, date, and period_type already exists
            existing_record = self.session.query(CashFlow).filter(
                and_(
                    CashFlow.ticker_id == self.ticker.id,
                    CashFlow.date == date_,
                    CashFlow.period_type == period_type
                )
            ).first()

            if not existing_record:
                # Create a new CashFlow object
                new_cash_flow = CashFlow(
                    ticker_id=self.ticker.id,
                    date=date_,
                    period_type=period_type,
                    free_cash_flow=row.get('free_cash_flow', None),
                    repurchase_of_capital_stock=row.get('repurchase_of_capital_stock', None),
                    repayment_of_debt=row.get('repayment_of_debt', None),
                    issuance_of_debt=row.get('issuance_of_debt', None),
                    issuance_of_capital_stock=row.get('issuance_of_capital_stock', None),
                    capital_expenditure=row.get('capital_expenditure', None),
                    interest_paid_supplemental_data=row.get('interest_paid_supplemental_data', None),
                    income_tax_paid_supplemental_data=row.get('income_tax_paid_supplemental_data', None),
                    end_cash_position=row.get('end_cash_position', None),
                    beginning_cash_position=row.get('beginning_cash_position', None),
                    changes_in_cash=row.get('changes_in_cash', None),
                    financing_cash_flow=row.get('financing_cash_flow', None),
                    cash_flow_from_continuing_financing_activities=row.get('cash_flow_from_continuing_financing_activities', None),
                    net_other_financing_charges=row.get('net_other_financing_charges', None),
                    cash_dividends_paid=row.get('cash_dividends_paid', None),
                    common_stock_dividend_paid=row.get('common_stock_dividend_paid', None),
                    net_common_stock_issuance=row.get('net_common_stock_issuance', None),
                    common_stock_payments=row.get('common_stock_payments', None),
                    common_stock_issuance=row.get('common_stock_issuance', None),
                    net_issuance_payments_of_debt=row.get('net_issuance_payments_of_debt', None),
                    net_short_term_debt_issuance=row.get('net_short_term_debt_issuance', None),
                    net_long_term_debt_issuance=row.get('net_long_term_debt_issuance', None),
                    long_term_debt_payments=row.get('long_term_debt_payments', None),
                    long_term_debt_issuance=row.get('long_term_debt_issuance', None),
                    investing_cash_flow=row.get('investing_cash_flow', None),
                    cash_flow_from_continuing_investing_activities=row.get('cash_flow_from_continuing_investing_activities', None),
                    net_other_investing_changes=row.get('net_other_investing_changes', None),
                    net_investment_purchase_and_sale=row.get('net_investment_purchase_and_sale', None),
                    sale_of_investment=row.get('sale_of_investment', None),
                    purchase_of_investment=row.get('purchase_of_investment', None),
                    net_business_purchase_and_sale=row.get('net_business_purchase_and_sale', None),
                    purchase_of_business=row.get('purchase_of_business', None),
                    net_ppe_purchase_and_sale=row.get('net_ppe_purchase_and_sale', None),
                    purchase_of_ppe=row.get('purchase_of_ppe', None),
                    operating_cash_flow=row.get('operating_cash_flow', None),
                    cash_flow_from_continuing_operating_activities=row.get('cash_flow_from_continuing_operating_activities', None),
                    change_in_working_capital=row.get('change_in_working_capital', None),
                    change_in_other_working_capital=row.get('change_in_other_working_capital', None),
                    change_in_other_current_liabilities=row.get('change_in_other_current_liabilities', None),
                    change_in_other_current_assets=row.get('change_in_other_current_assets', None),
                    change_in_payables_and_accrued_expense=row.get('change_in_payables_and_accrued_expense', None),
                    change_in_payable=row.get('change_in_payable', None),
                    change_in_account_payable=row.get('change_in_account_payable', None),
                    change_in_inventory=row.get('change_in_inventory', None),
                    change_in_receivables=row.get('change_in_receivables', None),
                    changes_in_account_receivables=row.get('changes_in_account_receivables', None),
                    other_non_cash_items=row.get('other_non_cash_items', None),
                    stock_based_compensation=row.get('stock_based_compensation', None),
                    deferred_tax=row.get('deferred_tax', None),
                    deferred_income_tax=row.get('deferred_income_tax', None),
                    depreciation_amortization_depletion=row.get('depreciation_amortization_depletion', None),
                    depreciation_and_amortization=row.get('depreciation_and_amortization', None),
                    net_income_from_continuing_operations=row.get('net_income_from_continuing_operations', None)
                )

                try:
                    # Add the object to the session
                    self.session.add(new_cash_flow)
                    # Commit the session to the database
                    self.commit()
                    LOGGER.info(f"{self.ticker.symbol} - {'Cash Flow'.rjust(50)} - inserted cash flow for date {date_} ({period_type})")

                except Exception as e:
                    # Rollback the transaction in case of an error
                    self.session.rollback()
                    LOGGER.error(f"Error occurred: {e}")

    def handle_financials(self, period_type: str) -> None:
        """
        Handle the insertion of financials data into the database.

        :param stock: yf.Ticker object containing the insider purchases data.
        :param period_type: The period type of the data (e.g., 'annual' or 'quarterly').
        """

        #switch period_type:
        match period_type:
            case 'annual':
                financials = safe_execute(None, lambda: getattr(self.stock, "financials"))
            case 'quarterly':
                financials = safe_execute(None, lambda: getattr(self.stock, "quarterly_financials"))
            case _:
                LOGGER.error(f"{self.ticker.symbol} - Invalid period_type: {period_type}. Expected 'annual' or 'quarterly'.")
                return None

        # __ if financials is empty, return __
        if financials is None or (isinstance(financials, pd.DataFrame) and financials.empty):
            LOGGER.warning(f"{self.ticker.symbol} - {'Financials'.rjust(50)} - no data to insert")
            return None

        # Transpose the DataFrame to have dates as rows
        financials = financials.T

        # Give "Date" name to index
        financials.index.name = "Date"

        # Reset the index to include the date as a column
        financials.reset_index(inplace=True)

        # Normalize column names and handle any renaming needed (matching the SQLAlchemy model attributes)
        financials.columns = [col.replace(' ', '_').lower() for col in financials.columns]

        for _, row in financials.iterrows():
            date_ = row['date']

            # Check if a record with the same ticker_id, date, and period_type already exists
            existing_record = self.session.query(Financials).filter(
                and_(
                    Financials.ticker_id == self.ticker.id,
                    Financials.date == date_,
                    Financials.period_type == period_type
                )
            ).first()

            if not existing_record:
                # Create a new Financials object
                new_financial = Financials(
                    ticker_id=self.ticker.id,
                    date=date_,
                    period_type=period_type,
                    tax_effect_of_unusual_items=row.get('tax_effect_of_unusual_items', None),
                    tax_rate_for_calcs=row.get('tax_rate_for_calcs', None),
                    normalized_ebitda=row.get('normalized_ebitda', None),
                    net_income_from_continuing_operation_net_minority_interest=row.get('net_income_from_continuing_operation_net_minority_interest', None),
                    reconciled_depreciation=row.get('reconciled_depreciation', None),
                    reconciled_cost_of_revenue=row.get('reconciled_cost_of_revenue', None),
                    ebitda=row.get('ebitda', None),
                    ebit=row.get('ebit', None),
                    net_interest_income=row.get('net_interest_income', None),
                    interest_expense=row.get('interest_expense', None),
                    interest_income=row.get('interest_income', None),
                    normalized_income=row.get('normalized_income', None),
                    net_income_from_continuing_and_discontinued_operation=row.get('net_income_from_continuing_and_discontinued_operation', None),
                    total_expenses=row.get('total_expenses', None),
                    total_operating_income_as_reported=row.get('total_operating_income_as_reported', None),
                    diluted_average_shares=row.get('diluted_average_shares', None),
                    basic_average_shares=row.get('basic_average_shares', None),
                    diluted_eps=row.get('diluted_eps', None),
                    basic_eps=row.get('basic_eps', None),
                    diluted_net_income_available_to_common_stockholders=row.get('diluted_ni_availto_com_stockholders', None),
                    net_income_common_stockholders=row.get('net_income_common_stockholders', None),
                    net_income=row.get('net_income', None),
                    net_income_including_non_controlling_interests=row.get('net_income_including_noncontrolling_interests', None),
                    net_income_continuous_operations=row.get('net_income_continuous_operations', None),
                    tax_provision=row.get('tax_provision', None),
                    pretax_income=row.get('pretax_income', None),
                    other_income_expense=row.get('other_income_expense', None),
                    other_non_operating_income_expenses=row.get('other_non_operating_income_expenses', None),
                    net_non_operating_interest_income_expense=row.get('net_non_operating_interest_income_expense', None),
                    interest_expense_non_operating=row.get('interest_expense_non_operating', None),
                    interest_income_non_operating=row.get('interest_income_non_operating', None),
                    operating_income=row.get('operating_income', None),
                    operating_expense=row.get('operating_expense', None),
                    research_and_development=row.get('research_and_development', None),
                    selling_general_and_administration=row.get('selling_general_and_administration', None),
                    gross_profit=row.get('gross_profit', None),
                    cost_of_revenue=row.get('cost_of_revenue', None),
                    total_revenue=row.get('total_revenue', None),
                    operating_revenue=row.get('operating_revenue', None)
                )

                try:
                    # Add the object to the session
                    self.session.add(new_financial)
                    # Commit the session to the database
                    self.commit()
                    LOGGER.info(f"{self.ticker.symbol} - {'Financials'.rjust(50)} - inserted financials for date {date_} ({period_type})")

                except Exception as e:
                    # Rollback the transaction in case of an error
                    self.session.rollback()
                    LOGGER.error(f"Error occurred: {e}")

    def handle_actions(self) -> None:
        """
        Handle the insertion or update of actions data into the database.

        :param stock: yf.Ticker object containing the insider purchases data.
        """

        actions = safe_execute(None, lambda: getattr(self.stock, "actions"))

        # __ if actions is empty, return __
        if actions is None or (isinstance(actions, pd.DataFrame) and actions.empty):
            LOGGER.warning(f"{self.ticker.symbol} - {'Actions'.rjust(50)} - no data to insert")
            return None

        # __ rename and reset the index and convert it to date __
        actions.reset_index(inplace=True)
        actions['Date'] = pd.to_datetime(actions['Date']).dt.tz_localize(None)

        # __ normalize column names to match SQLAlchemy model attributes __
        actions.columns = [col.lower().replace(' ', '_') for col in actions.columns]

        # __ convert the date column to datetime.date objects
        actions['date'] = pd.to_datetime(actions['date']).dt.date

        # __ call the bulk update handler __
        self.handle_generic_bulk_update(
            new_data_df=actions,
            model_class=Action
        )

    def handle_calendar(self) -> None:
        """
        Handle the insertion or update of calendar dates into the DB.

        :param stock: yf.Ticker object containing the insider purchases data.
        """

        calendar = safe_execute(None, lambda: getattr(self.stock, "calendar"))

        # __ if calendar is empty, return __
        if not calendar or not any(calendar.values()):
            LOGGER.warning(f"{self.ticker.symbol} - {'Calendar'.rjust(50)} - no data to insert")
            return None

        new_record_data = {
            'dividend_date': calendar.get('Dividend Date', None),
            'ex_dividend_date': calendar.get('Ex-Dividend Date', None),
            'earnings_high': calendar.get('Earnings High', None),
            'earnings_low': calendar.get('Earnings Low', None),
            'earnings_average': calendar.get('Earnings Average', None),
            'revenue_high': calendar.get('Revenue High', None),
            'revenue_low': calendar.get('Revenue Low', None),
            'revenue_average': calendar.get('Revenue Average', None)
        }

        self.handle_generic_record_update(
            new_record_data=new_record_data,
            model_class=Calendar
        )

    def handle_earnings_dates(self) -> None:
        """
        Handle the insertion of earnings dates data into the database.

        :param stock: yf.Ticker object containing the insider purchases data.
        """

        earnings_dates = safe_execute(None, lambda: getattr(self.stock, "earnings_dates"))

        # __ if earnings_dates is empty, return __
        if earnings_dates is None or (isinstance(earnings_dates, pd.DataFrame) and earnings_dates.empty):
            LOGGER.warning(f"{self.ticker.symbol} - {'Earnings Dates'.rjust(50)} - no data to insert")
            return None

        # __ rename and reset the index and convert it to date __
        earnings_dates.index.name = "date"
        earnings_dates.reset_index(inplace=True)
        earnings_dates['date'] = pd.to_datetime(earnings_dates['date']).dt.date  # Ensure it's date only

        # __ rename columns to match the database columns __
        earnings_dates.rename(columns={
            'Surprise(%)': 'Surprise Percent'
        }, inplace=True)

        def determine_earnings_period(earnings_date: date) -> str:
            """
            Determine the earnings period (Q1, Q2, Q3, Q4) based on the earnings date.

            :param earnings_date: The date of the earnings report.
            :return: A string representing the earnings period (e.g., 'Q1', 'Q2', 'Q3', 'Q4').
            """
            month = earnings_date.month

            if 1 <= month <= 3:
                return 'Q4'  # Earnings for the fourth quarter of the previous year
            elif 4 <= month <= 6:
                return 'Q1'  # Earnings for the first quarter of the current year
            elif 7 <= month <= 9:
                return 'Q2'  # Earnings for the second quarter of the current year
            elif 10 <= month <= 12:
                return 'Q3'  # Earnings for the third quarter of the current year
            else:
                raise ValueError("Invalid month for determining earnings period")

        earnings_dates['earnings_period'] = earnings_dates['date'].apply(determine_earnings_period)

        # __ call the bulk update handler __
        self.handle_generic_bulk_update(
            new_data_df=earnings_dates,
            model_class=EarningsDates
        )

    def handle_ticker_status(self, status: str, error: str) -> None:
        """
        Handle the insertion or update of ticker status into the database.

        :param status: The status of the ticker (e.g., 'active', 'inactive').
        :param error: Any error message associated with the ticker.
        """

        new_record_data = {
            'status': status,
            'yfinance_error': error
        }

        self.handle_generic_record_update(
            new_record_data=new_record_data,
            model_class=TickerStatus
        )

    def handle_info_company_address(self) -> None:
        """
        Handle the insertion or update of company address information into the database.

        :param info_data: Dictionary containing the company address information from stock.info.
        """

        # __ if info_data is empty, return __
        if not self.info:
            LOGGER.warning(f"{self.ticker.symbol} - {'Company Address'.rjust(50)} - no data to insert")
            return None

        # Prepare the new record data with default values as None
        new_record_data = {
            'address1': self.info.get('address1', None),
            'city': self.info.get('city', None),
            'state': self.info.get('state', None),
            'zip': self.info.get('zip', None),
            'country': self.info.get('country', None),
            'phone': self.info.get('phone', None),
            'website': self.info.get('website', None),
            'ir_website': self.info.get('irWebsite', None)
        }

        # Call the generic function to handle insert/update
        self.handle_generic_record_update(
            new_record_data=new_record_data,
            model_class=InfoCompanyAddress
        )

    def handle_sector_industry_history(self) -> None:
        """
        Handle the insertion or update of sector and industry history data into the database.

        :param info_data: Dictionary containing the stock information, including sector and industry.
        """

        # __ if info_data is empty, return __
        if not self.info:
            LOGGER.warning(f"{self.ticker.symbol} - {'Sector Industry History'.rjust(50)} - no data to insert")
            return None

        new_record_data = {
            'sector': self.info.get('sector', None),
            'industry': self.info.get('industry', None)
        }

        if new_record_data['sector'] is None or new_record_data['industry'] is None:
            LOGGER.warning(f"{self.ticker.symbol} - {'Sector Industry History'.rjust(50)} - no data to insert")
            return None

        self.handle_generic_record_update(
            new_record_data=new_record_data,
            model_class=InfoSectorIndustryHistory
        )

    def handle_info_target_price_and_recommendation(self) -> None:
        """
        Handle the insertion or update of target price and recommendation info into the database.

        :param info_data: Dictionary containing the target price and recommendation data.
        """

        # __ if info_data is empty, return __
        if not self.info:
            LOGGER.warning(f"{self.ticker.symbol} - {'Target Price and Recommendation'.rjust(50)} - no data to insert")
            return None

        new_record_data = {
            'target_high_price': self.info.get('targetHighPrice', None),
            'target_low_price': self.info.get('targetLowPrice', None),
            'target_mean_price': self.info.get('targetMeanPrice', None),
            'target_median_price': self.info.get('targetMedianPrice', None),
            'recommendation_mean': self.info.get('recommendationMean', None),
            'recommendation_key': self.info.get('recommendationKey', None),
            'number_of_analyst_opinions': self.info.get('numberOfAnalystOpinions', None)
        }

        self.handle_generic_record_update(
            new_record_data=new_record_data,
            model_class=InfoTargetPriceAndRecommendation
        )

    def handle_info_governance(self) -> None:
        """
        Handle the insertion or update of governance info into the database.

        :param governance_datainfo_data: Dictionary containing the governance data.
        """

        # __ if info_data is empty, return __
        if not self.info:
            LOGGER.warning(f"{self.ticker.symbol} - {'Governance'.rjust(50)} - no data to insert")
            return None

        # Prepare the new record data for InfoGovernance
        new_record_data = {
            'audit_risk': self.info.get('auditRisk', None),
            'board_risk': self.info.get('boardRisk', None),
            'compensation_risk': self.info.get('compensationRisk', None),
            'shareholder_rights_risk': self.info.get('shareHolderRightsRisk', None),
            'overall_risk': self.info.get('overallRisk', None),
            'governance_epoch_date': self.info.get('governanceEpochDate', None),
            'compensation_as_of_epoch_date': self.info.get('compensationAsOfEpochDate', None)
        }

        # Call the generic function to handle the update or insertion
        self.handle_generic_record_update(
            new_record_data=new_record_data,
            model_class=InfoGovernance,
            additional_filters=None  # No additional filters needed in this case
        )

    def handle_info_cash_and_financial_ratios(self) -> None:
        """
        Handle the insertion or update of cash and financial ratios info into the database.

        :param info_data: Dictionary containing the financial ratios data.
        """

        # __ if info_data is empty, return __
        if not self.info:
            LOGGER.warning(f"{self.ticker.symbol} - {'Cash and Financial Ratios'.rjust(50)} - no data to insert")
            return None

        new_record_data = {
            'total_cash': self.info.get('totalCash', None),
            'total_cash_per_share': self.info.get('totalCashPerShare', None),
            'ebitda': self.info.get('ebitda', None),
            'total_debt': self.info.get('totalDebt', None),
            'quick_ratio': self.info.get('quickRatio', None),
            'current_ratio': self.info.get('currentRatio', None),
            'total_revenue': self.info.get('totalRevenue', None),
            'debt_to_equity': self.info.get('debtToEquity', None),
            'revenue_per_share': self.info.get('revenuePerShare', None),
            'return_on_assets': self.info.get('returnOnAssets', None),
            'return_on_equity': self.info.get('returnOnEquity', None),
            'free_cashflow': self.info.get('freeCashflow', None),
            'operating_cashflow': self.info.get('operatingCashflow', None),
            'earnings_growth': self.info.get('earningsGrowth', None),
            'revenue_growth': self.info.get('revenueGrowth', None),
            'gross_margins': self.info.get('grossMargins', None),
            'ebitda_margins': self.info.get('ebitdaMargins', None),
            'operating_margins': self.info.get('operatingMargins', None),
            'trailing_peg_ratio': self.info.get('trailingPegRatio', None)
        }

        self.handle_generic_record_update(
            new_record_data=new_record_data,
            model_class=InfoCashAndFinancialRatios
        )

    def handle_info_market_and_financial_metrics(self) -> None:
        """
        Handle the insertion or update of market and financial metrics info into the database.

        :param info_data: Dictionary containing the market and financial metrics data.
        """

        # __ if info_data is empty, return __
        if not self.info:
            LOGGER.warning(f"{self.ticker.symbol} - {'Market and Financial Metrics'.rjust(50)} - no data to insert")
            return None

        # Extract the new data from the info_data dictionary
        new_record_data = {
            'dividend_rate': self.info.get('dividendRate', None),
            'dividend_yield': self.info.get('dividendYield', None),
            'ex_dividend_date': self.info.get('exDividendDate', None),
            'payout_ratio': self.info.get('payoutRatio', None),
            'five_year_avg_dividend_yield': self.info.get('fiveYearAvgDividendYield', None),
            'trailing_annual_dividend_rate': self.info.get('trailingAnnualDividendRate', None),
            'trailing_annual_dividend_yield': self.info.get('trailingAnnualDividendYield', None),
            'last_dividend_value': self.info.get('lastDividendValue', None),
            'last_dividend_date': self.info.get('lastDividendDate', None),

            'average_volume': self.info.get('averageVolume', None),
            'average_volume_10days': self.info.get('averageVolume10days', None),
            'average_daily_volume_10day': self.info.get('averageDailyVolume10Day', None),

            'enterprise_value': self.info.get('enterpriseValue', None),
            'book_value': self.info.get('bookValue', None),
            'enterprise_to_revenue': self.info.get('enterpriseToRevenue', None),
            'enterprise_to_ebitda': self.info.get('enterpriseToEbitda', None),

            'shares_short': self.info.get('sharesShort', None),
            'shares_short_prior_month': self.info.get('sharesShortPriorMonth', None),
            'shares_short_previous_month_date': self.info.get('sharesShortPreviousMonthDate', None),
            'date_short_interest': self.info.get('dateShortInterest', None),
            'shares_percent_shares_out': self.info.get('sharesPercentSharesOut', None),
            'held_percent_insiders': self.info.get('heldPercentInsiders', None),
            'held_percent_institutions': self.info.get('heldPercentInstitutions', None),
            'short_ratio': self.info.get('shortRatio', None),
            'short_percent_of_float': self.info.get('shortPercentOfFloat', None),
            'implied_shares_outstanding': self.info.get('impliedSharesOutstanding', None),
            'float_shares': self.info.get('floatShares', None),
            'shares_outstanding': self.info.get('sharesOutstanding', None),

            'earnings_quarterly_growth': self.info.get('earningsQuarterlyGrowth', None),
            'net_income_to_common': self.info.get('netIncomeToCommon', None),
            'trailing_eps': self.info.get('trailingEps', None),
            'forward_eps': self.info.get('forwardEps', None),
            'peg_ratio': self.info.get('pegRatio', None),

            'last_split_factor': self.info.get('lastSplitFactor', None),
            'last_split_date': self.info.get('lastSplitDate', None),

            'beta': self.info.get('beta', None),

            'profit_margins': self.info.get('profitMargins', None),
            'fifty_two_week_change': self.info.get('52WeekChange', None),
            'sp_fifty_two_week_change': self.info.get('SandP52WeekChange', None),
            'last_fiscal_year_end': self.info.get('lastFiscalYearEnd', None),
            'next_fiscal_year_end': self.info.get('nextFiscalYearEnd', None),
            'most_recent_quarter': self.info.get('mostRecentQuarter', None)
        }

        self.handle_generic_record_update(
            new_record_data=new_record_data,
            model_class=InfoMarketAndFinancialMetrics
        )

    def handle_info_general_stock(self) -> None:
        """
        Handle the insertion or update of general stock information into the database.

        :param info_data: Dictionary containing the general stock information from stock.info.
        """

        isin = safe_execute(None, lambda: getattr(self.stock, "isin"))
        history_metadata = safe_execute(None, lambda: getattr(self.stock, "history_metadata"))

        # __ if info_data is empty, return __
        if not self.info or not history_metadata or not isin:
            LOGGER.warning(f"{self.ticker.symbol} - {'General Stock Info'.rjust(50)} - no data to insert")
            return None

        # Prepare the new record data combining both info_data and metadata_data
        new_record_data = {
            'isin': isin,
            'currency': self.info.get('currency', None),
            'symbol': self.info.get('symbol', None),
            'exchange': self.info.get('exchange', None),
            'quote_type': self.info.get('quoteType', None),
            'underlying_symbol': self.info.get('underlyingSymbol', None),
            'short_name': self.info.get('shortName', None),
            'long_name': self.info.get('longName', None),
            'first_trade_date_epoch_utc': self.info.get('firstTradeDateEpochUtc', None),
            'time_zone_full_name': self.info.get('timeZoneFullName', None),
            'time_zone_short_name': self.info.get('timeZoneShortName', None),
            'uuid': self.info.get('uuid', None),
            'message_board_id': self.info.get('messageBoardId', None),
            'gmt_offset_milliseconds': self.info.get('gmtOffSetMilliseconds', None),
            'price_hint': self.info.get('priceHint', None),
            'max_age': self.info.get('maxAge', None),
            'full_time_employees': self.info.get('fullTimeEmployees', None),

            # Fields from history_metadata
            'full_exchange_name': history_metadata.get('fullExchangeName', None),
            'instrument_type': history_metadata.get('instrumentType', None),
            'has_pre_post_market_data': history_metadata.get('hasPrePostMarketData', None),
            'gmt_offset': history_metadata.get('gmtoffset', None),
            'chart_previous_close': history_metadata.get('chartPreviousClose', None),
            'data_granularity': history_metadata.get('dataGranularity', None)
        }

        # Call the generic function to handle insert/update
        self.handle_generic_record_update(
            new_record_data=new_record_data,
            model_class=InfoGeneralStock
        )

    def handle_info_trading_session(self) -> None:
        """
        Handle the insertion or update of trading session information into the database.
        """

        basic_info = safe_execute(None, lambda: getattr(self.stock, "fast_info"))
        history_metadata = safe_execute(None, lambda: getattr(self.stock, "history_metadata"))

        # __ if info is empty, return __
        if not self.info or not history_metadata: # TODO: log based on basic_info
            LOGGER.warning(f"{self.ticker.symbol} - {'Trading Session Info'.rjust(50)} - no data to insert")
            return None

        # Prepare the new record data with default values as None
        new_record_data = {
            # From history metadata (dynamic data)
            'regular_market_time': history_metadata.get('regularMarketTime', None),
            'regular_market_price': history_metadata.get('regularMarketPrice', None),
            'fifty_two_week_high': history_metadata.get('fiftyTwoWeekHigh', None),
            'fifty_two_week_low': history_metadata.get('fiftyTwoWeekLow', None),
            'regular_market_day_high': history_metadata.get('regularMarketDayHigh', None),
            'regular_market_day_low': history_metadata.get('regularMarketDayLow', None),
            'regular_market_volume': history_metadata.get('regularMarketVolume', None),

            # From info (dynamic and static data)
            'current_price': self.info.get('currentPrice', None),
            'open': self.info.get('open', None),
            'previous_close': self.info.get('previousClose', None),
            'regular_market_previous_close': self.info.get('regularMarketPreviousClose', None),
            'day_high': self.info.get('dayHigh', None),
            'day_low': self.info.get('dayLow', None),
            'market_cap': self.info.get('marketCap', None),
            'regular_market_open': self.info.get('regularMarketOpen', None),
            'trailing_pe': self.info.get('trailingPE', None),
            'forward_pe': self.info.get('forwardPE', None),
            'volume': self.info.get('volume', None),
            'bid': self.info.get('bid', None),
            'ask': self.info.get('ask', None),
            'bid_size': self.info.get('bidSize', None),
            'ask_size': self.info.get('askSize', None),
            'price_to_sales_trailing_12months': self.info.get('priceToSalesTrailing12Months', None),
            'price_to_book': self.info.get('priceToBook', None),
            'fifty_day_average': self.info.get('fiftyDayAverage', None),
            'two_hundred_day_average': self.info.get('twoHundredDayAverage', None)
        }

        try:
            # From basic info (dynamic and static data)
            new_record_basic_info = {
                'last_price': basic_info.get('lastPrice', None),
                'last_volume': basic_info.get('lastVolume', None),
                'ten_day_average_volume': basic_info.get('tenDayAverageVolume', None),
                'three_month_average_volume': basic_info.get('threeMonthAverageVolume', None),
                'year_change': basic_info.get('yearChange', None),
                'year_high': basic_info.get('yearHigh', None),
                'year_low': basic_info.get('yearLow', None)
            }
        except:
            new_record_basic_info = {
                'last_price': None,
                'last_volume': None,
                'ten_day_average_volume': None,
                'three_month_average_volume': None,
                'year_change': None,
                'year_high': None,
                'year_low': None
            }

        new_record_data = {
            **new_record_data,
            **new_record_basic_info
        }

        if type(new_record_data['trailing_pe']) == str:
            new_record_data['trailing_pe'] = np.nan
            LOGGER.warning(f"{self.ticker.symbol} - {'Info Trading Session'.rjust(50)} - trailing_pe converted to np.nan")

        if type(new_record_data['forward_pe']) == str:
            new_record_data['forward_pe'] = np.nan
            LOGGER.warning(f"{self.ticker.symbol} - {'Info Trading Session'.rjust(50)} - forward_pe converted to np.nan")

        if type(new_record_data['price_to_sales_trailing_12months']) == str:
            new_record_data['price_to_sales_trailing_12months'] = np.nan
            LOGGER.warning(f"{self.ticker.symbol} - {'Info Trading Session'.rjust(50)} - price_to_sales_trailing_12months converted to np.nan")

        # Call the generic function to handle insert/update
        self.handle_generic_record_update(
            new_record_data=new_record_data,
            model_class=InfoTradingSession
        )

    def handle_insider_purchases(self) -> None:
        """
        Handle the insertion or update of insider purchases data into the database.

        :param stock: yf.Ticker object containing the insider purchases data.
        """

        insider_purchases = safe_execute(None, lambda: getattr(self.stock, "insider_purchases"))

        # __ if insider_purchases is empty, return __
        if insider_purchases is None or (isinstance(insider_purchases, pd.DataFrame) and insider_purchases.empty):
            LOGGER.warning(f"{self.ticker.symbol} - {'Insider Purchases'.rjust(50)} - no data to insert")
            return None

        # __ substitute NaN values with 0 in the 'Shares' column __
        insider_purchases['Shares'] = insider_purchases['Shares'].fillna(0)  # TODO: review this operation
        insider_purchases['Trans'] = insider_purchases['Trans'].fillna(0)

        # Define a mapping from the DataFrame index to the corresponding database columns
        type_mapping = {
            'Purchases': {
                'shares_column': 'purchases_shares',
                'transactions_column': 'purchases_transactions'
            },
            'Sales': {
                'shares_column': 'sales_shares',
                'transactions_column': 'sales_transactions'
            },
            'Net Shares Purchased (Sold)': {
                'shares_column': 'net_shares_purchased_sold',
                'transactions_column': 'net_shares_purchased_sold_transactions'
            },
            'Total Insider Shares Held': {
                'shares_column': 'total_insider_shares_held',
                'transactions_column': None  # This row does not have associated transactions
            },
            '% Net Shares Purchased (Sold)': {
                'shares_column': 'percent_net_shares_purchased_sold',
                'transactions_column': None
            },
            '% Buy Shares': {
                'shares_column': 'percent_buy_shares',
                'transactions_column': None
            },
            '% Sell Shares': {
                'shares_column': 'percent_sell_shares',
                'transactions_column': None
            }
        }

        # Initialize a dictionary to hold the combined record data
        combined_record_data = {}

        # Iterate over each row in the insider_purchases DataFrame
        for _, row in insider_purchases.iterrows():
            # Determine the type of transaction from the DataFrame index
            transaction_type = row["Insider Purchases Last 6m"]  # Using the index name as the transaction type

            # Check if the transaction type is supported by the mapping
            if transaction_type in type_mapping:
                shares_column = type_mapping[transaction_type]['shares_column']
                transactions_column = type_mapping[transaction_type]['transactions_column']

                # Add shares data to the combined record
                combined_record_data[shares_column] = row['Shares']

                # Add transactions data to the combined record if it exists
                if transactions_column:
                    combined_record_data[transactions_column] = row['Trans']

        # If there's any data to insert or update, proceed
        if combined_record_data:
            # Handle updating or inserting the record in the database
            self.handle_generic_record_update(
                new_record_data=combined_record_data,
                model_class=InsiderPurchases
            )

    def handle_insider_roster_holders(self) -> None:
        """
        Handle the insertion or update of insider roster holders data into the database.

        :param stock: yf.Ticker object containing the insider purchases data.
        """

        insider_roster_holders = safe_execute(None, lambda: getattr(self.stock, "insider_roster_holders"))

        # __ if insider roster holders is empty, return __
        if insider_roster_holders is None or (isinstance(insider_roster_holders, pd.DataFrame) and insider_roster_holders.empty):
            LOGGER.warning(f"{self.ticker.symbol} - {'Insider Roster Holders'.rjust(50)} - no data to insert")
            return None

        # insider_roster_holders['Shares Owned Directly'] = insider_roster_holders['Shares Owned Directly'].apply(lambda x: int(x) if not pd.isna(x) else x)
        # insider_roster_holders['Shares Owned Indirectly'] = insider_roster_holders['Shares Owned Indirectly'].apply(lambda x: int(x) if not pd.isna(x) else x)

        # __ normalize column names to match SQLAlchemy model attributes __
        insider_roster_holders.columns = [col.lower().replace(' ', '_') for col in insider_roster_holders.columns]
        if 'shares_owned_directly' not in insider_roster_holders.columns:
            insider_roster_holders['shares_owned_directly'] = None
        if 'shares_owned_indirectly' not in insider_roster_holders.columns:
            insider_roster_holders['shares_owned_indirectly'] = None
        if 'position_indirect_date' not in insider_roster_holders.columns:
            insider_roster_holders['position_indirect_date'] = None
        if 'positionsummarydate' in insider_roster_holders.columns:
            insider_roster_holders.drop(columns='positionsummarydate', inplace=True)

        insider_roster_holders['shares_owned_directly'] = insider_roster_holders['shares_owned_directly'].fillna(0).astype('int64')  # TODO: improve this
        insider_roster_holders['shares_owned_indirectly'] = insider_roster_holders['shares_owned_indirectly'].fillna(0).astype('int64')

        # __ call the bulk update handler __
        self.handle_generic_bulk_update(
            new_data_df=insider_roster_holders,
            model_class=InsiderRosterHolders
        )

    def handle_insider_transactions(self) -> None:
        """
        Handle the bulk update or insertion of insider transactions data into the database.

        :param stock: yf.Ticker object containing the insider purchases data.
        """

        insider_transactions = safe_execute(None, lambda: getattr(self.stock, "insider_transactions"))

        # __ if insider transactions is empty, return __
        if insider_transactions is None or (isinstance(insider_transactions, pd.DataFrame) and insider_transactions.empty):
            LOGGER.warning(f"{self.ticker.symbol} - {'Insider Transactions'.rjust(50)} - no data to insert")
            return None

        # __ if insider_transactions is empty, return __
        if insider_transactions.empty:
            LOGGER.warning(f"{self.ticker.symbol} - {'Insider Transactions'.rjust(50)} - no data to insert")
            return None

        # __ normalize column names to match SQLAlchemy model attributes __
        insider_transactions.columns = [col.lower().replace(' ', '_') for col in insider_transactions.columns]

        # __ convert relevant columns to appropriate types __
        insider_transactions['start_date'] = pd.to_datetime(insider_transactions['start_date'])

        # __ call the bulk update handler __
        self.handle_generic_bulk_update(
            new_data_df=insider_transactions,
            model_class=InsiderTransactions
        )

    def handle_institutional_holders(self) -> None:
        """
        Handle the insertion or update of institutional holders data into the database.

        :param stock: yf.Ticker object containing the insider purchases data.
        """

        institutional_holders = safe_execute(None, lambda: getattr(self.stock, "institutional_holders"))

        # __ if institutional holders is empty, return __
        if institutional_holders is None or (isinstance(institutional_holders, pd.DataFrame) and institutional_holders.empty):
            LOGGER.warning(f"{self.ticker.symbol} - {'Institutional Holders'.rjust(50)} - no data to insert")
            return None

        # __ check if the DataFrame is empty __
        if institutional_holders.empty:
            LOGGER.warning(f"{self.ticker.symbol} - {'Institutional Holders'.rjust(50)} - no data to process")
            return

        # __ normalize column names to match SQLAlchemy model attributes __
        institutional_holders.columns = [col.lower().replace(' ', '_') for col in institutional_holders.columns]

        # __ convert the date_reported column to datetime.date objects (if it's not already in that format) __
        institutional_holders['date_reported'] = pd.to_datetime(institutional_holders['date_reported']).dt.date  # Ensure it's date only

        # __ rename columns to match the database columns __
        institutional_holders.rename(columns={
            'pctheld': 'pct_held'
        }, inplace=True)

        # __ call the bulk update handler __
        self.handle_generic_bulk_update(
            new_data_df=institutional_holders,
            model_class=InstitutionalHolders
        )

    def handle_major_holders(self) -> None:
        """
        Handle the insertion or update of major holders data into the database from a DataFrame.

        :param stock: yf.Ticker object containing the insider purchases data.
        """

        major_holders = safe_execute(None, lambda: getattr(self.stock, "major_holders"))

        # __ if major_holders is empty, return __
        if major_holders is None or (isinstance(major_holders, pd.DataFrame) and major_holders.empty):
            LOGGER.warning(f"{self.ticker.symbol} - {'Major Holders'.rjust(50)} - no data to insert")
            return None

        # Ensure the DataFrame has the correct structure
        if major_holders.empty or major_holders.index.empty:
            LOGGER.warning("Major holders DataFrame is empty or does not have the correct structure.")
            return

        # Transpose the DataFrame to switch columns to rows
        major_holders = major_holders.T

        if 'insidersPercentHeld' not in major_holders.columns or \
            'institutionsPercentHeld' not in major_holders.columns or \
            'institutionsFloatPercentHeld' not in major_holders.columns or \
            'institutionsCount' not in major_holders.columns:
            LOGGER.warning(f"{self.ticker.symbol} - {'Major Holders'.rjust(50)} - missing columns - not updated")
            return None

        # Extract the relevant values into a dictionary
        new_record_data = {
            'insiders_percent_held': major_holders.loc['Value', 'insidersPercentHeld'],
            'institutions_percent_held': major_holders.loc['Value', 'institutionsPercentHeld'],
            'institutions_float_percent_held': major_holders.loc['Value', 'institutionsFloatPercentHeld'],
            'institutions_count': major_holders.loc['Value', 'institutionsCount'],
        }

        # Call the generic function to handle insertion/update
        self.handle_generic_record_update(
            new_record_data=new_record_data,
            model_class=MajorHolders
        )

    def handle_mutual_fund_holders_deprecated(self) -> None:
        """
        Handle the insertion or update of mutual fund holders data into the database.

        :param stock: yf.Ticker object containing the insider purchases data.
        """

        mutual_fund_holders = safe_execute(None, lambda: getattr(self.stock, "mutualfund_holders"))

        # __ if mutual_fund_holders is empty, return __
        if mutual_fund_holders is None or (isinstance(mutual_fund_holders, pd.DataFrame) and mutual_fund_holders.empty):
            LOGGER.warning(f"{self.ticker.symbol} - {'Mutual Fund Holders'.rjust(50)} - no data to insert")
            return None

        # Normalize column names to match SQLAlchemy model attributes
        mutual_fund_holders.columns = [col.lower().replace(' ', '_') for col in mutual_fund_holders.columns]

        # Convert the date_reported column to datetime.date objects (if it's not already in that format)
        mutual_fund_holders['date_reported'] = pd.to_datetime(mutual_fund_holders['date_reported']).dt.date  # Ensure it's date only

        changed = False

        # Iterate over each row in the mutual_fund_holders DataFrame
        for _, row in mutual_fund_holders.iterrows():
            # Prepare the new record data
            new_record_data = {
                'date_reported': row['date_reported'],
                'holder': row['holder'],
                'pct_held': row.get('pctheld', None),
                'shares': row.get('shares', None),
                'value': row.get('value', None)
            }

            # Call the generic function to handle insert/update
            changed |=  self.handle_generic_record_update(
                new_record_data=new_record_data,
                model_class=MutualFundHolders,
                additional_filters=[
                    MutualFundHolders.date_reported == row['date_reported'],
                    MutualFundHolders.holder == row['holder']
                ],
                print_no_changes=False  # Do not print "no changes detected" messages
            )

        if not changed:
            LOGGER.warning(f"{self.ticker.symbol} - {'Mutual Fund Holders'.rjust(50)} - no changes detected")

    def handle_mutual_fund_holders(self) -> None:
        """
        Handle the insertion or update of mutual fund holders data into the database.

        :param stock: yf.Ticker object containing the insider purchases data.
        """

        mutual_fund_holders = safe_execute(None, lambda: getattr(self.stock, "mutualfund_holders"))

        # __ if mutual_fund_holders is empty, return __
        if mutual_fund_holders is None or (isinstance(mutual_fund_holders, pd.DataFrame) and mutual_fund_holders.empty):
            LOGGER.warning(f"{self.ticker.symbol} - {'Mutual Fund Holders'.rjust(50)} - no data to insert")
            return None

        # __ check if the DataFrame is empty __
        if mutual_fund_holders.empty:
            LOGGER.warning(f"{self.ticker.symbol} - {'Mutual Fund Holders'.rjust(50)} - no data to process")
            return

        # __ normalize column names to match SQLAlchemy model attributes __
        mutual_fund_holders.columns = [col.lower().replace(' ', '_') for col in mutual_fund_holders.columns]

        # __ convert the date_reported column to datetime.date objects (if it's not already in that format) __
        mutual_fund_holders['date_reported'] = pd.to_datetime(mutual_fund_holders['date_reported']).dt.date  # Ensure it's date only

        # __ rename columns to match the database columns __
        mutual_fund_holders.rename(columns={
            'pctheld': 'pct_held'
        }, inplace=True)

        # __ call the bulk update handler __
        self.handle_generic_bulk_update(
            new_data_df=mutual_fund_holders,
            model_class=MutualFundHolders
        )

    def handle_recommendations(self) -> None:
        """
        Handle the insertion or update of recommendations data into the database.

        :param stock: yf.Ticker object containing the insider purchases data.
        """

        recommendations = safe_execute(None, lambda: getattr(self.stock, "recommendations"))

        # __ if recommendations is empty, return __
        if recommendations is None or (isinstance(recommendations, pd.DataFrame) and recommendations.empty):
            LOGGER.warning(f"{self.ticker.symbol} - {'Recommendations'.rjust(50)} - no data to insert")
            return None

        # Normalize column names to match SQLAlchemy model attributes
        recommendations.columns = [col.lower().replace(' ', '_') for col in recommendations.columns]

        # __ rename columns to match the database columns __
        recommendations.rename(columns={
            'strongbuy': 'strong_buy',
            'strongsell': 'strong_sell'
        }, inplace=True)

        # __ call the bulk update handler __
        self.handle_generic_bulk_update(
            new_data_df=recommendations,
            model_class=Recommendations
        )

    @staticmethod
    def clean_upgrades_downgrades(df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the upgrades/downgrades DataFrame by removing unnecessary columns and rows.

        :param df: The upgrades/downgrades DataFrame to clean.
        :return: The cleaned upgrades/downgrades DataFrame.
        """
        # Treat empty strings as NaN
        df.replace('', None, inplace=True)

        # Drop rows where 'to_grade' is null after the replacement
        df = df.dropna(subset=['tograde'])

        # Make a copy to avoid the SettingWithCopyWarning
        df_cleaned = df.copy()

        # Create a column that counts the number of null values (including the treated empty strings) in each row
        df_cleaned['null_count'] = df_cleaned.isnull().sum(axis=1)

        # Sort the DataFrame to ensure the record with the least number of nulls is first
        df_cleaned = df_cleaned.sort_values(by=['date', 'firm', 'null_count'], ascending=[True, True, True])

        # Drop duplicates, keeping the first occurrence (which has the least number of nulls)
        df_cleaned = df_cleaned.drop_duplicates(subset=['date', 'firm'], keep='first')

        # Drop the helper column
        df_cleaned = df_cleaned.drop(columns=['null_count'])

        return df_cleaned

    def handle_upgrades_downgrades_deprecated(self, upgrades_downgrades: pd.DataFrame) -> None:
        """
        Handle the insertion or update of upgrades and downgrades data into the database.

        :param upgrades_downgrades: DataFrame containing the upgrades and downgrades data.
        """

        # __ if upgrades_downgrades is empty, return __
        if upgrades_downgrades.empty:
            LOGGER.warning(f"{self.ticker.symbol} - {'Upgrades/Downgrades'.rjust(50)} - no data to insert")
            return None

        # __ rename index to 'date' and reset the index to make it a column __
        upgrades_downgrades.index.name = 'date'
        upgrades_downgrades.reset_index(inplace=True)
        upgrades_downgrades['date'] = pd.to_datetime(upgrades_downgrades['date']).dt.date  # Ensure it's date only

        # __ normalize column names to match SQLAlchemy model attributes __
        upgrades_downgrades.columns = [col.lower().replace(' ', '_') for col in upgrades_downgrades.columns]

        # __ clean the upgrades_downgrades DataFrame __
        upgrades_downgrades = self.clean_upgrades_downgrades(upgrades_downgrades)

        changed = False

        # Iterate over each row in the upgrades_downgrades DataFrame
        for _, row in upgrades_downgrades.iterrows():
            # Prepare the new record data
            new_record_data = {
                'date': row['date'],
                'firm': row.get('firm', None),
                'to_grade': row.get('tograde', None),
                'from_grade': row.get('fromgrade', None),
                'action': row.get('action', None)
            }

            # Call the generic function to handle insert/update
            changed |= self.handle_generic_record_update(
                new_record_data=new_record_data,
                model_class=UpgradesDowngrades,
                additional_filters=[
                    UpgradesDowngrades.date == row['date'],
                    UpgradesDowngrades.firm == row['firm']
                ],
                print_no_changes=False  # Do not print "no changes detected" messages
            )

        if not changed:
            LOGGER.warning(f"{self.ticker.symbol} - {'Upgrades/Downgrades'.rjust(50)} - no changes detected")

    def handle_upgrades_downgrades(self) -> None:
        """
        Handle the insertion or update of upgrades and downgrades data into the database.

        :param stock: yf.Ticker object containing the insider purchases data.
        """

        upgrades_downgrades = safe_execute(None, lambda: getattr(self.stock, "upgrades_downgrades"))

        # __ if upgrades_downgrades is empty, return __
        if upgrades_downgrades is None or (isinstance(upgrades_downgrades, pd.DataFrame) and upgrades_downgrades.empty):
            LOGGER.warning(f"{self.ticker.symbol} - {'Upgrades/Downgrades'.rjust(50)} - no data to insert")
            return None

        # __ rename index to 'date' and reset the index to make it a column __
        upgrades_downgrades.index.name = 'date'
        upgrades_downgrades.reset_index(inplace=True)
        upgrades_downgrades['date'] = pd.to_datetime(upgrades_downgrades['date']).dt.date  # Ensure it's date only

        # __ normalize column names to match SQLAlchemy model attributes __
        upgrades_downgrades.columns = [col.lower().replace(' ', '_') for col in upgrades_downgrades.columns]

        # __ rename columns to match the database columns __
        upgrades_downgrades.rename(columns={
            'firm': 'firm',
            'tograde': 'to_grade',
            'fromgrade': 'from_grade',
            'action': 'action'
        }, inplace=True)

        # __ call the bulk update handler __
        self.handle_generic_bulk_update(
            new_data_df=upgrades_downgrades,
            model_class=UpgradesDowngrades
        )

    # __ candles handling __
    def handle_candle_data(self, interval: CandleDataInterval) -> bool:
        """
        Delegate the handling of candle data to the CandleService.
        """
        return self.candle_service.handle_candle_data(interval)
