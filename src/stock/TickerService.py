import pandas as pd
import numpy as np
import uuid
from datetime import datetime, date
from typing import Type, Optional, List
from dataclasses import dataclass, field
from decimal import Decimal

from sqlalchemy.orm import session as sess
from sqlalchemy.sql import literal, and_

from TickerServiceBase import Ticker, TickerServiceBase
from models import Action, BalanceSheet, Calendar, CashFlow, Financials, EarningsDates
from models import InfoCashAndFinancialRatios, InfoCompanyAddress, InfoSectorIndustryHistory, InfoTradingSession
from models import InfoTargetPriceAndRecommendation, InfoMarketAndFinancialMetrics, InfoGeneralStock, InfoGovernance
from models import InsiderPurchases, InsiderRosterHolders, InsiderTransactions, InstitutionalHolders, MajorHolders, MutualFundHolders, Recommendations
from models import UpgradesDowngrades


@dataclass
class TickerService(TickerServiceBase):
    """ Handle the insertion or update of a Ticker record in the database. """
    def handle_ticker(self, info: dict) -> bool:
        ticker_data = {"symbol": self.symbol, "company_name": info["longName"], "business_summary": info["longBusinessSummary"]}
        try:
            # __ check if the ticker already exists __
            existing_ticker: Optional[Ticker] = self.session.query(Ticker).filter_by(symbol=ticker_data['symbol']).first()

            # __ update the existing ticker if it exists __
            if existing_ticker:
                return self.update_existing_ticker(existing_ticker=existing_ticker, ticker_data=ticker_data)

            # __ create a new instance of the Ticker class if it does not exist __
            self.create_new_ticker(ticker_data)
            return True

        except Exception as e:
            self.session.rollback()
            print(f"Error occurred: {e}")

        return False

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

        self.ticker = existing_ticker

        # Only commit and print updates if there are changes
        if updated_fields:
            self.session.commit()  # Commit the changes
            print(f"{ticker_data['symbol']} - {'Ticker'.rjust(50)} - UPDATED successfully.")
            for field_, (old_value, new_value) in updated_fields.items():
                print(f" - {field_}: {old_value} -> {new_value}")
            return True

        print(f"{ticker_data['symbol']} - {'Ticker'.rjust(50)} - no changes detected.")
        return False

    def create_new_ticker(self, ticker_data: dict) -> None:
        """
        Create a new Ticker object with the provided data.

        :param ticker_data: Dictionary containing the ticker data.
        """
        new_ticker = Ticker(**ticker_data)
        self.session.add(new_ticker)
        self.session.commit()
        print(f"{ticker_data['symbol']} - {'Ticker'.rjust(50)} - ADDED successfully.")
        self.ticker = new_ticker

    """Handle of all other information"""

    def handle_balance_sheet(self, balance_sheet: pd.DataFrame, period_type: str) -> None:
        """
        Handle the insertion of balance sheet data into the database.

        :param balance_sheet: DataFrame containing the balance sheet data.
        :param period_type: The period type of the data (e.g., 'annual' or 'quarterly').
        """
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
                    self.session.commit()
                    print(f"{self.ticker.symbol} - {'Balance Sheet'.rjust(50)} - inserted balance sheet for date {date_} ({period_type})")

                except Exception as e:
                    # Rollback the transaction in case of an error
                    self.session.rollback()
                    print(f"Error occurred: {e}")

    def handle_cash_flow(self, cash_flow: pd.DataFrame, period_type: str) -> None:
        """
        Handle the insertion of cash flow data into the database.

        :param cash_flow: DataFrame containing the cash flow data.
        :param period_type: The period type of the data (e.g., 'annual' or 'quarterly').
        """
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
                    self.session.commit()
                    print(f"{self.ticker.symbol} - {'Cash Flow'.rjust(50)} - inserted cash flow for date {date_} ({period_type})")

                except Exception as e:
                    # Rollback the transaction in case of an error
                    self.session.rollback()
                    print(f"Error occurred: {e}")

    def handle_financials(self, financials: pd.DataFrame, period_type: str) -> None:
        """
        Handle the insertion of financials data into the database.

        :param financials: DataFrame containing the financials data.
        :param period_type: The period type of the data (e.g., 'annual' or 'quarterly').
        """
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
                    self.session.commit()
                    print(f"{self.ticker.symbol} - {'Financials'.rjust(50)} - inserted financials for date {date_} ({period_type})")

                except Exception as e:
                    # Rollback the transaction in case of an error
                    self.session.rollback()
                    print(f"Error occurred: {e}")

    def handle_actions(self, actions: pd.DataFrame) -> None:
        """
        Handle the insertion or update of actions data into the database.

        :param actions: DataFrame containing the actions data.
        """
        # Reset the index to include the date as a column
        actions.reset_index(inplace=True)
        # Filter out rows that already exist in the database
        actions['Date'] = pd.to_datetime(actions['Date']).dt.tz_localize(None)

        # Normalize column names to match SQLAlchemy model attributes
        actions.columns = [col.lower().replace(' ', '_') for col in actions.columns]

        # Convert the date column to datetime.date objects
        actions['date'] = pd.to_datetime(actions['date']).dt.date

        changed = False

        # Iterate over each row in the actions DataFrame
        for _, row in actions.iterrows():
            # Prepare the new record data
            new_record_data = {
                'date': row['date'],
                'dividends': row.get('dividends', None),
                'stock_splits': row.get('stock_splits', None)
            }

            # Call the generic function to handle insert/update
            changed |= self.handle_generic_record_update(
                new_record_data=new_record_data,
                model_class=Action,
                additional_filters=[
                    Action.date == row['date']
                ],
                print_no_changes=False  # Do not print "no changes detected" messages
            )

        if not changed:
            print(f"{self.ticker.symbol} - {'Actions'.rjust(50)} - no changes detected.")

    def handle_calendar(self, calendar: dict) -> None:
        """
        Handle the insertion or update of calendar dates into the DB.

        :param calendar: Dictionary containing the dividend and earnings data.
        """

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

    def handle_earnings_dates(self, earnings_dates: pd.DataFrame) -> None:
        """
        Handle the insertion of earnings dates data into the database.

        :param earnings_dates: DataFrame containing the earnings dates data.
        """
        # Rename index to "Date"
        earnings_dates.index.name = "Date"

        # Extract the date column and reset the index
        earnings_dates.reset_index(inplace=True)

        # Normalize column names and handle any renaming needed (matching the SQLAlchemy model attributes)
        earnings_dates.columns = [col.replace(' ', '_').lower() for col in earnings_dates.columns]

        # Convert the date column to datetime objects (if it's not already in that format)
        # earnings_dates['date'] = pd.to_datetime(earnings_dates['date']).dt.tz_localize(None).date()  # Ensure it's date only
        earnings_dates['date'] = pd.to_datetime(earnings_dates['date']).dt.date  # Ensure it's date only

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

        changed = False

        # Iterate over each row in the earnings_dates DataFrame
        for _, row in earnings_dates.iterrows():
            # Prepare the new record data
            new_record_data = {
                'date': row['date'],
                'earnings_period': determine_earnings_period(row['date']),
                'eps_estimate': row.get('eps_estimate', None),
                'reported_eps': row.get('reported_eps', None),
                'surprise_percent': row.get('surprise_percent', None)
            }

            # Call the generic function to handle insert/update
            changed |= self.handle_generic_record_update(
                new_record_data=new_record_data,
                model_class=EarningsDates,
                additional_filters=[
                    EarningsDates.date == row['date']
                ],
                print_no_changes=False  # Do not print "no changes detected" messages
            )

        if not changed:
            print(f"{self.ticker.symbol} - {'Earnings Dates'.rjust(50)} - no changes detected")

    def handle_info_company_address(self, info_data: dict) -> None:
        """
        Handle the insertion or update of company address information into the database.

        :param info_data: Dictionary containing the company address information from stock.info.
        """
        # Prepare the new record data with default values as None
        new_record_data = {
            'address1': info_data.get('address1', None),
            'city': info_data.get('city', None),
            'state': info_data.get('state', None),
            'zip': info_data.get('zip', None),
            'country': info_data.get('country', None),
            'phone': info_data.get('phone', None),
            'website': info_data.get('website', None),
            'ir_website': info_data.get('irWebsite', None)
        }

        # Call the generic function to handle insert/update
        self.handle_generic_record_update(
            new_record_data=new_record_data,
            model_class=InfoCompanyAddress
        )

    def handle_sector_industry_history(self, info_data: dict) -> None:
        """
        Handle the insertion or update of sector and industry history data into the database.

        :param info_data: Dictionary containing the stock information, including sector and industry.
        """
        try:
            # Extract the necessary information from stock_info
            sector = info_data.get('sector')
            industry = info_data.get('industry')
            start_date = pd.to_datetime('today').date()  # Use today's date as the start date for the new record

            # Check for an existing record with a NULL end_date for the same ticker_id
            existing_record = self.session.query(InfoSectorIndustryHistory).filter(
                InfoSectorIndustryHistory.ticker_id == literal(self.ticker.id),
                InfoSectorIndustryHistory.end_date == None
            ).order_by(
                InfoSectorIndustryHistory.start_date.desc()
            ).first()

            if existing_record:
                changes_log = []

                # Compare the current sector and industry with the new values
                if existing_record.sector != sector or existing_record.industry != industry:
                    # Log the change details
                    changes_log.append(
                        f"{self.ticker.symbol} - Sector/Industry changed from Sector: {existing_record.sector}, "
                        f"Industry: {existing_record.industry} to Sector: {sector}, Industry: {industry}."
                    )

                    # Update the existing record to set the end_date
                    existing_record.end_date = start_date

                    # Create a new record with the new sector and industry
                    new_sector_industry_record = InfoSectorIndustryHistory(
                        ticker_id=self.ticker.id,
                        sector=sector,
                        industry=industry,
                        start_date=start_date,
                        end_date=None  # This is the current valid entry
                    )

                    self.session.add(new_sector_industry_record)

                    # Commit the changes to the database
                    self.session.commit()
                    for change in changes_log:
                        print(change)

                else:
                    print(f"{self.ticker.symbol} - {'Sector/Industry'.rjust(50)} - no changes detected")

            else:
                # Create a new record if no existing record with a NULL end_date is found
                new_sector_industry_record = InfoSectorIndustryHistory(
                    ticker_id=self.ticker.id,
                    sector=sector,
                    industry=industry,
                    start_date=start_date,
                    end_date=None  # This is the current valid entry
                )

                # Add the object to the session
                self.session.add(new_sector_industry_record)
                # Commit the session to the database
                self.session.commit()
                print(f"{self.ticker.symbol} - {'Sector/Industry'.rjust(50)} - INSERTED successfully.")

        except Exception as e:
            # Rollback the transaction in case of an error
            self.session.rollback()
            print(f"Error occurred: {e}")

    def handle_info_target_price_and_recommendation(self, info_data: dict) -> None:
        """
        Handle the insertion or update of target price and recommendation info into the database.

        :param info_data: Dictionary containing the target price and recommendation data.
        """
        new_record_data = {
            'target_high_price': info_data.get('targetHighPrice', None),
            'target_low_price': info_data.get('targetLowPrice', None),
            'target_mean_price': info_data.get('targetMeanPrice', None),
            'target_median_price': info_data.get('targetMedianPrice', None),
            'recommendation_mean': info_data.get('recommendationMean', None),
            'recommendation_key': info_data.get('recommendationKey', None),
            'number_of_analyst_opinions': info_data.get('numberOfAnalystOpinions', None)
        }

        self.handle_generic_record_update(
            new_record_data=new_record_data,
            model_class=InfoTargetPriceAndRecommendation
        )

    def handle_info_governance(self, info_data: dict) -> None:
        """
        Handle the insertion or update of governance info into the database.

        :param governance_datainfo_data: Dictionary containing the governance data.
        """
        # Prepare the new record data for InfoGovernance
        new_record_data = {
            'audit_risk': info_data.get('auditRisk', None),
            'board_risk': info_data.get('boardRisk', None),
            'compensation_risk': info_data.get('compensationRisk', None),
            'shareholder_rights_risk': info_data.get('shareHolderRightsRisk', None),
            'overall_risk': info_data.get('overallRisk', None),
            'governance_epoch_date': info_data.get('governanceEpochDate', None),
            'compensation_as_of_epoch_date': info_data.get('compensationAsOfEpochDate', None)
        }

        # Call the generic function to handle the update or insertion
        self.handle_generic_record_update(
            new_record_data=new_record_data,
            model_class=InfoGovernance,
            additional_filters=None  # No additional filters needed in this case
        )

    def handle_info_cash_and_financial_ratios(self, info_data: dict) -> None:
        """
        Handle the insertion or update of cash and financial ratios info into the database.

        :param info_data: Dictionary containing the financial ratios data.
        """
        new_record_data = {
            'total_cash': info_data.get('totalCash', None),
            'total_cash_per_share': info_data.get('totalCashPerShare', None),
            'ebitda': info_data.get('ebitda', None),
            'total_debt': info_data.get('totalDebt', None),
            'quick_ratio': info_data.get('quickRatio', None),
            'current_ratio': info_data.get('currentRatio', None),
            'total_revenue': info_data.get('totalRevenue', None),
            'debt_to_equity': info_data.get('debtToEquity', None),
            'revenue_per_share': info_data.get('revenuePerShare', None),
            'return_on_assets': info_data.get('returnOnAssets', None),
            'return_on_equity': info_data.get('returnOnEquity', None),
            'free_cashflow': info_data.get('freeCashflow', None),
            'operating_cashflow': info_data.get('operatingCashflow', None),
            'earnings_growth': info_data.get('earningsGrowth', None),
            'revenue_growth': info_data.get('revenueGrowth', None),
            'gross_margins': info_data.get('grossMargins', None),
            'ebitda_margins': info_data.get('ebitdaMargins', None),
            'operating_margins': info_data.get('operatingMargins', None),
            'trailing_peg_ratio': info_data.get('trailingPegRatio', None)
        }

        self.handle_generic_record_update(
            new_record_data=new_record_data,
            model_class=InfoCashAndFinancialRatios
        )

    def handle_info_market_and_financial_metrics(self, info_data: dict) -> None:
        """
        Handle the insertion or update of market and financial metrics info into the database.

        :param info_data: Dictionary containing the market and financial metrics data.
        """

        # Extract the new data from the info_data dictionary
        new_record_data = {
            'dividend_rate': info_data.get('dividendRate', None),
            'dividend_yield': info_data.get('dividendYield', None),
            'ex_dividend_date': info_data.get('exDividendDate', None),
            'payout_ratio': info_data.get('payoutRatio', None),
            'five_year_avg_dividend_yield': info_data.get('fiveYearAvgDividendYield', None),
            'trailing_annual_dividend_rate': info_data.get('trailingAnnualDividendRate', None),
            'trailing_annual_dividend_yield': info_data.get('trailingAnnualDividendYield', None),
            'last_dividend_value': info_data.get('lastDividendValue', None),
            'last_dividend_date': info_data.get('lastDividendDate', None),

            'average_volume': info_data.get('averageVolume', None),
            'average_volume_10days': info_data.get('averageVolume10days', None),
            'average_daily_volume_10day': info_data.get('averageDailyVolume10Day', None),

            'enterprise_value': info_data.get('enterpriseValue', None),
            'book_value': info_data.get('bookValue', None),
            'enterprise_to_revenue': info_data.get('enterpriseToRevenue', None),
            'enterprise_to_ebitda': info_data.get('enterpriseToEbitda', None),

            'shares_short': info_data.get('sharesShort', None),
            'shares_short_prior_month': info_data.get('sharesShortPriorMonth', None),
            'shares_short_previous_month_date': info_data.get('sharesShortPreviousMonthDate', None),
            'date_short_interest': info_data.get('dateShortInterest', None),
            'shares_percent_shares_out': info_data.get('sharesPercentSharesOut', None),
            'held_percent_insiders': info_data.get('heldPercentInsiders', None),
            'held_percent_institutions': info_data.get('heldPercentInstitutions', None),
            'short_ratio': info_data.get('shortRatio', None),
            'short_percent_of_float': info_data.get('shortPercentOfFloat', None),
            'implied_shares_outstanding': info_data.get('impliedSharesOutstanding', None),
            'float_shares': info_data.get('floatShares', None),
            'shares_outstanding': info_data.get('sharesOutstanding', None),

            'earnings_quarterly_growth': info_data.get('earningsQuarterlyGrowth', None),
            'net_income_to_common': info_data.get('netIncomeToCommon', None),
            'trailing_eps': info_data.get('trailingEps', None),
            'forward_eps': info_data.get('forwardEps', None),
            'peg_ratio': info_data.get('pegRatio', None),

            'last_split_factor': info_data.get('lastSplitFactor', None),
            'last_split_date': info_data.get('lastSplitDate', None),

            'beta': info_data.get('beta', None),

            'profit_margins': info_data.get('profitMargins', None),
            'fifty_two_week_change': info_data.get('52WeekChange', None),
            'sp_fifty_two_week_change': info_data.get('SandP52WeekChange', None),
            'last_fiscal_year_end': info_data.get('lastFiscalYearEnd', None),
            'next_fiscal_year_end': info_data.get('nextFiscalYearEnd', None),
            'most_recent_quarter': info_data.get('mostRecentQuarter', None)
        }

        self.handle_generic_record_update(
            new_record_data=new_record_data,
            model_class=InfoMarketAndFinancialMetrics
        )

    def handle_info_general_stock(self, isin: str, info_data: dict, history_metadata: dict) -> None:
        """
        Handle the insertion or update of general stock information into the database.

        :param isin: The ISIN of the stock.
        :param info_data: Dictionary containing the general stock information from stock.info.
        :param history_metadata: Dictionary containing the general stock information from stock.history_metadata.
        """
        # Prepare the new record data combining both info_data and metadata_data
        new_record_data = {
            'isin': isin,
            'currency': info_data.get('currency', None),
            'symbol': info_data.get('symbol', None),
            'exchange': info_data.get('exchange', None),
            'quote_type': info_data.get('quoteType', None),
            'underlying_symbol': info_data.get('underlyingSymbol', None),
            'short_name': info_data.get('shortName', None),
            'long_name': info_data.get('longName', None),
            'first_trade_date_epoch_utc': info_data.get('firstTradeDateEpochUtc', None),
            'time_zone_full_name': info_data.get('timeZoneFullName', None),
            'time_zone_short_name': info_data.get('timeZoneShortName', None),
            'uuid': info_data.get('uuid', None),
            'message_board_id': info_data.get('messageBoardId', None),
            'gmt_offset_milliseconds': info_data.get('gmtOffSetMilliseconds', None),
            'price_hint': info_data.get('priceHint', None),
            'max_age': info_data.get('maxAge', None),
            'full_time_employees': info_data.get('fullTimeEmployees', None),

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

    def handle_info_trading_session(self, info: dict, basic_info: dict, history_metadata: dict) -> None:
        """
        Handle the insertion or update of trading session information into the database.

        :param info: Dictionary containing the info data from stock.info.
        :param basic_info: Dictionary containing the basic info data from stock.basic_info.
        :param history_metadata: Dictionary containing the history metadata from stock.history_metadata.
        """
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

            # From basic info (dynamic and static data)
            'last_price': basic_info.get('lastPrice', None),
            'last_volume': basic_info.get('lastVolume', None),
            'ten_day_average_volume': basic_info.get('tenDayAverageVolume', None),
            'three_month_average_volume': basic_info.get('threeMonthAverageVolume', None),
            'year_change': basic_info.get('yearChange', None),
            'year_high': basic_info.get('yearHigh', None),
            'year_low': basic_info.get('yearLow', None),

            # From info (dynamic and static data)
            'current_price': info.get('currentPrice', None),
            'open': info.get('open', None),
            'previous_close': info.get('previousClose', None),
            'regular_market_previous_close': info.get('regularMarketPreviousClose', None),
            'day_high': info.get('dayHigh', None),
            'day_low': info.get('dayLow', None),
            'market_cap': info.get('marketCap', None),
            'regular_market_open': info.get('regularMarketOpen', None),
            'trailing_pe': info.get('trailingPE', None),
            'forward_pe': info.get('forwardPE', None),
            'volume': info.get('volume', None),
            'bid': info.get('bid', None),
            'ask': info.get('ask', None),
            'bid_size': info.get('bidSize', None),
            'ask_size': info.get('askSize', None),
            'price_to_sales_trailing_12months': info.get('priceToSalesTrailing12Months', None),
            'price_to_book': info.get('priceToBook', None),
            'fifty_day_average': info.get('fiftyDayAverage', None),
            'two_hundred_day_average': info.get('twoHundredDayAverage', None)
        }

        # Call the generic function to handle insert/update
        self.handle_generic_record_update(
            new_record_data=new_record_data,
            model_class=InfoTradingSession
        )

    def handle_insider_purchases(self, insider_purchases: pd.DataFrame) -> None:
        """
        Handle the insertion or update of insider purchases data into the database.

        :param insider_purchases: DataFrame containing the insider purchases data.
        """
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

    def handle_insider_roster_holders(self, insider_roster_holders: pd.DataFrame) -> None:
        """
        Handle the insertion or update of insider roster holders data into the database.

        :param insider_roster_holders: DataFrame containing the insider roster holders data.
        """

        changed = False

        # Iterate over each row in the insider_roster_holders DataFrame
        for _, row in insider_roster_holders.iterrows():
            # Prepare a dictionary to hold the new record data
            new_record_data = {
                'name': row.get('Name', None),
                'position': row.get('Position', None),
                'url': row.get('URL', None),
                'most_recent_transaction': row.get('Most Recent Transaction', None),
                'latest_transaction_date': pd.to_datetime(row.get('Latest Transaction Date', None)),  # Convert to datetime
                'shares_owned_directly': row.get('Shares Owned Directly', None),
                'position_direct_date': pd.to_datetime(row.get('Position Direct Date', None)),  # Convert to datetime
                'shares_owned_indirectly': row.get('Shares Owned Indirectly', None),
                'position_indirect_date': pd.to_datetime(row.get('Position Indirect Date', None))  # Convert to datetime
            }

            # Replace NaT with None for datetime fields
            for date_field in ['latest_transaction_date', 'position_direct_date', 'position_indirect_date']:
                if pd.isna(new_record_data[date_field]):
                    new_record_data[date_field] = None

            # Replace NaN with None for float fields
            for numeric_field in ['shares_owned_directly', 'shares_owned_indirectly']:
                if pd.isna(new_record_data[numeric_field]):
                    new_record_data[numeric_field] = None

            # Use the handle_record_update function to handle the record
            changed |= self.handle_generic_record_update(
                new_record_data=new_record_data,
                model_class=InsiderRosterHolders,
                additional_filters=[InsiderRosterHolders.name == new_record_data['name']],
                print_no_changes=False
            )

        if not changed:
            print(f"{self.ticker.symbol} - {'Insider Roster Holders'.rjust(50)} - no changes detected")

    def handle_insider_transactions_old(self, insider_transactions: pd.DataFrame) -> None:
        """
        Handle the insertion or update of insider transactions data into the database.

        :param insider_transactions: DataFrame containing the insider transactions data.
        """
        # Normalize column names to match SQLAlchemy model attributes
        insider_transactions.columns = [col.lower().replace(' ', '_') for col in insider_transactions.columns]

        # Convert relevant columns to appropriate types
        insider_transactions['start_date'] = pd.to_datetime(insider_transactions['start_date'])

        changed = False

        # Iterate over each row in the insider_transactions DataFrame
        for _, row in insider_transactions.iterrows():
            new_record_data = {
                'shares': row.get('shares', None),
                'value': row.get('value', None),
                'url': row.get('url', None),
                'text': row.get('text', None),
                'insider': row.get('insider', None),
                'position': row.get('position', None),
                'transaction_type': row.get('transaction', None),
                'start_date': row.get('start_date', None),
                'ownership': row.get('ownership', None)
            }

            # Replace NaN with None for float fields
            for numeric_field in ['shares', 'value']:
                if pd.isna(new_record_data[numeric_field]):
                    new_record_data[numeric_field] = None

            # Call the generic function to handle insert/update
            changed |= self.handle_generic_record_update(
                new_record_data=new_record_data,
                model_class=InsiderTransactions,
                additional_filters=[InsiderTransactions.start_date == row['start_date'].date(),
                                    InsiderTransactions.insider == new_record_data['insider'],
                                    InsiderTransactions.value == new_record_data['value']],
                print_no_changes=False
            )

        if not changed:
            print(f"{self.ticker.symbol} - {'Insider Transactions'.rjust(50)} - no changes detected")

    def handle_insider_transactions(self, insider_transactions: pd.DataFrame) -> None:
        """
        Handle the bulk update or insertion of insider transactions data into the database.

        :param insider_transactions: DataFrame containing the insider transactions data.
        """
        # Normalize column names to match SQLAlchemy model attributes
        insider_transactions.columns = [col.lower().replace(' ', '_') for col in insider_transactions.columns]

        # Convert relevant columns to appropriate types
        insider_transactions['start_date'] = pd.to_datetime(insider_transactions['start_date'])

        # Filter out rows where 'start_date' or 'insider' is null
        insider_transactions = insider_transactions.dropna(subset=['start_date', 'insider'])

        # Define the columns to retrieve from the database and compare with the new data
        db_columns = [
            'ticker_id', 'start_date', 'shares', 'value', 'url',
            'text', 'insider', 'position', 'transaction', 'ownership'
        ]

        # Define the columns to use for comparison to find new or updated records
        comparison_columns = ['start_date', 'insider', 'shares', 'value']

        # Call the bulk update handler
        self.handle_generic_bulk_update(
            new_data_df=insider_transactions,
            model_class=InsiderTransactions,
            db_columns=db_columns,
            comparison_columns=comparison_columns
        )

    def handle_institutional_holders(self, institutional_holders: pd.DataFrame) -> None:
        """
        Handle the insertion or update of institutional holders data into the database.

        :param institutional_holders: DataFrame containing the institutional holders' data.
        """
        # Normalize column names to match SQLAlchemy model attributes
        institutional_holders.columns = [col.lower().replace(' ', '_') for col in institutional_holders.columns]

        # Convert the date_reported column to datetime.date objects (if it's not already in that format)
        institutional_holders['date_reported'] = pd.to_datetime(institutional_holders['date_reported']).dt.date  # Ensure it's date only

        changed = False

        # Iterate over each row in the institutional_holders DataFrame
        for _, row in institutional_holders.iterrows():
            # Prepare the new record data
            new_record_data = {
                'date_reported': row['date_reported'],
                'holder': row['holder'],
                'pct_held': row.get('pctheld', None),
                'shares': row.get('shares', None),
                'value': row.get('value', None)
            }

            # Call the generic function to handle insert/update
            changed |= self.handle_generic_record_update(
                new_record_data=new_record_data,
                model_class=InstitutionalHolders,
                additional_filters=[
                    InstitutionalHolders.date_reported == row['date_reported'],
                    InstitutionalHolders.holder == row['holder']
                ],
                print_no_changes=False  # Do not print "no changes detected" messages
            )

        if not changed:
            print(f"{self.ticker.symbol} - {'Institutional Holders'.rjust(50)} - no changes detected")

    def handle_major_holders(self, major_holders: pd.DataFrame) -> None:
        """
        Handle the insertion or update of major holders data into the database from a DataFrame.

        :param major_holders: DataFrame containing the major holders data as a single record.
        """
        # Ensure the DataFrame has the correct structure
        if major_holders.empty or major_holders.index.empty:
            print("Major holders DataFrame is empty or does not have the correct structure.")
            return

        # Transpose the DataFrame to switch columns to rows
        major_holders = major_holders.T

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

    def handle_mutual_fund_holders(self, mutual_fund_holders: pd.DataFrame) -> None:
        """
        Handle the insertion or update of mutual fund holders data into the database.

        :param mutual_fund_holders: DataFrame containing the mutual fund holders data.
        """
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
            print(f"{self.ticker.symbol} - {'Mutual Fund Holders'.rjust(50)} - no changes detected")

    def handle_recommendations(self, recommendations: pd.DataFrame) -> None:
        """
        Handle the insertion or update of recommendations data into the database.

        :param recommendations: DataFrame containing the recommendations data.
        """
        # Normalize column names to match SQLAlchemy model attributes
        recommendations.columns = [col.lower().replace(' ', '_') for col in recommendations.columns]

        changed = False

        # Iterate over each row in the recommendations DataFrame
        for _, row in recommendations.iterrows():
            # Prepare the new record data
            new_record_data = {
                'period': row.get('period', None),
                'strong_buy': row.get('strongbuy', None),
                'buy': row.get('buy', None),
                'hold': row.get('hold', None),
                'sell': row.get('sell', None),
                'strong_sell': row.get('strongsell', None)
            }

            # Call the generic function to handle insert/update
            changed |= self.handle_generic_record_update(
                new_record_data=new_record_data,
                model_class=Recommendations,
                additional_filters=[
                    Recommendations.period == row['period']
                ],
                print_no_changes=False  # Do not print "no changes detected" messages
            )

        if not changed:
            print(f"{self.ticker.symbol} - {'Recommendations'.rjust(50)} - no changes detected")

    def handle_upgrades_downgrades(self, upgrades_downgrades: pd.DataFrame) -> None:
        """
        Handle the insertion or update of upgrades and downgrades data into the database.

        :param upgrades_downgrades: DataFrame containing the upgrades and downgrades data.
        """

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

        # Rename index to 'date' and reset the index to make it a column
        upgrades_downgrades.index.name = 'date'
        upgrades_downgrades.reset_index(inplace=True)
        upgrades_downgrades['date'] = pd.to_datetime(upgrades_downgrades['date']).dt.date  # Ensure it's date only

        # Normalize column names to match SQLAlchemy model attributes
        upgrades_downgrades.columns = [col.lower().replace(' ', '_') for col in upgrades_downgrades.columns]

        # clean the upgrades_downgrades DataFrame
        upgrades_downgrades = clean_upgrades_downgrades(upgrades_downgrades)

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
            print(f"{self.ticker.symbol} - {'Upgrades/Downgrades'.rjust(50)} - no changes detected")
