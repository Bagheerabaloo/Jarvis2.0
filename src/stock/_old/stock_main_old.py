import pandas as pd
import yfinance as yf
import uuid
from typing import Optional, Type
from datetime import datetime, date
from src.common.file_manager.FileManager import FileManager

from sqlalchemy import and_, literal
from sqlalchemy.orm import session as sess
from stock.src.models import Base, Ticker, Action, BalanceSheet, Calendar, CashFlow, Financials, EarningsDates
from stock.src.models import InfoCashAndFinancialRatios, InfoCompanyAddress, InfoSectorIndustryHistory, InfoTradingSession
from stock.src.models import InfoTargetPriceAndRecommendation, InfoMarketAndFinancialMetrics, InfoGeneralStock, InfoGovernance
from stock.src.models import InsiderPurchases, InsiderRosterHolders, InsiderTransactions, InstitutionalHolders, MajorHolders, MutualFundHolders, Recommendations
from stock.src.models import UpgradesDowngrades


def handle_record_update(
    ticker: Ticker,
    new_record_data: dict,
    model_class: Type[Base],
    session: sess.Session,
    additional_filters: Optional[list] = None,
    comparison_fields: Optional[list[str]] = None,
    print_no_changes: bool = True
) -> bool:
    """
    Handle the insertion or update of a generic record in the database.

    :param ticker: The Ticker object.
    :param new_record_data: Dictionary containing the new record data.
    :param model_class: The SQLAlchemy model class to interact with.
    :param session: SQLAlchemy session for database operations.
    :param additional_filters: Optional list of additional filters for the query.
    :param comparison_fields: Optional list of fields to compare for detecting changes.
    :param print_no_changes: Whether to print a message when no changes are detected.
    :param write_date: Whether to write the current timestamp to the record.
    """
    try:
        # Create a datetime object for the current timestamp
        current_timestamp = datetime.now()
        model_class_name = ' '.join([x.capitalize() for x in model_class.__tablename__.replace('_', ' ').split(' ')]).rjust(50)

        # Base filter criteria
        filters = [model_class.ticker_id == literal(ticker.id)]

        # Add any additional filters
        if additional_filters:
            filters.extend(additional_filters)

        # Get the last record for the ticker with the applied filters
        last_record = session.query(model_class).filter(
            *filters
        ).order_by(model_class.last_update.desc()).first()

        changes_log = []
        # Compare the new data with the last record
        if last_record:
            for field, new_value in new_record_data.items():
                old_value = getattr(last_record, field)

                # Handling UUID comparisons
                if isinstance(old_value, uuid.UUID):
                    old_value = str(old_value)
                    new_value = str(new_value)

                # Handling datetime comparisons
                if isinstance(old_value, (datetime, date)) and isinstance(new_value, (datetime, date)):
                    old_value = old_value.date() if isinstance(old_value, datetime) else old_value
                    new_value = new_value.date() if isinstance(new_value, datetime) else new_value

                # Handling float comparisons to account for small differences
                if isinstance(new_value, float):
                    # Avoid detecting changes if both are NaN
                    if pd.isna(old_value) and pd.isna(new_value):
                        continue
                    old_value = float(old_value) if old_value is not None else old_value
                    new_value = float(new_value) if new_value is not None else new_value

                if new_value is not None and old_value != new_value:
                    if len(changes_log) == 0:
                        changes_log.append(f"{ticker.symbol} - {model_class_name} - {field} changed from {old_value} to {new_value}")
                    else:
                        changes_log.append(f"{' ' * len(ticker.symbol)}   {' ' * len(model_class_name)} - {field} changed from {old_value} to {new_value}")

        has_changes = bool(changes_log)

        if has_changes or not last_record:
            # Create a new model instance with the new data
            new_record_data['ticker_id'] = ticker.id
            new_record_data['last_update'] = current_timestamp
            new_record = model_class(**new_record_data)

            # Add the object to the session
            session.add(new_record)

            # Commit the session to the database
            session.commit()

            if not last_record:
                print(f"{ticker.symbol} - {model_class_name} - NEW ROW INSERTED")
            else:
                for change in changes_log:
                    print(change)
            return True

        print(f"{ticker.symbol} - {model_class_name} - no changes detected") if print_no_changes else None
        return False

    except Exception as e:
        # Rollback the transaction in case of an error
        session.rollback()
        print(f"Error occurred: {e}")
        return False


def handle_ticker(ticker: str, info: dict, session: sess.Session) -> Type[Ticker] | Ticker | None:
    ticker_data = {"symbol": ticker, "company_name": info["longName"], "business_summary": info["longBusinessSummary"]}
    try:
        # Check if the ticker already exists
        existing_ticker = session.query(Ticker).filter_by(symbol=ticker_data['symbol']).first()

        if existing_ticker:
            # Track changes
            updated_fields = {}

            # Update the existing record and track changes
            if existing_ticker.company_name != ticker_data['company_name']:
                updated_fields['company_name'] = (existing_ticker.company_name, ticker_data['company_name'])
                existing_ticker.company_name = ticker_data['company_name']

            if existing_ticker.business_summary != ticker_data['business_summary']:
                updated_fields['business_summary'] = (existing_ticker.business_summary, ticker_data['business_summary'])
                existing_ticker.business_summary = ticker_data['business_summary']

            # Only commit and print updates if there are changes
            if updated_fields:
                session.commit()  # Commit the changes
                print(f"{ticker_data['symbol']} - {'Ticker'.rjust(50)} - UPDATED successfully.")
                for field, (old_value, new_value) in updated_fields.items():
                    print(f" - {field}: {old_value} -> {new_value}")
            else:
                print(f"{ticker_data['symbol']} - {'Ticker'.rjust(50)} - no changes detected.")

            return existing_ticker
        else:
            # Create an instance of the Ticker class
            new_ticker = Ticker(**ticker_data)
            session.add(new_ticker)
            session.commit()
            print(f"{ticker_data['symbol']} - {'Ticker'.rjust(50)} - ADDED successfully.")
            return new_ticker

    except Exception as e:
        # If there is an error, rollback the transaction
        session.rollback()
        print(f"Error occurred: {e}")

    return None


def handle_actions_old(ticker: Ticker, actions: pd.DataFrame, session: sess.Session) -> None:
    # Reset the index to include the date as a column
    actions.reset_index(inplace=True)

    # Iterate over each row in the actions DataFrame
    for _, row in actions.iterrows():
        date = row['Date']  # Extract the date
        dividends = row['Dividends'] if 'Dividends' in row else None  # Extract dividends if available
        stock_splits = row['Stock Splits'] if 'Stock Splits' in row else None  # Extract stock splits if available

        try:
            # Check if a record with the same ticker_id and date already exists
            existing_action = session.query(Action).filter(
                and_(Action.ticker_id == ticker.id, Action.date == date)
            ).first()

            if not existing_action:
                # Create a new Action object
                new_action = Action(
                    ticker_id=ticker.id,
                    date=date,
                    dividends=dividends,
                    stock_splits=stock_splits
                )

                # Add the object to the session
                session.add(new_action)

                # Commit the session to the database
                session.commit()

                # Retrieve the ID generated by the database
                # action_id = new_action.id
                # print(f"The new action ID is: {action_id}")

        except Exception as e:
            # If there is an error, rollback the transaction
            session.rollback()
            print(f"Error occurred: {e}")


def handle_actions_bulk(ticker: Ticker, actions: pd.DataFrame, session: sess.Session) -> None:
    # Reset the index to include the date as a column
    actions.reset_index(inplace=True)

    if ticker.id is None:
        raise ValueError("Ticker ID must be set before adding actions.")

    # Retrieve existing action dates for the ticker
    existing_dates = session.query(Action.date).filter(Action.ticker_id == literal(ticker.id)).all()
    existing_dates = {pd.to_datetime(date) for date, in existing_dates}  # Convert to a set for fast lookup

    # Filter out rows that already exist in the database
    actions['Date'] = pd.to_datetime(actions['Date']).dt.tz_localize(None)
    new_actions = actions[~actions['Date'].isin(existing_dates)]

    # Prepare the list of Action objects to insert
    to_insert = [
        Action(
            ticker_id=ticker.id,
            date=row['Date'],
            dividends=row.get('Dividends', None),
            stock_splits=row.get('Stock Splits', None)
        )
        for _, row in new_actions.iterrows()
    ]

    try:
        # Bulk insert the new Action objects
        if to_insert:
            session.bulk_save_objects(to_insert)
            session.commit()
            print(f"{ticker.symbol} - {'Actions'.rjust(50)} - Inserted {len(to_insert)} new actions")

    except Exception as e:
        # Rollback in case of error
        session.rollback()
        print(f"Error occurred: {e}")


def handle_actions(ticker: Ticker, actions: pd.DataFrame, session: sess.Session) -> None:
    """
    Handle the insertion or update of actions data into the database.

    :param ticker: The Ticker object.
    :param actions: DataFrame containing the actions data.
    :param session: SQLAlchemy session for database operations.
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
        changed |= handle_record_update(
            ticker=ticker,
            new_record_data=new_record_data,
            model_class=Action,
            session=session,
            additional_filters=[
                Action.date == row['date']
            ],
            print_no_changes=False  # Do not print "no changes detected" messages
        )

    if not changed:
        print(f"{ticker.symbol} - {'Actions'.rjust(50)} - no changes detected.")


def handle_balance_sheet_bulk(ticker: Ticker, balance_sheet: pd.DataFrame, period_type: str, session: sess.Session) -> None:
    """
    Insert balance sheet data into the database, avoiding duplicates.

    :param ticker: The Ticker object corresponding to the stock.
    :param balance_sheet: DataFrame containing the balance sheet data.
    :param period_type: The period type of the balance sheet data (e.g., 'quarterly' or 'annual').
    :param session: SQLAlchemy session to interact with the database.
    """
    # Transpose the DataFrame to have dates as rows
    balance_sheet = balance_sheet.T

    # give "Date" name to index
    balance_sheet.index.name = "Date"

    # Reset the index to include the date as a column
    balance_sheet.reset_index(inplace=True)

    if ticker.id is None:
        raise ValueError("Ticker ID must be set before adding balance sheet data.")

    # Retrieve existing dates for the ticker and period type to avoid duplicates
    existing_dates = session.query(BalanceSheet.date).filter(and_(BalanceSheet.ticker_id == ticker.id, BalanceSheet.period_type == period_type)).all()
    existing_dates = {pd.to_datetime(date) for date, in existing_dates}

    # Filter out rows that already exist in the database based on date and period_type
    new_records = balance_sheet[~balance_sheet.apply(lambda row: row['Date'] in existing_dates, axis=1)]

    # Prepare the list of BalanceSheet objects to insert
    to_insert = [
        BalanceSheet(
            ticker_id=ticker.id,
            date=row['Date'],
            period_type=period_type,
            treasury_shares_number=row.get('Treasury Shares Number', None),
            ordinary_shares_number=row.get('Ordinary Shares Number', None),
            share_issued=row.get('Share Issued', None),
            net_debt=row.get('Net Debt', None),
            total_debt=row.get('Total Debt', None),
            tangible_book_value=row.get('Tangible Book Value', None),
            invested_capital=row.get('Invested Capital', None),
            working_capital=row.get('Working Capital', None),
            net_tangible_assets=row.get('Net Tangible Assets', None),
            common_stock_equity=row.get('Common Stock Equity', None),
            total_capitalization=row.get('Total Capitalization', None),
            total_equity_gross_minority_interest=row.get('Total Equity Gross Minority Interest', None),
            stockholders_equity=row.get('Stockholders Equity', None),
            gains_losses_not_affecting_retained_earnings=row.get('Gains Losses Not Affecting Retained Earnings', None),
            other_equity_adjustments=row.get('Other Equity Adjustments', None),
            retained_earnings=row.get('Retained Earnings', None),
            capital_stock=row.get('Capital Stock', None),
            common_stock=row.get('Common Stock', None),
            total_liabilities_net_minority_interest=row.get('Total Liabilities Net Minority Interest', None),
            total_non_current_liabilities_net_minority_interest=row.get('Total Non Current Liabilities Net Minority Interest', None),
            other_non_current_liabilities=row.get('Other Non Current Liabilities', None),
            trade_and_other_payables_non_current=row.get('Tradeand Other Payables Non Current', None),
            long_term_debt_and_capital_lease_obligation=row.get('Long Term Debt And Capital Lease Obligation', None),
            long_term_debt=row.get('Long Term Debt', None),
            current_liabilities=row.get('Current Liabilities', None),
            other_current_liabilities=row.get('Other Current Liabilities', None),
            current_deferred_liabilities=row.get('Current Deferred Liabilities', None),
            current_deferred_revenue=row.get('Current Deferred Revenue', None),
            current_debt_and_capital_lease_obligation=row.get('Current Debt And Capital Lease Obligation', None),
            current_debt=row.get('Current Debt', None),
            other_current_borrowings=row.get('Other Current Borrowings', None),
            commercial_paper=row.get('Commercial Paper', None),
            payables_and_accrued_expenses=row.get('Payables And Accrued Expenses', None),
            payables=row.get('Payables', None),
            accounts_payable=row.get('Accounts Payable', None),
            total_assets=row.get('Total Assets', None),
            total_non_current_assets=row.get('Total Non Current Assets', None),
            other_non_current_assets=row.get('Other Non Current Assets', None),
            non_current_deferred_assets=row.get('Non Current Deferred Assets', None),
            non_current_deferred_taxes_assets=row.get('Non Current Deferred Taxes Assets', None),
            investments_and_advances=row.get('Investments And Advances', None),
            other_investments=row.get('Other Investments', None),
            investment_in_financial_assets=row.get('Investmentin Financial Assets', None),
            available_for_sale_securities=row.get('Available For Sale Securities', None),
            net_ppe=row.get('Net PPE', None),
            accumulated_depreciation=row.get('Accumulated Depreciation', None),
            gross_ppe=row.get('Gross PPE', None),
            leases=row.get('Leases', None),
            machinery_furniture_equipment=row.get('Machinery Furniture Equipment', None),
            land_and_improvements=row.get('Land And Improvements', None),
            properties=row.get('Properties', None),
            current_assets=row.get('Current Assets', None),
            other_current_assets=row.get('Other Current Assets', None),
            inventory=row.get('Inventory', None),
            # finished_goods=row.get('Finished Goods', None),
            # raw_materials=row.get('Raw Materials', None),
            receivables=row.get('Receivables', None),
            other_receivables=row.get('Other Receivables', None),
            accounts_receivable=row.get('Accounts Receivable', None),
            cash_cash_equivalents_and_short_term_investments=row.get('Cash Cash Equivalents And Short Term Investments', None),
            other_short_term_investments=row.get('Other Short Term Investments', None),
            cash_and_cash_equivalents=row.get('Cash And Cash Equivalents', None),
            cash_equivalents=row.get('Cash Equivalents', None),
            cash_financial=row.get('Cash Financial', None)
        )
        for _, row in new_records.iterrows()
    ]

    try:
        # Bulk insert the new BalanceSheet objects
        if to_insert:
            session.bulk_save_objects(to_insert)
            session.commit()
            print(f"Inserted {len(to_insert)} new balance sheet entries for ticker {ticker.symbol}")

    except Exception as e:
        # Rollback in case of error
        session.rollback()
        print(f"Error occurred: {e}")


def handle_balance_sheet(ticker: Ticker, balance_sheet: pd.DataFrame, period_type: str, session: sess.Session) -> None:
    """
    Handle the insertion of balance sheet data into the database.

    :param ticker: The Ticker object.
    :param balance_sheet: DataFrame containing the balance sheet data.
    :param period_type: The period type of the data (e.g., 'annual' or 'quarterly').
    :param session: SQLAlchemy session for database operations.
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
        date = row['date']

        # Check if a record with the same ticker_id, date, and period_type already exists
        existing_record = session.query(BalanceSheet).filter(
            and_(
                BalanceSheet.ticker_id == ticker.id,
                BalanceSheet.date == date,
                BalanceSheet.period_type == period_type
            )
        ).first()

        if not existing_record:
            # Create a new BalanceSheet object
            new_balance_sheet = BalanceSheet(
                ticker_id=ticker.id,
                date=date,
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
                session.add(new_balance_sheet)
                # Commit the session to the database
                session.commit()
                print(f"{ticker.symbol} - {'Balance Sheet'.rjust(50)} - inserted balance sheet for date {date} ({period_type})")

            except Exception as e:
                # Rollback the transaction in case of an error
                session.rollback()
                print(f"Error occurred: {e}")


def handle_cash_flow_bulk(ticker, cash_flow: pd.DataFrame, period_type: str, session: sess.Session) -> None:
    """
    Handle the insertion of cash flow data into the database.

    :param ticker: The Ticker object.
    :param cash_flow: DataFrame containing the cash flow data.
    :param period_type: The period type of the data (e.g., 'annual' or 'quarterly').
    :param session: SQLAlchemy session for database operations.
    """
    # Transpose the DataFrame to have dates as rows
    cash_flow = cash_flow.T

    # give "Date" name to index
    cash_flow.index.name = "Date"

    # Reset the index to include the date as a column
    cash_flow.reset_index(inplace=True)

    # Normalize column names and handle any renaming needed (matching the SQLAlchemy model attributes)
    cash_flow.columns = [col.replace(' ', '_').lower() for col in cash_flow.columns]

    # Extract existing dates and period types from the database for this ticker
    existing_dates = (
        session
        .query(CashFlow.date)
        .filter(
            and_(
                CashFlow.ticker_id == ticker.id,
                CashFlow.period_type == period_type
            )
        )
        .all()
    )
    existing_dates = {pd.to_datetime(date) for date, in existing_dates}

    # Filter out rows that already exist in the database
    new_cash_flow_data = cash_flow[
        ~cash_flow.apply(lambda row: row['date'] in existing_dates, axis=1)
    ]

    # Prepare a list of CashFlow objects to insert
    to_insert = [
        CashFlow(
            ticker_id=ticker.id,
            date=row.get('date'),
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
        for _, row in new_cash_flow_data.iterrows()
    ]

    try:
        # Bulk insert the new BalanceSheet objects
        if to_insert:
            session.bulk_save_objects(to_insert)
            session.commit()
            print(f"Inserted {len(to_insert)} new cash flows entries for ticker {ticker.symbol}")

    except Exception as e:
        # Rollback in case of error
        session.rollback()
        print(f"Error occurred: {e}")


def handle_cash_flow(ticker: Ticker, cash_flow: pd.DataFrame, period_type: str, session: sess.Session) -> None:
    """
    Handle the insertion of cash flow data into the database.

    :param ticker: The Ticker object.
    :param cash_flow: DataFrame containing the cash flow data.
    :param period_type: The period type of the data (e.g., 'annual' or 'quarterly').
    :param session: SQLAlchemy session for database operations.
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
        date = row['date']

        # Check if a record with the same ticker_id, date, and period_type already exists
        existing_record = session.query(CashFlow).filter(
            and_(
                CashFlow.ticker_id == ticker.id,
                CashFlow.date == date,
                CashFlow.period_type == period_type
            )
        ).first()

        if not existing_record:
            # Create a new CashFlow object
            new_cash_flow = CashFlow(
                ticker_id=ticker.id,
                date=date,
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
                session.add(new_cash_flow)
                # Commit the session to the database
                session.commit()
                print(f"{ticker.symbol} - {'Cash Flow'.rjust(50)} - inserted cash flow for date {date} ({period_type})")

            except Exception as e:
                # Rollback the transaction in case of an error
                session.rollback()
                print(f"Error occurred: {e}")


def handle_financials_bulk(ticker, financials: pd.DataFrame, period_type: str, session: sess.Session) -> None:
    """
    Handle the insertion of financials data into the database.

    :param ticker: The Ticker object.
    :param financials: DataFrame containing the financials data.
    :param period_type: The period type of the data (e.g., 'annual' or 'quarterly').
    :param session: SQLAlchemy session for database operations.
    """
    # Transpose the DataFrame to have dates as rows
    financials = financials.T

    # Give "Date" name to index
    financials.index.name = "Date"

    # Reset the index to include the date as a column
    financials.reset_index(inplace=True)

    # Normalize column names and handle any renaming needed (matching the SQLAlchemy model attributes)
    financials.columns = [col.replace(' ', '_').lower() for col in financials.columns]

    # Extract existing dates and period types from the database for this ticker
    existing_dates = (
        session
        .query(Financials.date)
        .filter(
            and_(
                Financials.ticker_id == ticker.id,
                Financials.period_type == period_type
            )
        )
        .all()
    )
    existing_dates = {pd.to_datetime(date) for date, in existing_dates}

    # Filter out rows that already exist in the database
    new_financials_data = financials[
        ~financials.apply(lambda row: row['date'] in existing_dates, axis=1)
    ]

    # Prepare a list of Financials objects to insert
    to_insert = [
        Financials(
            ticker_id=ticker.id,
            date=row.get('date', None),
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
            diluted_net_income_available_to_common_stockholders=row.get('diluted_net_income_available_to_common_stockholders', None),
            net_income_common_stockholders=row.get('net_income_common_stockholders', None),
            net_income=row.get('net_income', None),
            net_income_including_non_controlling_interests=row.get('net_income_including_non_controlling_interests', None),
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
        for _, row in new_financials_data.iterrows()
    ]

    try:
        # Bulk insert the new Financials objects
        if to_insert:
            session.bulk_save_objects(to_insert)
            session.commit()
            print(f"Inserted {len(to_insert)} new {period_type} financials entries for ticker {ticker.symbol}")

    except Exception as e:
        # Rollback in case of error
        session.rollback()
        print(f"Error occurred: {e}")


def handle_financials(ticker: Ticker, financials: pd.DataFrame, period_type: str, session: sess.Session) -> None:
    """
    Handle the insertion of financials data into the database.

    :param ticker: The Ticker object.
    :param financials: DataFrame containing the financials data.
    :param period_type: The period type of the data (e.g., 'annual' or 'quarterly').
    :param session: SQLAlchemy session for database operations.
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
        date = row['date']

        # Check if a record with the same ticker_id, date, and period_type already exists
        existing_record = session.query(Financials).filter(
            and_(
                Financials.ticker_id == ticker.id,
                Financials.date == date,
                Financials.period_type == period_type
            )
        ).first()

        if not existing_record:
            # Create a new Financials object
            new_financial = Financials(
                ticker_id=ticker.id,
                date=date,
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
                session.add(new_financial)
                # Commit the session to the database
                session.commit()
                print(f"{ticker.symbol} - {'Financials'.rjust(50)} - inserted financials for date {date} ({period_type})")

            except Exception as e:
                # Rollback the transaction in case of an error
                session.rollback()
                print(f"Error occurred: {e}")


def handle_calendar(ticker: Ticker, calendar: dict, session: sess.Session) -> None:
    """
    Handle the insertion or update of calendar dates into the DB.

    :param ticker: The Ticker object.
    :param calendar: Dictionary containing the dividend and earnings data.
    :param session: SQLAlchemy session for database operations.
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

    handle_record_update(
        ticker=ticker,
        new_record_data=new_record_data,
        model_class=Calendar,
        session=session
    )


def handle_earnings_dates(ticker: Ticker, earnings_dates: pd.DataFrame, session: sess.Session) -> None:
    """
    Handle the insertion of earnings dates data into the database.

    :param ticker: The Ticker object.
    :param earnings_dates: DataFrame containing the earnings dates data.
    :param session: SQLAlchemy session for database operations.
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
        changed |= handle_record_update(
            ticker=ticker,
            new_record_data=new_record_data,
            model_class=EarningsDates,
            session=session,
            additional_filters=[
                EarningsDates.date == row['date']
            ],
            print_no_changes=False  # Do not print "no changes detected" messages
        )

    if not changed:
        print(f"{ticker.symbol} - {'Earnings Dates'.rjust(50)} - no changes detected")


def handle_info_company_address(ticker: Ticker, info_data: dict, session: sess.Session) -> None:
    """
    Handle the insertion or update of company address information into the database.

    :param ticker: The Ticker object.
    :param info_data: Dictionary containing the company address information from stock.info.
    :param session: SQLAlchemy session for database operations.
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
    handle_record_update(
        ticker=ticker,
        new_record_data=new_record_data,
        model_class=InfoCompanyAddress,
        session=session
    )


def handle_sector_industry_history(ticker: Ticker, info_data: dict, session: sess.Session) -> None:
    """
    Handle the insertion or update of sector and industry history data into the database.

    :param ticker: The Ticker object.
    :param info_data: Dictionary containing the stock information, including sector and industry.
    :param session: SQLAlchemy session for database operations.
    """
    try:
        # Extract the necessary information from stock_info
        sector = info_data.get('sector')
        industry = info_data.get('industry')
        start_date = pd.to_datetime('today').date()  # Use today's date as the start date for the new record

        # Check for an existing record with a NULL end_date for the same ticker_id
        existing_record = session.query(InfoSectorIndustryHistory).filter(
            InfoSectorIndustryHistory.ticker_id == literal(ticker.id),
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
                    f"{ticker.symbol} - Sector/Industry changed from Sector: {existing_record.sector}, "
                    f"Industry: {existing_record.industry} to Sector: {sector}, Industry: {industry}."
                )

                # Update the existing record to set the end_date
                existing_record.end_date = start_date

                # Create a new record with the new sector and industry
                new_sector_industry_record = InfoSectorIndustryHistory(
                    ticker_id=ticker.id,
                    sector=sector,
                    industry=industry,
                    start_date=start_date,
                    end_date=None  # This is the current valid entry
                )

                session.add(new_sector_industry_record)

                # Commit the changes to the database
                session.commit()
                for change in changes_log:
                    print(change)

            else:
                print(f"{ticker.symbol} - {'Sector/Industry'.rjust(50)} - no changes detected")

        else:
            # Create a new record if no existing record with a NULL end_date is found
            new_sector_industry_record = InfoSectorIndustryHistory(
                ticker_id=ticker.id,
                sector=sector,
                industry=industry,
                start_date=start_date,
                end_date=None  # This is the current valid entry
            )

            # Add the object to the session
            session.add(new_sector_industry_record)
            # Commit the session to the database
            session.commit()
            print(f"{ticker.symbol} - {'Sector/Industry'.rjust(50)} - INSERTED successfully.")

    except Exception as e:
        # Rollback the transaction in case of an error
        session.rollback()
        print(f"Error occurred: {e}")


def handle_info_target_price_and_recommendation(ticker: Ticker, info_data: dict, session: sess.Session) -> None:
    """
    Handle the insertion or update of target price and recommendation info into the database.

    :param ticker: The Ticker object.
    :param info_data: Dictionary containing the target price and recommendation data.
    :param session: SQLAlchemy session for database operations.
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

    handle_record_update(
        ticker=ticker,
        new_record_data=new_record_data,
        model_class=InfoTargetPriceAndRecommendation,
        session=session
    )


def handle_info_governance(ticker: Ticker, governance_datainfo_data: dict, session: sess.Session) -> None:
    """
    Handle the insertion or update of governance info into the database.

    :param ticker: The Ticker object.
    :param governance_datainfo_data: Dictionary containing the governance data.
    :param session: SQLAlchemy session for database operations.
    """
    # Prepare the new record data for InfoGovernance
    new_record_data = {
        'audit_risk': governance_datainfo_data.get('auditRisk', None),
        'board_risk': governance_datainfo_data.get('boardRisk', None),
        'compensation_risk': governance_datainfo_data.get('compensationRisk', None),
        'shareholder_rights_risk': governance_datainfo_data.get('shareHolderRightsRisk', None),
        'overall_risk': governance_datainfo_data.get('overallRisk', None),
        'governance_epoch_date': governance_datainfo_data.get('governanceEpochDate', None),
        'compensation_as_of_epoch_date': governance_datainfo_data.get('compensationAsOfEpochDate', None)
    }

    # Call the generic function to handle the update or insertion
    handle_record_update(
        ticker=ticker,
        new_record_data=new_record_data,
        model_class=InfoGovernance,
        session=session,
        additional_filters=None  # No additional filters needed in this case
    )


def handle_info_cash_and_financial_ratios(ticker: Ticker, info_data: dict, session: sess.Session) -> None:
    """
    Handle the insertion or update of cash and financial ratios info into the database.

    :param ticker: The Ticker object.
    :param info_data: Dictionary containing the financial ratios data.
    :param session: SQLAlchemy session for database operations.
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

    handle_record_update(
        ticker=ticker,
        new_record_data=new_record_data,
        model_class=InfoCashAndFinancialRatios,
        session=session
    )


def handle_info_market_and_financial_metrics(ticker: Ticker, info_data: dict, session: sess.Session) -> None:
    """
    Handle the insertion or update of market and financial metrics info into the database.

    :param ticker: The Ticker object.
    :param info_data: Dictionary containing the market and financial metrics data.
    :param session: SQLAlchemy session for database operations.
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

    handle_record_update(
        ticker=ticker,
        new_record_data=new_record_data,
        model_class=InfoMarketAndFinancialMetrics,
        session=session
    )


def handle_info_general_stock(ticker: Ticker, isin: str, info_data: dict, history_metadata: dict, session: sess.Session) -> None:
    """
    Handle the insertion or update of general stock information into the database.

    :param ticker: The Ticker object.
    :param isin: The ISIN of the stock.
    :param info_data: Dictionary containing the general stock information from stock.info.
    :param history_metadata: Dictionary containing the general stock information from stock.history_metadata.
    :param session: SQLAlchemy session for database operations.
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
    handle_record_update(
        ticker=ticker,
        new_record_data=new_record_data,
        model_class=InfoGeneralStock,
        session=session
    )


def handle_info_trading_session(ticker: Ticker, info: dict, basic_info: dict, history_metadata: dict, session: sess.Session) -> None:
    """
    Handle the insertion or update of trading session information into the database.

    :param ticker: The Ticker object.
    :param info: Dictionary containing the info data from stock.info.
    :param basic_info: Dictionary containing the basic info data from stock.basic_info.
    :param history_metadata: Dictionary containing the history metadata from stock.history_metadata.
    :param session: SQLAlchemy session for database operations.
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
    handle_record_update(
        ticker=ticker,
        new_record_data=new_record_data,
        model_class=InfoTradingSession,
        session=session
    )


def handle_insider_purchases(ticker: Ticker, insider_purchases: pd.DataFrame, session: sess.Session) -> None:
    """
    Handle the insertion or update of insider purchases data into the database.

    :param ticker: The Ticker object.
    :param insider_purchases: DataFrame containing the insider purchases data.
    :param session: SQLAlchemy session for database operations.
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
        handle_record_update(
            ticker=ticker,
            new_record_data=combined_record_data,
            model_class=InsiderPurchases,
            session=session
        )


def handle_insider_roster_holders(ticker: Ticker, insider_roster_holders: pd.DataFrame, session: sess.Session) -> None:
    """
    Handle the insertion or update of insider roster holders data into the database.

    :param ticker: The Ticker object.
    :param insider_roster_holders: DataFrame containing the insider roster holders data.
    :param session: SQLAlchemy session for database operations.
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
        changed |= handle_record_update(
            ticker=ticker,
            new_record_data=new_record_data,
            model_class=InsiderRosterHolders,
            session=session,
            additional_filters=[InsiderRosterHolders.name == new_record_data['name']],
            print_no_changes=False
        )

    if not changed:
        print(f"{ticker.symbol} - {'Insider Roster Holders'.rjust(50)} - no changes detected")


def handle_insider_transactions(ticker: Ticker, insider_transactions: pd.DataFrame, session: sess.Session) -> None:
    """
    Handle the insertion or update of insider transactions data into the database.

    :param ticker: The Ticker object.
    :param insider_transactions: DataFrame containing the insider transactions data.
    :param session: SQLAlchemy session for database operations.
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
        changed |= handle_record_update(
            ticker=ticker,
            new_record_data=new_record_data,
            model_class=InsiderTransactions,
            session=session,
            additional_filters=[InsiderTransactions.start_date == row['start_date'].date(),
                                InsiderTransactions.insider == new_record_data['insider'],
                                InsiderTransactions.value == new_record_data['value']],
            print_no_changes=False
        )

    if not changed:
        print(f"{ticker.symbol} - {'Insider Transactions'.rjust(50)} - no changes detected")


def handle_institutional_holders(ticker: Ticker, institutional_holders: pd.DataFrame, session: sess.Session) -> None:
    """
    Handle the insertion or update of institutional holders data into the database.

    :param ticker: The Ticker object.
    :param institutional_holders: DataFrame containing the institutional holders data.
    :param session: SQLAlchemy session for database operations.
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
        changed |= handle_record_update(
            ticker=ticker,
            new_record_data=new_record_data,
            model_class=InstitutionalHolders,
            session=session,
            additional_filters=[
                InstitutionalHolders.date_reported == row['date_reported'],
                InstitutionalHolders.holder == row['holder']
            ],
            print_no_changes=False  # Do not print "no changes detected" messages
        )

    if not changed:
        print(f"{ticker.symbol} - {'Institutional Holders'.rjust(50)} - no changes detected")


def handle_major_holders(ticker: Ticker, major_holders: pd.DataFrame, session: sess.Session) -> None:
    """
    Handle the insertion or update of major holders data into the database from a DataFrame.

    :param ticker: The Ticker object.
    :param major_holders: DataFrame containing the major holders data as a single record.
    :param session: SQLAlchemy session for database operations.
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
    handle_record_update(
        ticker=ticker,
        new_record_data=new_record_data,
        model_class=MajorHolders,
        session=session
    )


def handle_mutual_fund_holders(ticker: Ticker, mutual_fund_holders: pd.DataFrame, session: sess.Session) -> None:
    """
    Handle the insertion or update of mutual fund holders data into the database.

    :param ticker: The Ticker object.
    :param mutual_fund_holders: DataFrame containing the mutual fund holders data.
    :param session: SQLAlchemy session for database operations.
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
        changed |= handle_record_update(
            ticker=ticker,
            new_record_data=new_record_data,
            model_class=MutualFundHolders,
            session=session,
            additional_filters=[
                MutualFundHolders.date_reported == row['date_reported'],
                MutualFundHolders.holder == row['holder']
            ],
            print_no_changes=False  # Do not print "no changes detected" messages
        )

    if not changed:
        print(f"{ticker.symbol} - {'Mutual Fund Holders'.rjust(50)} - no changes detected")


def handle_recommendations(ticker: Ticker, recommendations: pd.DataFrame, session: sess.Session) -> None:
    """
    Handle the insertion or update of recommendations data into the database.

    :param ticker: The Ticker object.
    :param recommendations: DataFrame containing the recommendations data.
    :param session: SQLAlchemy session for database operations.
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
        changed |= handle_record_update(
            ticker=ticker,
            new_record_data=new_record_data,
            model_class=Recommendations,
            session=session,
            additional_filters=[
                Recommendations.period == row['period']
            ],
            print_no_changes=False  # Do not print "no changes detected" messages
        )

    if not changed:
        print(f"{ticker.symbol} - {'Recommendations'.rjust(50)} - no changes detected")


def handle_upgrades_downgrades(ticker: Ticker, upgrades_downgrades: pd.DataFrame, session: sess.Session) -> None:
    """
    Handle the insertion or update of upgrades and downgrades data into the database.

    :param ticker: The Ticker object.
    :param upgrades_downgrades: DataFrame containing the upgrades and downgrades data.
    :param session: SQLAlchemy session for database operations.
    """
    # Rename index to 'date' and reset the index to make it a column
    upgrades_downgrades.index.name = 'date'
    upgrades_downgrades.reset_index(inplace=True)

    # Normalize column names to match SQLAlchemy model attributes
    upgrades_downgrades.columns = [col.lower().replace(' ', '_') for col in upgrades_downgrades.columns]

    # Convert the date column to datetime.date objects (if it's not already in that format)
    upgrades_downgrades['date'] = pd.to_datetime(upgrades_downgrades['date']).dt.date  # Ensure it's date only

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
        changed |= handle_record_update(
            ticker=ticker,
            new_record_data=new_record_data,
            model_class=UpgradesDowngrades,
            session=session,
            additional_filters=[
                UpgradesDowngrades.date == row['date'],
                UpgradesDowngrades.firm == row['firm']
            ],
            print_no_changes=False  # Do not print "no changes detected" messages
        )

    if not changed:
        print(f"{ticker.symbol} - {'Upgrades/Downgrades'.rjust(50)} - no changes detected")


def main():
    # __ init file manager __
    config_manager = FileManager()
    postgre_url = config_manager.get_postgre_url("POSTGRE_URL_LOCAL_STOCK")

    # Create new session
    session = session_local()

    # Get the balance sheet data for a sample ticker
    ticker = "AAPL"
    stock = yf.Ticker(ticker)

    # __ handle ticker __
    ticker = handle_ticker(ticker=ticker, info=stock.info, session=session)

    # __ handle balance sheet - annual and quarterly__
    handle_balance_sheet(ticker=ticker, balance_sheet=stock.balance_sheet, period_type="annual", session=session)
    handle_balance_sheet(ticker=ticker, balance_sheet=stock.quarterly_balance_sheet, period_type="quarterly", session=session)

    # __ handle cash flow - annual and quarterly__
    handle_cash_flow(ticker=ticker, cash_flow=stock.cashflow, period_type="annual", session=session)
    handle_cash_flow(ticker=ticker, cash_flow=stock.quarterly_cashflow, period_type="quarterly", session=session)

    # __ handle financials - annual and quarterly__
    handle_financials(ticker=ticker, financials=stock.financials, period_type="annual", session=session)
    handle_financials(ticker=ticker, financials=stock.quarterly_financials, period_type="quarterly", session=session)

    # __ handle actions __
    handle_actions(ticker=ticker, actions=stock.actions, session=session)

    # __ handle calendar __
    handle_calendar(ticker=ticker, calendar=stock.calendar, session=session)

    # __ handle earnings dates __
    handle_earnings_dates(ticker=ticker, earnings_dates=stock.earnings_dates, session=session)

    # __ handle info company address __
    handle_info_company_address(ticker=ticker, info_data=stock.info, session=session)

    # __ handle sector industry history __
    handle_sector_industry_history(ticker=ticker, info_data=stock.info, session=session)

    # __ handle info company target price and recommendation __
    handle_info_target_price_and_recommendation(ticker=ticker, info_data=stock.info, session=session)

    # __ handle info governance __
    handle_info_governance(ticker=ticker, governance_datainfo_data=stock.info, session=session)

    # __ handle info cash and financial ratios __
    handle_info_cash_and_financial_ratios(ticker=ticker, info_data=stock.info, session=session)

    # __ handle info market and financial metrics __
    handle_info_market_and_financial_metrics(ticker=ticker, info_data=stock.info, session=session)

    # __ handle info general stock __
    handle_info_general_stock(ticker=ticker, isin=stock.isin, info_data=stock.info, history_metadata=stock.history_metadata, session=session)

    # __ handle insider purchases __
    handle_insider_purchases(ticker=ticker, insider_purchases=stock.insider_purchases, session=session)

    # __ handle insider roster holders __
    handle_insider_roster_holders(ticker=ticker, insider_roster_holders=stock.insider_roster_holders, session=session)

    # __ handle insider transactions __
    handle_insider_transactions(ticker=ticker, insider_transactions=stock.insider_transactions, session=session)

    # __ handle institutional holders __
    handle_institutional_holders(ticker=ticker, institutional_holders=stock.institutional_holders, session=session)

    # __ handle major holders __
    handle_major_holders(ticker=ticker, major_holders=stock.major_holders, session=session)

    # __ handle mutual fund holders __
    handle_mutual_fund_holders(ticker=ticker, mutual_fund_holders=stock.mutualfund_holders, session=session)

    # __ handle recommendations __
    handle_recommendations(ticker=ticker, recommendations=stock.recommendations, session=session)

    # __ handle upgrades downgrades __
    handle_upgrades_downgrades(ticker=ticker, upgrades_downgrades=stock.upgrades_downgrades, session=session)

    # __ handle info trading session __  # TODO: change basic_info
    handle_info_trading_session(ticker=ticker, info=stock.info, basic_info=stock.basic_info, history_metadata=stock.history_metadata, session=session)

    session.close()
    print('end')


if __name__ == "__main__":
    main()
