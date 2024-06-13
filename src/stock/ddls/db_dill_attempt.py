import pandas as pd
import yfinance as yf
from psycopg2 import sql
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from src.common.file_manager.FileManager import FileManager
from src.common.postgre.PostgreManager import PostgreManager
from src.common.tools.library import get_environ


def get_ticker_actions(ticker_: yf.Ticker) -> pd.DataFrame:
    """
    Fetch historical actions (dividends, stock splits) for a given ticker symbol.

    :param ticker_: A yfinance Ticker object.
    :return: A DataFrame containing dates and actions (dividends, stock splits).
    """

    # Fetch historical actions (dividends and stock splits)
    actions = ticker_.actions

    # Reset the index to include the date as a column
    actions.reset_index(inplace=True)

    return actions


def get_ticker_balance_sheet(ticker_: yf.Ticker) -> pd.DataFrame:
    """
    Fetch historical balance sheet data for a given ticker symbol.

    :param ticker_: A yfinance Ticker object.
    :return: A DataFrame containing balance sheet data.
    """

    # Fetch historical balance sheet data
    balance_sheet = ticker_.balance_sheet

    return balance_sheet


def insert_actions_to_db(actions: pd.DataFrame, ticker_id: str, pm: PostgreManager, commit: bool = False) -> None:
    """
    Insert the fetched actions into the PostgreSQL database.

    :param actions: DataFrame containing the actions' data.
    :param ticker_id: The ID of the ticker in the database.
    :param pm: PostgreManager object to interact with the database.
    :param commit: Whether to commit the changes to the database.
    """
    # SQL query to insert data into the actions table
    insert_query = sql.SQL("""
        INSERT INTO actions (ticker_id, date, dividends, stock_splits)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (ticker_id, date) DO NOTHING
    """)

    # Iterate over each row in the actions DataFrame
    for _, row in actions.iterrows():
        date = row['Date']  # Extract the date
        dividends = row['Dividends'] if 'Dividends' in row else None  # Extract dividends if available
        stock_splits = row['Stock Splits'] if 'Stock Splits' in row else None  # Extract stock splits if available

        # Execute the insert query
        pm.insert_query(query=insert_query, values=(ticker_id, date, dividends, stock_splits))

    pm.commit() if commit else None  # Commit the changes to the database


def insert_balance_sheet_to_db(balance_sheet: pd.DataFrame, ticker_id: str, pm: PostgreManager, commit: bool = False) -> None:
    """
    Insert the fetched balance sheet data into the PostgreSQL database, ensuring no duplicate (ticker_id, date) entries.

    :param balance_sheet: DataFrame containing the balance sheet data.
    :param ticker_id: The ID of the ticker in the database.
    :param pm: PostgreManager object to interact with the database.
    :param commit: Whether to commit the changes to the database.
    """

    fields = [
        "ticker_id", "date", "treasury_shares_number", "ordinary_shares_number", "share_issued",
        "net_debt", "total_debt", "tangible_book_value", "invested_capital", "working_capital",
        "net_tangible_assets", "common_stock_equity", "total_capitalization", "total_equity_gross_minority_interest",
        "stockholders_equity", "gains_losses_not_affecting_retained_earnings", "other_equity_adjustments",
        "retained_earnings", "capital_stock", "common_stock", "total_liabilities_net_minority_interest",
        "total_non_current_liabilities_net_minority_interest", "other_non_current_liabilities",
        "trade_and_other_payables_non_current", "long_term_debt_and_capital_lease_obligation",
        "long_term_debt", "current_liabilities", "other_current_liabilities", "current_deferred_liabilities",
        "current_deferred_revenue", "current_debt_and_capital_lease_obligation", "current_debt",
        "other_current_borrowings", "commercial_paper", "payables_and_accrued_expenses", "payables",
        "accounts_payable", "total_assets", "total_non_current_assets", "other_non_current_assets",
        "non_current_deferred_assets", "non_current_deferred_taxes_assets", "investments_and_advances",
        "other_investments", "investment_in_financial_assets", "available_for_sale_securities",
        "net_ppe", "accumulated_depreciation", "gross_ppe", "leases", "machinery_furniture_equipment",
        "land_and_improvements", "properties", "current_assets", "other_current_assets", "inventory",
        "receivables", "other_receivables", "accounts_receivable", "cash_cash_equivalents_and_short_term_investments",
        "other_short_term_investments", "cash_and_cash_equivalents", "cash_equivalents", "cash_financial"
    ]

    # SQL query to insert data into the balance_sheet table with conflict handling
    insert_query = sql.SQL(f"""
                            INSERT INTO balance_sheet ({sql.SQL(', ').join(map(sql.Identifier, fields))})
                            VALUES ({sql.SQL(', ').join(sql.Placeholder() * len(fields))})
                            ON CONFLICT (ticker_id, date) DO NOTHING
                            """)

    # Iterate over each column in the DataFrame
    for date in balance_sheet.columns:
        row = balance_sheet[date]
        date = pd.to_datetime(date).strftime('%Y-%m-%d')  # Convert column to date string

        # Execute the insert query with conflict handling
        pm.insert_query(
            query=insert_query,
            values=(
                ticker_id,
                date,
                row.get('Treasury Stock', None),
                row.get('Ordinary Shares', None),
                row.get('Shares Issued', None),
                row.get('Net Debt', None),
                row.get('Total Debt', None),
                row.get('Tangible Book Value', None),
                row.get('Invested Capital', None),
                row.get('Working Capital', None),
                row.get('Net Tangible Assets', None),
                row.get('Common Stock Equity', None),
                row.get('Total Capitalization', None),
                row.get('Total Equity Gross Minority Interest', None),
                row.get('Stockholders Equity', None),
                row.get('Gains Losses Not Affecting Retained Earnings', None),
                row.get('Other Equity Adjustments', None),
                row.get('Retained Earnings', None),
                row.get('Capital Stock', None),
                row.get('Common Stock', None),
                row.get('Total Liabilities Net Minority Interest', None),
                row.get('Total Non Current Liabilities Net Minority Interest', None),
                row.get('Other Non Current Liabilities', None),
                row.get('Trade and Other Payables Non Current', None),
                row.get('Long Term Debt and Capital Lease Obligation', None),
                row.get('Long Term Debt', None),
                row.get('Current Liabilities', None),
                row.get('Other Current Liabilities', None),
                row.get('Current Deferred Liabilities', None),
                row.get('Current Deferred Revenue', None),
                row.get('Current Debt and Capital Lease Obligation', None),
                row.get('Current Debt', None),
                row.get('Other Current Borrowings', None),
                row.get('Commercial Paper', None),
                row.get('Payables and Accrued Expenses', None),
                row.get('Payables', None),
                row.get('Accounts Payable', None),
                row.get('Total Assets', None),
                row.get('Total Non Current Assets', None),
                row.get('Other Non Current Assets', None),
                row.get('Non Current Deferred Assets', None),
                row.get('Non Current Deferred Taxes Assets', None),
                row.get('Investments and Advances', None),
                row.get('Other Investments', None),
                row.get('Investment in Financial Assets', None),
                row.get('Available For Sale Securities', None),
                row.get('Net PPE', None),
                row.get('Accumulated Depreciation', None),
                row.get('Gross PPE', None),
                row.get('Leases', None),
                row.get('Machinery Furniture Equipment', None),
                row.get('Land and Improvements', None),
                row.get('Properties', None),
                row.get('Current Assets', None),
                row.get('Other Current Assets', None),
                row.get('Inventory', None),
                row.get('Receivables', None),
                row.get('Other Receivables', None),
                row.get('Accounts Receivable', None),
                row.get('Cash Cash Equivalents and Short Term Investments', None),
                row.get('Other Short Term Investments', None),
                row.get('Cash and Cash Equivalents', None),
                row.get('Cash Equivalents', None),
                row.get('Cash Financial', None)
            )
        )

    if commit:
        pm.commit()  # Commit the changes to the database if specified


def main():
    # __ init file manager __
    config_manager = FileManager()
    postgre_url = config_manager.get_postgre_url("POSTGRE_URL_LOCAL_STOCK")

    engine = create_engine(postgre_url)

    # Create a base class for our ORM classes
    Base = declarative_base()

    # Create a configured session class to interact with the database
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    # Get the balance sheet data for a sample ticker
    ticker = "AAPL"
    stock = yf.Ticker(ticker)

    # __ Ticker table __
    query = f"""SELECT * FROM ticker WHERE symbol = '{ticker}'"""
    results = postgre_manager.select_query(query)
    if not results:
        print(f"Adding {ticker} to the database")
        query = f"""INSERT INTO ticker 
                    (symbol, company_name, business_summary) 
                    VALUES ($${ticker}$$, $${stock.info.get('longName', 'N/A')}$$, $${stock.info.get('longBusinessSummary', 'N/A')}$$)
                    """
        postgre_manager.insert_query(query, commit=True)

    ticker_id = postgre_manager.select_query(f"""SELECT id FROM ticker WHERE symbol = '{ticker}'""")[0]['id']

    # __ Actions table __
    actions = get_ticker_actions(stock)
    insert_actions_to_db(actions, ticker_id, postgre_manager, commit=True)

    # __ Balance sheet table __
    balance_sheet = get_ticker_balance_sheet(stock)
    insert_balance_sheet_to_db(balance_sheet, ticker_id, postgre_manager, commit=True)

    postgre_manager.close_connection()
    print('end')


if __name__ == "__main__":
    main()
