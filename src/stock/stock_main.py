import yfinance as yf
from time import time
from sqlalchemy.orm import session as sess

from src.common.file_manager.FileManager import FileManager
from src.common.tools.library import seconds_to_time

from TickerService import TickerService
from database import engine, session_local


def update_ticker(symbol: str, session: sess.Session):
    # __ start tracking the elapsed time __
    start_time = time()

    # __ get the balance sheet data for a sample ticker __
    stock = yf.Ticker(symbol)

    # __ update the database with new data __
    ticker_service = TickerService(session=session, symbol=symbol)
    ticker_service.handle_ticker(info=stock.info)
    ticker_service.handle_balance_sheet(balance_sheet=stock.balance_sheet, period_type="annual")
    ticker_service.handle_balance_sheet(balance_sheet=stock.quarterly_balance_sheet, period_type="quarterly")
    ticker_service.handle_cash_flow(cash_flow=stock.cashflow, period_type="annual")
    ticker_service.handle_cash_flow(cash_flow=stock.quarterly_cashflow, period_type="quarterly")
    ticker_service.handle_financials(financials=stock.financials, period_type="annual")
    ticker_service.handle_financials(financials=stock.quarterly_financials, period_type="quarterly")
    ticker_service.handle_actions(actions=stock.actions)
    ticker_service.handle_calendar(calendar=stock.calendar)
    ticker_service.handle_earnings_dates(earnings_dates=stock.earnings_dates)
    ticker_service.handle_info_company_address(info_data=stock.info)
    ticker_service.handle_sector_industry_history(info_data=stock.info)
    ticker_service.handle_info_target_price_and_recommendation(info_data=stock.info)
    ticker_service.handle_info_governance(info_data=stock.info)
    ticker_service.handle_info_cash_and_financial_ratios(info_data=stock.info)
    ticker_service.handle_info_market_and_financial_metrics(info_data=stock.info)
    ticker_service.handle_info_general_stock(isin=stock.isin, info_data=stock.info, history_metadata=stock.history_metadata)
    ticker_service.handle_insider_purchases(insider_purchases=stock.insider_purchases)
    ticker_service.handle_insider_roster_holders(insider_roster_holders=stock.insider_roster_holders)
    ticker_service.handle_insider_transactions(insider_transactions=stock.insider_transactions)
    ticker_service.handle_institutional_holders(institutional_holders=stock.institutional_holders)
    ticker_service.handle_major_holders(major_holders=stock.major_holders)
    ticker_service.handle_mutual_fund_holders(mutual_fund_holders=stock.mutualfund_holders)
    ticker_service.handle_recommendations(recommendations=stock.recommendations)
    ticker_service.handle_upgrades_downgrades(upgrades_downgrades=stock.upgrades_downgrades)
    ticker_service.handle_info_trading_session(info=stock.info, basic_info=stock.basic_info, history_metadata=stock.history_metadata)

    # __ stop tracking the elapsed time and print the difference __
    end_time = time()
    total_time = seconds_to_time(end_time - start_time)
    print(f"{symbol} - Total time: {total_time['minutes']} min {total_time['seconds']} sec\n")


def main():
    # __ sqlAlchemy __ create new session
    session = session_local()

    # __ get all tickers __
    symbols = ["AAPL", "MSFT", "TSLA", "GME"]
    # symbols = ["TSLA"]

    # __ update all tickers __
    for symbol in symbols:
        update_ticker(symbol=symbol, session=session)

    # __ sqlAlchemy __ close the session
    session.close()
    print('end')


if __name__ == "__main__":
    main()
