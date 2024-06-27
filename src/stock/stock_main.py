import yfinance as yf
import pandas as pd
from time import time
from sqlalchemy.orm import session as sess

from src.common.file_manager.FileManager import FileManager
from src.common.tools.library import seconds_to_time, safe_execute

from TickerService import TickerService
from database import engine, session_local
from CandleDataInterval import CandleDataInterval


def update_ticker(symbol: str, session: sess.Session):
    # __ start tracking the elapsed time __
    start_time = time()
    print(f"{symbol} - Start updating...")

    # __ get the balance sheet data for a sample ticker __
    stock = yf.Ticker(symbol)

    # __ update the database with new data __
    ticker_service = TickerService(session=session, symbol=symbol)

    # __ ticker __
    info = safe_execute(None, lambda: getattr(stock, "info"))
    ticker_service.handle_ticker(info=stock.info) if info is not None else None

    ticker_service.handle_balance_sheet(balance_sheet=stock.balance_sheet, period_type="annual")
    ticker_service.handle_balance_sheet(balance_sheet=stock.quarterly_balance_sheet, period_type="quarterly")
    ticker_service.handle_cash_flow(cash_flow=stock.cashflow, period_type="annual")
    ticker_service.handle_cash_flow(cash_flow=stock.quarterly_cashflow, period_type="quarterly")
    ticker_service.handle_financials(financials=stock.financials, period_type="annual")
    ticker_service.handle_financials(financials=stock.quarterly_financials, period_type="quarterly")
    ticker_service.handle_actions(actions=stock.actions)
    ticker_service.handle_calendar(calendar=stock.calendar)

    # __ earnings dates __
    earning_dates = safe_execute(None, lambda: getattr(stock, "earnings_dates"))
    ticker_service.handle_earnings_dates(earnings_dates=stock.earnings_dates) if earning_dates is not None else None

    # __ info __
    if info is not None:
        ticker_service.handle_info_company_address(info_data=stock.info)
        ticker_service.handle_sector_industry_history(info_data=stock.info)
        ticker_service.handle_info_target_price_and_recommendation(info_data=stock.info)
        ticker_service.handle_info_governance(info_data=stock.info)
        ticker_service.handle_info_cash_and_financial_ratios(info_data=stock.info)
        ticker_service.handle_info_market_and_financial_metrics(info_data=stock.info)

        isin = safe_execute(None, lambda: getattr(stock, "isin"))
        history_metadata = safe_execute(None, lambda: getattr(stock, "history_metadata"))
        if isin is not None and history_metadata is not None:
            ticker_service.handle_info_general_stock(isin=stock.isin, info_data=stock.info, history_metadata=stock.history_metadata)

    insider_purchases = safe_execute(None, lambda: getattr(stock, "insider_purchases"))
    ticker_service.handle_insider_purchases(insider_purchases=insider_purchases) if insider_purchases is not None else None

    insider_roster_holders = safe_execute(None, lambda: getattr(stock, "insider_roster_holders"))
    ticker_service.handle_insider_roster_holders(insider_roster_holders=insider_roster_holders) if insider_roster_holders is not None else None

    insider_transactions = safe_execute(None, lambda: getattr(stock, "insider_transactions"))
    ticker_service.handle_insider_transactions(insider_transactions=insider_transactions) if insider_transactions is not None else None

    institutional_holders = safe_execute(None, lambda: getattr(stock, "institutional_holders"))
    ticker_service.handle_institutional_holders(institutional_holders=institutional_holders) if institutional_holders is not None else None

    major_holders = safe_execute(None, lambda: getattr(stock, "major_holders"))
    ticker_service.handle_major_holders(major_holders=major_holders) if major_holders is not None else None

    mutual_fund_holders = safe_execute(None, lambda: getattr(stock, "mutualfund_holders"))
    ticker_service.handle_mutual_fund_holders(mutual_fund_holders=mutual_fund_holders) if mutual_fund_holders is not None else None

    recommendations = safe_execute(None, lambda: getattr(stock, "recommendations"))
    ticker_service.handle_recommendations(recommendations=stock.recommendations) if recommendations is not None else None

    upgrades_downgrades = safe_execute(None, lambda: getattr(stock, "upgrades_downgrades"))
    ticker_service.handle_upgrades_downgrades(upgrades_downgrades=stock.upgrades_downgrades) if upgrades_downgrades is not None else None

    if info is not None:
        basic_info = safe_execute(None, lambda: getattr(stock, "basic_info"))
        if basic_info is not None:
            ticker_service.handle_info_trading_session(info=stock.info, basic_info=stock.basic_info, history_metadata=stock.history_metadata)

    # __ handle candle data update/insert __
    intervals = list(CandleDataInterval)
    # intervals = ["1wk"]
    for interval in intervals:
        ticker_service.handle_candle_data(interval=interval)

    # __ stop tracking the elapsed time and print the difference __
    end_time = time()
    total_time = seconds_to_time(end_time - start_time)
    print(f"{symbol} - Total time: {total_time['minutes']} min {total_time['seconds']} sec\n")


def tickers_sp500(include_company_data=False):
    """Downloads list of tickers currently listed in the S&P 500"""

    # __ get list of all S&P 500 stocks __
    sp500 = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]
    # sp500["Symbol"] = sp500["Symbol"].str.replace(".", "-", regex=True)

    if include_company_data:
        return sp500

    sp_tickers = sp500.Symbol.tolist()
    sp_tickers = sorted(sp_tickers)

    return [x.replace(".", "-") for x in sp_tickers]


def main():
    # __ sqlAlchemy __ create new session
    session = session_local()

    # __ get all tickers __
    symbols = ["AAPL", "MSFT", "TSLA", "GME", "NVDA", "GOOGL", "NFLX", "META", "INTC", "AMZN", "BRK-B", "UNH", "XOM", "LLY"]
    symbols += ["JPM", "JNJ", "V", "PG", "MA", "AVGO", "HD", "CVX", "MRK", "ABBV", "COST", "PEP", "ADBE"]
    # symbols = symbols[-2:]
    # symbols = ["MSFT"]
    symbols = tickers_sp500()
    # symbols = symbols[378:]

    start_time = time()

    # __ update all tickers __
    for symbol in symbols:
        update_ticker(symbol=symbol, session=session)

    end_time = time()
    total_time = seconds_to_time(end_time - start_time)
    print(f"{'Total elapsed time:'.ljust(25)} {total_time['hours']} hours {total_time['minutes']} min {total_time['seconds']} sec")
    print(f"{'Total tickers:'.ljust(25)} {len(symbols)}")
    print(f"{'Average time per ticker:'.ljust(25)} {round((end_time - start_time) / len(symbols), 3)} sec")

    # __ sqlAlchemy __ close the session
    session.close()


def plot_candles():
    import pandas as pd
    from database import engine, session_local
    from TickerService import Ticker
    from models import CandleDataDay
    from mpl_finance import candlestick_ohlc
    import matplotlib.dates as mpl_dates
    from src.common.tools.library import plt

    session = session_local()

    query = (session.query(CandleDataDay)
             .join(Ticker)
             .filter(Ticker.symbol == "AAPL")
             .order_by(CandleDataDay.date.desc())
             .limit(100))

    candles = pd.read_sql(query.statement, engine)

    # candles['date'] = candles['date'].apply(mpl_dates.date2num)
    # candles['date'] = pd.to_datetime(candles['date'])
    candles.set_index('date', inplace=True)
    candles['date'] = pd.to_datetime(candles.index)
    candles['date'] = candles['date'].apply(mpl_dates.date2num)
    # candles = candles.astype(float)
    candles = candles[['date', 'open', 'high', 'low', 'close', 'volume']]

    # Creating Subplots
    fig, ax = plt.subplots()

    candlestick_ohlc(ax, candles.values, width=0.6, colorup='green', colordown='red', alpha=0.8)

    # Setting labels & titles
    ax.set_xlabel('Date')
    ax.set_ylabel('Price')
    fig.suptitle('AAPL')

    # Formatting Date
    date_format = mpl_dates.DateFormatter('%d-%m-%Y')
    ax.xaxis.set_major_formatter(date_format)
    ax.set_yscale('log')
    fig.autofmt_xdate()

    fig.tight_layout()

    ax.grid()
    plt.show()

    session.close()


if __name__ == "__main__":
    main()
    # plot_candles()
