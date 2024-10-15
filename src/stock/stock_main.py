import yfinance as yf
import pandas as pd
from time import time, sleep
from datetime import datetime, timedelta

from sqlalchemy.orm import session as sess
from sqlalchemy import text

from src.common.tools.library import seconds_to_time, safe_execute

from stock.src.TickerService import TickerService
from stock.src.database import session_local
from stock.src.CandleService import CandleDataInterval, CandleDataDay


def update_ticker(symbol: str, session: sess.Session):
    # __ start tracking the elapsed time __
    start_time = time()
    print(f"{symbol} - Start updating...")

    is_index = symbol.startswith("^")

    # __ get all the data for a sample ticker __
    stock = yf.Ticker(symbol)

    # __ update the database with new data __
    ticker_service = TickerService(session=session, symbol=symbol)

    # __ ticker __
    info = safe_execute(None, lambda: getattr(stock, "info"))

    ticker_service.handle_ticker(info=stock.info) if info is not None else None

    if not is_index:
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
        if not is_index:
            ticker_service.handle_sector_industry_history(info_data=stock.info)
        ticker_service.handle_info_target_price_and_recommendation(info_data=stock.info)
        ticker_service.handle_info_governance(info_data=stock.info)
        ticker_service.handle_info_cash_and_financial_ratios(info_data=stock.info)
        ticker_service.handle_info_market_and_financial_metrics(info_data=stock.info)

        isin = safe_execute(None, lambda: getattr(stock, "isin"))
        history_metadata = safe_execute(None, lambda: getattr(stock, "history_metadata"))
        if isin is not None and history_metadata is not None:
            ticker_service.handle_info_general_stock(isin=stock.isin, info_data=stock.info, history_metadata=stock.history_metadata)

    if not is_index:
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


def tickers_sp500_from_wikipedia(include_company_data=False):
    """Downloads list of tickers currently listed in the S&P 500"""

    # __ get list of all S&P 500 stocks __
    sp500 = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]
    # sp500["Symbol"] = sp500["Symbol"].str.replace(".", "-", regex=True)

    if include_company_data:
        return sp500

    sp_tickers = sp500.Symbol.tolist()
    sp_tickers = sorted(sp_tickers)

    return [x.replace(".", "-") for x in sp_tickers]


def get_symbols_from_db_not_updated_from_days(session: sess.Session, days: int = 5):
    # Calculate the date limit (5 days ago)
    date_limit = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')

    query = text(f"""
    WITH LatestUpdates AS (
        SELECT ticker_id, MAX(last_update) AS max_last_update
        FROM candle_data_day
        GROUP BY ticker_id
    )
    SELECT T.symbol, LU.max_last_update
    FROM ticker T
    JOIN LatestUpdates LU ON T.id = LU.ticker_id
    WHERE LU.max_last_update < '{date_limit}'
    ORDER BY T.symbol
    """)

    # Execute the query through the session
    result = session.execute(query).fetchall()
    symbols = [x[0] for x in result]
    return symbols


def get_all_symbols_from_db(session: sess.Session):
    query = text("""
    SELECT symbol
    FROM ticker
    ORDER BY symbol
    """)

    # Execute the query through the session
    result = session.execute(query).fetchall()
    symbols = [x[0] for x in result]
    return symbols


def get_all_not_updated_analysis_symbols_from_db(session: sess.Session):
    query = text(f"""
    SELECT DISTINCT T.symbol
    FROM candle_data_day C
    JOIN ticker T ON C.ticker_id = T.id
    WHERE NOT EXISTS (
        SELECT 1
        FROM candle_analysis_candlestick_day A
        WHERE C.id = A.candle_data_day_id
    )
    ORDER BY T.symbol
    """)

    # Execute the query through the session
    result = session.execute(query).fetchall()
    symbols = [x[0] for x in result]
    return symbols


def get_symbols_from_sp500_handler():
    from stock.src.sp500.sp500Handler import SP500Handler
    sp500_handler = SP500Handler()
    symbols = sp500_handler.get_delisted_sp500_components()
    return symbols


def get_indexes_symbols():
    """
    Ticker	    Description	                            Maintained By
    ^GSPC	    S&P 500 Index (Market Cap Weighted)	    S&P Dow Jones Indices
    ^SP500EW	S&P 500 Equal Weight Index	            S&P Dow Jones Indices
    ^SP500G	    S&P 500 Growth Index	                S&P Dow Jones Indices
    ^SP500V	    S&P 500 Value Index	                    S&P Dow Jones Indices
    ^SPDAUDP	S&P 500 Dividend Aristocrats	        S&P Dow Jones Indices
    ^SP500LVOL	S&P 500 Low Volatility Index	        S&P Dow Jones Indices
    ^SPXESUP	S&P 500 ESG Index	                    S&P Dow Jones Indices
    ^SP500PG	S&P 500 Pure Growth Index	            S&P Dow Jones Indices
    ^SP500PV	S&P 500 Pure Value Index	            S&P Dow Jones Indices
    ^SPX50	    S&P 500 Top 50 Index	                Likely a custom index; check provider
    ^SPXHDUP	S&P 500 High Dividend Index	            Likely a custom index; check provider
    ^SP500-20	S&P 500 Industrials Index	            S&P Dow Jones Indices
    ^SP500-45	S&P 500 Information Technology Index	S&P Dow Jones Indices
    ^SP500-35	S&P 500 Health Care Index	            S&P Dow Jones Indices
    ^SP500-40	S&P 500 Financials Index	            S&P Dow Jones Indices
    """

    return [
        "^GSPC", "^SP500EW", "^SP500G", "^SP500V", "^SPDAUDP", "^SPXHDUP", "^SP500LVOL",
        "^SPXESUP", "^SP500PG", "^SP500PV", "^SPX50", "^SP500-20", "^SP500-45", "^SP500-40",
        "^SP500-35", "^DJI", "^IXIC", "^RUT", "^FTSE", "^GDAXI", "^FCHI", "^N225", "^HSI",
        "000001.SS", "^STOXX50E", "^BVSP", "^GSPTSE", "^AXJO", "^KS11", "^SP400", "^SP600",
        "^MSCIW", "^NYA", "^XAX", "^XAR", "^IBEX", "^MDAXI", "^SSMI", "^TA35", "^VIX",
        "^MXX", "^BSESN", "^NSEI", "^AEX", "FTSEMIB.MI", "^NDX", "^W5000", "^OEX", "^DJT",
        "^DJU"
    ]


def main():
    # __ sqlAlchemy __ create new session
    session = session_local()

    # __ get tickers __
    # symbols = tickers_sp500_from_wikipedia()
    # symbols = get_symbols_from_sp500_handler()
    # symbols = get_indexes_symbols()
    symbols = get_symbols_from_db_not_updated_from_days(session=session, days=3)

    print(f"{'Total tickers:'.ljust(25)} {len(symbols)}")

    start_time = time()

    # __ update all tickers __
    for symbol in symbols:
        try:
            update_ticker(symbol=symbol, session=session)
        except Exception as e:
            print(f"{symbol} - Error: {e}")
            sleep(1)

    end_time = time()
    total_time = seconds_to_time(end_time - start_time)
    print(f"{'Total elapsed time:'.ljust(25)} {total_time['hours']} hours {total_time['minutes']} min {total_time['seconds']} sec")
    print(f"{'Total tickers:'.ljust(25)} {len(symbols)}")
    print(f"{'Average time per ticker:'.ljust(25)} {round((end_time - start_time) / len(symbols), 3)} sec")

    # __ sqlAlchemy __ close the session
    session.close()


def plot_candles():
    import pandas as pd
    from stock.src.database import engine, session_local
    from stock.src.TickerService import Ticker
    from mpl_finance import candlestick_ohlc
    import matplotlib.dates as mpl_dates
    from src.common.tools.library import plt

    session = session_local()

    query = (session.query(CandleDataDay)
             .join(Ticker)
             .filter(Ticker.symbol == "^SP500EW")
             .order_by(CandleDataDay.date.desc())
             .limit(1500))

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
