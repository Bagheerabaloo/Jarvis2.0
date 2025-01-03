import os
from time import time
from src.stock.src.database import session_local
from src.stock.src.TickerLister import TickerLister
from src.stock.src.StockUpdater import StockUpdater

from logger_setup import LOGGER


def archive_logs():
    log_files = os.listdir(r"C:\Users\Vale\PycharmProjects\Jarvis2.0\logs") # TODO: change to relative path
    # __ get files that are older than one week __
    older_logs = [f for f in log_files if os.path.getmtime(f"C:/Users/Vale/PycharmProjects/Jarvis2.0/logs/{f}") < time() - 7 * 24 * 3600]
    # __ make backup dir if it doesn't exist __
    if not os.path.exists(r"C:\Users\Vale\PycharmProjects\Jarvis2.0\logs\logs_backup"):
        os.makedirs(r"C:\Users\Vale\PycharmProjects\Jarvis2.0\logs\logs_backup")
    # __ move files to back-up dir __
    for f in older_logs:
        os.rename(f"C:/Users/Vale/PycharmProjects/Jarvis2.0/logs/{f}", f"C:/Users/Vale/PycharmProjects/Jarvis2.0/logs/logs_backup/{f}")

    print('end')


def main():
    # __ sqlAlchemy __ create new session
    session = session_local()

    # __ get tickers __
    tl = TickerLister(session)
    # symbols = tl.tickers_sp500_from_wikipedia()
    # symbols = tl.get_sp500_tickers()
    # symbols = tl.get_indexes_tickers()
    # symbols = tl.get_nasdaq_tickers()
    # symbols = tl.get_nyse_tickers()
    # symbols = tl.get_tickers_with_candles_not_updated_from_days(days=5)
    symbols = list(set(tl.get_tickers_not_updated_from_days(days=5) + tl.get_tickers_with_candles_not_updated_from_days(days=5)))

    # __ remove tickers already present in DB __
    # symbols = tl.remove_tickers_present_in_db(tickers=symbols)

    # __ print the number of tickers __
    LOGGER.info(f"{'Total tickers:'.ljust(25)} {len(symbols)}")

    # __ limit the number of tickers __
    symbols = symbols[:3000]
    # symbols = ["BSGM"]

    stock_updater = StockUpdater(session=session)
    stock_updater.update_all_tickers(symbols=symbols)

    # __ sqlAlchemy __ close the session
    session.close()



# def plot_candles():
#     import pandas as pd
#     from src.stock.src.database import engine, session_local
#     from src.stock.src.TickerService import Ticker
#     from mpl_finance import candlestick_ohlc
#     import matplotlib.dates as mpl_dates
#     from src.common.tools.library import plt
#
#     session = session_local()
#
#     query = (session.query(CandleDataDay)
#              .join(Ticker)
#              .filter(Ticker.symbol == "^SP500EW")
#              .order_by(CandleDataDay.date.desc())
#              .limit(1500))
#
#     candles = pd.read_sql(query.statement, engine)
#
#     # candles['date'] = candles['date'].apply(mpl_dates.date2num)
#     # candles['date'] = pd.to_datetime(candles['date'])
#     candles.set_index('date', inplace=True)
#     candles['date'] = pd.to_datetime(candles.index)
#     candles['date'] = candles['date'].apply(mpl_dates.date2num)
#     # candles = candles.astype(float)
#     candles = candles[['date', 'open', 'high', 'low', 'close', 'volume']]
#
#     # Creating Subplots
#     fig, ax = plt.subplots()
#
#     candlestick_ohlc(ax, candles.values, width=0.6, colorup='green', colordown='red', alpha=0.8)
#
#     # Setting labels & titles
#     ax.set_xlabel('Date')
#     ax.set_ylabel('Price')
#     fig.suptitle('AAPL')
#
#     # Formatting Date
#     date_format = mpl_dates.DateFormatter('%d-%m-%Y')
#     ax.xaxis.set_major_formatter(date_format)
#     ax.set_yscale('log')
#     fig.autofmt_xdate()
#
#     fig.tight_layout()
#
#     ax.grid()
#     plt.show()
#
#     session.close()


if __name__ == '__main__':
    archive_logs()
    main()
    # plot_candles()
