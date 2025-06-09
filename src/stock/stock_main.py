import os
import argparse
import asyncio
from time import time
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.orm import session as sess
# from common.tools.DataFrameTools import process
from src.stock.src.db.database import session_local
from src.stock.src.Queries import Queries
from src.stock.src.StockUpdater import StockUpdater

from src.common.telegram_manager.telegram_manager import TelegramBot
from src.common.file_manager.FileManager import FileManager

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


def refresh_materialized_views(session: sess.Session):
    # __ refresh materialized views __
    LOGGER.info("Refreshing materialized views...")
    session.execute(text("REFRESH MATERIALIZED VIEW mv_recent_candle_data_day;"))
    session.execute(text("REFRESH MATERIALIZED VIEW mv_last_info_trading_session;"))
    session.execute(text("REFRESH MATERIALIZED VIEW mv_last_info_general_stock;"))
    session.execute(text("REFRESH MATERIALIZED VIEW mv_ticker_overview;"))
    session.execute(text("REFRESH MATERIALIZED VIEW mv_pe;"))
    session.execute(text("REFRESH MATERIALIZED VIEW mv_next_earnings_per_ticker;"))
    session.commit()
    LOGGER.info("Materialized views refreshed.")

    # __ sqlAlchemy __ close the session
    session.close()


def main(process_name_: str = None, limit: int = 3000, add_sp500: bool = True, refresh_materialized: bool = True):
    # __ sqlAlchemy __ create new session
    session = session_local()

    # __ get tickers __
    tl = Queries(session, remove_yfinance_error_tickers=True)
    # symbols = tl.tickers_sp500_from_wikipedia()
    # symbols = tl.get_sp500_tickers()
    # symbols = tl.get_indexes_tickers()
    # symbols = tl.get_nasdaq_tickers()
    # symbols = tl.get_nyse_tickers()
    # symbols = tl.get_tickers_with_candles_not_updated_from_days(days=5)
    symbols = sorted(list(set(
        []
        + tl.get_tickers_not_updated_from_days(days=5)
        + tl.get_tickers_with_day_candles_not_updated_from_days(days=5)
        # + tl.get_sp500_tickers()
        # + tl.get_etf_tickers()
    )))

    if add_sp500 and len(symbols) < 2495 and 0 < datetime.now().weekday() < 6:  # 1-5 are Monday to Friday
        LOGGER.info("Adding S&P 500 tickers to the list...")
        symbols = sorted(list(set(
            symbols
            + tl.get_sp500_tickers()
            # + tl.get_etf_tickers()
        )))

    # __ remove tickers already present in DB __
    # symbols = tl.remove_tickers_present_in_db(tickers=symbols)

    # __ print the number of tickers __
    LOGGER.info(f"{'Total tickers:'.ljust(25)} {len(symbols)}")

    # __ limit the number of tickers __
    symbols = symbols[:limit]

    print(process_name_)

    stock_updater = StockUpdater(session=session)
    results = stock_updater.update_all_tickers(symbols=symbols)

    # __ refresh materialized views __
    if refresh_materialized:
        refresh_materialized_views(session=session)

    # __ sqlAlchemy __ close the session
    session.close()

    telegram_token_key = "TELEGRAM_TOKEN"
    config_manager = FileManager()
    token = config_manager.get_telegram_token(database_key=telegram_token_key)
    admin_info = config_manager.get_admin()
    telegram_bot = TelegramBot(token=token)

    """    {'total_time': total_time_str,
    'completed_symbols': completed_symbols,
    'failed_symbols': failed_symbols,
    'average_time_middle': average_time_middle,
    'average_time_end': average_time_end}"""

    text1 = f"Analysis completed for {len(symbols)} tickers"
    text2 = f"Completed symbols: {results['completed_symbols']}"
    text3 = f"Failed symbols: {', '.join(results['failed_symbols'])}"
    text4 = f"Total time taken: {results['total_time']}"
    text5 = f"Average time taken: {results['average_time_end']:.2f} seconds per ticker"

    text_ = f"{text1}\n{text2}\n{text3}\n{text4}\n{text5}"

    asyncio.run(telegram_bot.send_message(chat_id=admin_info["chat"], text=f"{text_}"))


def update_only_candles(process_name_: str = None):
    # __ sqlAlchemy __ create new session
    session = session_local()

    # __ get tickers __
    tl = Queries(session, remove_not_existing_db_tickers=True, remove_failed_candle_download_tickers=True)

    symbols = sorted(list(
        set([]
            # + tl.get_all_tickers()
            # + tl.get_tickers_not_updated_from_days(days=5)
            # + tl.get_tickers_with_day_candles_not_updated_from_days(days=5)
            # + tl.get_tickers_with_week_candles_not_updated_from_days(days=5)
            + tl.get_sp500_tickers()
            # + tl.get_nyse_tickers()
            # + tl.get_nasdaq_tickers()
            # + tl.get_indexes_tickers()
            # + tl.tickers_sp500_from_wikipedia()
            )
    ))

    # __ print the number of tickers __
    LOGGER.info(f"{'Total tickers:'.ljust(25)} {len(symbols)}")

    stock_updater = StockUpdater(session=session)
    stock_updater.update_only_candles_all_tickers(symbols=symbols)

    # __ refresh materialized views __
    LOGGER.info("Refreshing materialized views...")
    session.execute(text("REFRESH MATERIALIZED VIEW recent_candle_data_day_materialized;"))
    session.commit()
    LOGGER.info("Materialized views refreshed.")

    # __ sqlAlchemy __ close the session
    session.close()


if __name__ == '__main__':
    # Create a parser
    parser = argparse.ArgumentParser(description="Stock main script")

    # Add an optional argument
    parser.add_argument(
        '--process_name',  # Argument name
        type=str,  # Data type
        default=None,  # Default value (if not provided)
        help='Name of the calling process (optional)'  # Description
    )

    # Parse the arguments
    args = parser.parse_args()

    # Print the argument
    if args.process_name:
        process_name = args.process_name
        print(f"The name of the calling process is: {args.process_name}")
    else:
        process_name = 'manual'
        print("No process name provided.")

    archive_logs()
    if process_name == 'scheduled':
        main(process_name)
    else:
        main(process_name, limit=10, add_sp500=False, refresh_materialized=False)
        # update_only_candles(process_name)
    # plot_candles()
