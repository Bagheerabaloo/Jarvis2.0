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
from src.stock.src.db.models import *

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
    session.execute(text("REFRESH MATERIALIZED VIEW mv_monthly_net_insider_transactions;"))
    session.commit()
    LOGGER.info("Materialized views refreshed.")

    # __ sqlAlchemy __ close the session
    session.close()


def bronze_to_silver(session: sess.Session):
    """
    Upsert from bronze `earnings_history` to silver `slv_earnings_history`,
    deriving `quarter` ('Q1'..'Q4') and `year` from `quarter_date`.

    - Primary key on silver: (ticker_id, quarter_date, last_update)
    - On conflict, updates all non-key columns.

    Args:
        session: SQLAlchemy Session (transaction managed by caller).
        since_last_update: optional filter to process only bronze rows with last_update >= this date.
        ticker_ids: optional iterable of ticker IDs to restrict the upsert.

    Returns:
        Number of rows inserted/updated (as reported by DB).
    """
    import sqlalchemy as sa

    RAW_SQL_BRONZE_TO_SILVER = """
        INSERT INTO slv_earnings_history
        (ticker_id, quarter_date, last_update, quarter, year, eps_actual, eps_estimate, eps_difference, surprise_percent)
        SELECT e.ticker_id,
          e.quarter_date,
          e.last_update,
          'Q' || to_char(e.quarter_date, 'Q') AS quarter,
          EXTRACT(YEAR FROM e.quarter_date) ::int AS year,
        e.eps_actual,
        e.eps_estimate,
        e.eps_difference,
        e.surprise_percent
        FROM earnings_history e
        ON CONFLICT (ticker_id, quarter_date, last_update) DO UPDATE  
        SET 
            quarter = EXCLUDED.quarter,
            year = EXCLUDED.year,
            eps_actual = EXCLUDED.eps_actual,
            eps_estimate = EXCLUDED.eps_estimate,
            eps_difference = EXCLUDED.eps_difference,
            surprise_percent = EXCLUDED.surprise_percent
        """
    sql = RAW_SQL_BRONZE_TO_SILVER
    res = session.execute(sa.text(sql))
    return res.rowcount or 0


def set_up_telegram_bot():
    telegram_token_key = "TELEGRAM_TOKEN"
    config_manager = FileManager()
    token = config_manager.get_telegram_token(database_key=telegram_token_key)
    admin_info = config_manager.get_admin()
    telegram_bot = TelegramBot(token=token)
    return admin_info, telegram_bot


def select_tickers(session: sess.Session,
                   limit: int = 3000,
                   add_sp500: bool = True,
                   only_sp500: bool = False,
                   only_yf_error: bool = False,
                   ):
    if only_yf_error:  # return only yfinance error tickers
        return select_only_yfinance_error_tickers(session=session, limit=limit)

    if only_sp500:  # get only sp500 tickers
        return select_only_sp500_tickers(session=session)

    # __ get tickers __
    is_weekday = 0 < datetime.now().weekday() < 6
    remove_yfinance_error_tickers = True if is_weekday else False
    queries = Queries(session, remove_yfinance_error_tickers=remove_yfinance_error_tickers)

    # symbols = tl.tickers_sp500_from_wikipedia()
    # symbols = tl.get_indexes_tickers()
    # symbols = tl.get_nasdaq_tickers()
    # symbols = tl.get_nyse_tickers()
    # symbols = tl.get_tickers_with_candles_not_updated_from_days(days=5)
    # etf_symbols = tl.get_etf_tickers()

    sp500_symbols = queries.get_sp500_tickers()
    symbols = sorted(list(set(
        []
        + queries.get_tickers_not_updated_from_days(days=5)
        + queries.get_tickers_with_day_candles_not_updated_from_days(days=5)
    )))

    if add_sp500 and len(symbols) < 2495 and is_weekday:  # 1-5 are Monday to Friday
        LOGGER.info("Adding S&P 500 tickers to the list...")
        symbols = sorted(list(set(
            symbols
            + sp500_symbols
        )))

    # __ remove tickers already present in DB __
    # symbols = tl.remove_tickers_present_in_db(tickers=symbols)

    LOGGER.info(f"{'Total tickers before limit:'.ljust(25)} {len(symbols)}")
    symbols = symbols[:limit]
    LOGGER.info(f"{'Tickers after limit:'.ljust(25)} {len(symbols)}")

    return symbols


def select_only_yfinance_error_tickers(session: sess.Session, limit: int = 3000):
    queries = Queries(session)
    symbols = queries.get_yfinance_error_tickers()
    LOGGER.info(f"{'Total tickers before limit:'.ljust(25)} {len(symbols)}")
    symbols = symbols[:limit]
    LOGGER.info(f"{'Tickers after limit:'.ljust(25)} {len(symbols)}")
    return symbols

def select_only_sp500_tickers(session: sess.Session, limit: int = 3000):
    queries = Queries(session, remove_yfinance_error_tickers=False)
    symbols = queries.get_sp500_tickers()
    LOGGER.info(f"{'Total tickers before limit:'.ljust(25)} {len(symbols)}")
    symbols = symbols[:limit]
    LOGGER.info(f"{'Tickers after limit:'.ljust(25)} {len(symbols)}")
    return symbols


def main(process_name_: str = None,
         limit: int = 3000,
         add_sp500: bool = True,
         only_sp500: bool = False,
         only_yf_error: bool = False,
         refresh_materialized: bool = True):

    LOGGER.info(process_name_)

    # __ sqlAlchemy __ create new session
    session = session_local()

    # __ set up telegram bot __
    admin_info, telegram_bot = set_up_telegram_bot()

    # __ get tickers __
    symbols = select_tickers(session=session, limit=limit, add_sp500=add_sp500, only_sp500=only_sp500, only_yf_error=only_yf_error)

    # __ get list of invalid symbols __
    queries = Queries(session)
    symbols_with_errors = queries.get_yfinance_error_tickers()

    # symbols=['TSM']

    stock_updater = StockUpdater(session=session, symbols_with_errors=symbols_with_errors)
    results = stock_updater.update_all_tickers(symbols=symbols)

    # __ refresh materialized views __
    if refresh_materialized:
        refresh_materialized_views(session=session)

    # __ sqlAlchemy __ close the session
    session.close()

    text1 = f"Analysis completed for {len(symbols)} tickers"
    text2 = f"Completed symbols: {results['completed_symbols']}"
    text3 = f"Failed symbols: {', '.join(results['failed_symbols'])}"
    text4 = f"Total time taken: {results['total_time']}"
    text5 = f"Average time taken (middle): {results['average_time_middle']:.2f} seconds per ticker"
    text6 = f"Average time taken: {results['average_time_end']:.2f} seconds per ticker"

    text_ = f"{text1}\n{text2}\n{text3}\n{text4}\n{text5}\n{text6}"

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
        main(process_name, limit=1000, only_sp500=True, add_sp500=True, only_yf_error=True, refresh_materialized=True)
        # update_only_candles(process_name)
    # plot_candles()
