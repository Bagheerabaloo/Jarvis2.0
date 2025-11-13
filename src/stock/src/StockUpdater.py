import yfinance as yf
import numpy as np
import pandas as pd
from time import time, sleep
from typing import Optional
from datetime import datetime, timedelta

from numpy.array_api import floor
from sqlalchemy.orm import session as sess
from sqlalchemy import text

from src.common.tools.library import seconds_to_time, safe_execute
from src.stock.src.CandleBulkService import CandleBulkService
from src.stock.src.TickerUpdater import TickerUpdater
from src.stock.src.TickerService import TickerService
from src.stock.src.db.database import session_local
from src.stock.src.CandleService import CandleDataInterval, CandleDataDay
from src.stock.src.Queries import Queries


from logger_setup import LOGGER, error_handler
import logging
from src.stock.src.RaiseOnErrorHandler import RaiseOnErrorHandler


class StockUpdater:
    def __init__(self, session: sess.Session, symbols_with_errors: Optional[list[str]] = None):
        self.session = session
        self.symbols_with_errors = symbols_with_errors

    def execute_function(self,
                         default,
                         function,
                         *args,
                         label: str = '',
                         check_yf: bool = True) -> (Optional[pd.DataFrame], bool, str, str):
        """ Executes a function with error handling and logging.
        Args:
            default: The default value to return in case of an error.
            function: The function to execute.
            *args: Arguments to pass to the function.
            label (str): Label for logging purposes.
            check_yf (bool): Whether to check for yfinance exceptions.
        Returns:
            Optional[pd.DataFrame]: The result of the function execution, or the default value in case of an error.
            bool: True if the function executed successfully, False otherwise.
            str: The status of the ticker.
            str: The error message if an error occurred, otherwise an empty string.
        Raises:
            RuntimeError: If an error occurs during the function execution.

        This method executes a function with the provided arguments and handles any exceptions that may occur.
        It logs the error and returns a default value if an error occurs. If the function returns an empty DataFrame,
        it checks for yfinance exceptions and handles them accordingly. If the symbol is delisted or not found, it updates the ticker status.
        If the function executes successfully, it returns the result of the function execution.
        If an error occurs, it logs the error and returns the default value.
        """

        try:
            x =  function(*args)
        except RuntimeError as e:
            LOGGER.error(f"{e}")
            x = default
        except Exception as e:
            LOGGER.error(f"{e}")
            x = default

        if check_yf and isinstance(x, pd.DataFrame) and x.empty:
            LOGGER.warning(f"{self.symbol} - Empty DataFrame returned from yfinance for {label}. Sleeping for 2.5 seconds to check for exceptions.")
            sleep(2.5)  # Sleep to wait for the exception to be raised in the ErrorHandler
            try:
                error_handler.check_for_exception()  # Check and raise exception in the main thread
            except RuntimeError as e:
                lines = str(e).split('\n')
                sublines = lines[1].split('\n') if len(lines) > 1 else []

                if "yfinance ERROR:" in lines[0]:
                    if "possibly delisted" in lines[1] or "may be delisted" in lines[1]:
                        # LOGGER.warning(f"{self.symbol} - Ticker possibly delisted or not found.")
                        # self.execute_function(None, lambda: self.ticker_service.handle_ticker_status(
                        #     status="Possibly Delisted", error=sublines[0]), check_yf=False)
                        return x, False, "Possibly Delisted", sublines[0]
                    else:
                        LOGGER.warning(f"{self.symbol} - Case not handled: {lines[0]} - {sublines[0]}")
                        return x, False, lines[0], sublines[0]

        return x, True, "", ""

    def update_all_tickers(self, symbols: list[str]) -> dict:
        num_symbols = len(symbols)

        # __ start tracking the elapsed time __
        start_time = time()

        middle_time_secs = 0
        end_time_secs = 0
        completed_symbols = 0

        failed_symbols = []

        # __ update all tickers __
        for index, symbol in enumerate(symbols):
            try:
                # check if the symbol is in the list of yfinance errors
                if len(self.symbols_with_errors or []) > 0 and symbol in self.symbols_with_errors:
                    has_yf_errors = True
                else:
                    has_yf_errors = False
                ticker_updater = TickerUpdater(session=self.session, symbol=symbol, has_yf_errors=has_yf_errors)
                LOGGER.info(f"{symbol.rjust(5)} - {index+1}/{num_symbols} - Start updating...")
                middle, end = ticker_updater.update_ticker()
                middle_time_secs += middle
                end_time_secs += end
                completed_symbols += 1
            except RuntimeError as e:
                LOGGER.error(f"{e}")
                failed_symbols.append(symbol)
            except Exception as e:
                LOGGER.warning(f"{symbol} - Error: {e}")
                failed_symbols.append(symbol)
                sleep(1)

        # __ stop tracking the elapsed time and print the stats __
        end_time = time()
        total_time = seconds_to_time(end_time - start_time)
        average_time_middle = round(middle_time_secs / completed_symbols, 3) if completed_symbols > 0 else 0
        average_time_end = round(end_time_secs / completed_symbols, 3) if completed_symbols > 0 else 0
        total_time_str = f"{total_time['hours']} hours {total_time['minutes']} min {total_time['seconds']} sec"

        LOGGER.info(f"{'Total elapsed time:'.ljust(25)} {total_time_str}")
        LOGGER.info(f"{'Total tickers:'.ljust(25)} {len(symbols)}")
        LOGGER.info(f"{'Completed symbols:'.ljust(25)} {completed_symbols}")
        LOGGER.info(f"{'Failed symbols:'.ljust(25)} {len(symbols) - completed_symbols}")
        LOGGER.info(f"{'Average time per ticker:'.ljust(25)} {round((end_time - start_time) / len(symbols), 3)} sec")

        LOGGER.info(f"{'Average time per ticker (middle time):'.ljust(25)} {average_time_middle} sec")
        LOGGER.info(f"{'Average time per ticker (end time):'.ljust(25)} {average_time_end} sec")

        return {'total_time': total_time_str,
                'completed_symbols': completed_symbols,
                'failed_symbols': failed_symbols,
                'average_time_middle': average_time_middle,
                'average_time_end': average_time_end}

    def update_only_candles_all_tickers(self, symbols: list[str]):
        # __ start tracking the elapsed time __
        start_time = time()

        def split_into_batches(tickers, batch_size=500):
            num_batches = -(-len(tickers) // batch_size)  # Equivalent to math.ceil(len(tickers) / batch_size)
            return [list(batch) for batch in np.array_split(tickers, num_batches)]

        num_of_batches = len(symbols) / 100
        if num_of_batches - np.floor(num_of_batches) < 0.5:
            batch_size = np.ceil(len(symbols) / np.floor(num_of_batches))
        else:
            batch_size = 100

        batches = split_into_batches(symbols, batch_size=batch_size)

        for batch in batches:
            candle_bulk_service = CandleBulkService(session=self.session, symbols=batch, commit_enable=True)
            candle_bulk_service.update_all_tickers_candles()

        end_time = time()
        total_time = seconds_to_time(end_time - start_time)
        LOGGER.info(f"{'Total elapsed time:'.ljust(25)} {total_time['hours']} hours {total_time['minutes']} min {total_time['seconds']} sec")
        LOGGER.info(f"{'Total tickers:'.ljust(25)} {len(symbols)}")
        LOGGER.info(f"{'Average time per ticker:'.ljust(25)} {round((end_time - start_time) / len(symbols), 3)} sec")