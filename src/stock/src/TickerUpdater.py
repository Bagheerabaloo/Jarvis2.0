import yfinance as yf
import numpy as np
import pandas as pd
import re
from time import time, sleep
from typing import Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from functools import partial
from numpy.array_api import floor
from sqlalchemy.orm import session as sess
from sqlalchemy import text
from sqlalchemy.util import symbol

from src.common.tools.library import seconds_to_time, safe_execute
from src.stock.src.TickerService import TickerService
from src.stock.src.db.database import session_local
from src.stock.src.CandleService import CandleDataInterval, CandleDataDay
from src.stock.src.Queries import Queries
from src.stock.src.CandleBulkService import CandleBulkService

from logger_setup import LOGGER, error_handler
import logging
from src.stock.src.RaiseOnErrorHandler import RaiseOnErrorHandler
from enum import Enum


class TickerUpdaterStatus(Enum):
    INFO_GETATTR = "info_getattr"

    # IS NOT INDEX
    ACTIONS_GETATTR = "actions_getattr"
    EARNINGS_DATES_GETATTR = "earnings_dates_getattr"
    CALENDAR_GETATTR = "calendar_getattr"

    BALANCE_SHEET_ANNUAL = "balance_sheet_annual"
    BALANCE_SHEET_QUARTERLY = "balance_sheet_quarterly"
    CASH_FLOW_ANNUAL = "cash_flow_annual"
    CASH_FLOW_QUARTERLY = "cash_flow_quarterly"
    FINANCIALS_ANNUAL = "financials_annual"
    FINANCIALS_QUARTERLY = "financials_quarterly"

    ACTIONS = "actions"
    EARNINGS_DATES = "earnings_dates"
    CALENDAR = "calendar"

    # INFO NOT NONE
    ISIN_GETATTR = "isin_getattr"
    HISTORY_METADATA_GETATTR = "history_metadata_getattr"

    INFO_COMPANY_ADDRESS = "info_company_address"
    SECTOR_INDUSTRY_HISTORY = "sector_industry_history"
    INFO_TARGET_PRICE_AND_RECOMMENDATION = "info_target_price_and_recommendation"
    INFO_GOVERNANCE = "info_governance"
    INFO_CASH_AND_FINANCIAL_RATIOS = "info_cash_and_financial_ratios"
    INFO_MARKET_AND_FINANCIAL_METRICS = "info_market_and_financial_metrics"

    INFO_GENERAL_STOCK = "info_general_stock"

    # NOT INDEX
    INSIDER_PURCHASES_GETATTR = "insider_purchases_getattr"
    INSIDER_ROSTER_HOLDERS_GETATTR = "insider_roster_holders_getattr"
    INSIDER_TRANSACTIONS_GETATTR = "insider_transactions_getattr"
    INSTITUTIONAL_HOLDERS_GETATTR = "institutional_holders_getattr"
    MAJOR_HOLDERS_GETATTR = "major_holders_getattr"
    MUTUAL_FUND_HOLDERS_GETATTR = "mutual_fund_holders_getattr"
    RECOMMENDATIONS_GETATTR = "recommendations_getattr"
    UPGRADES_DOWNGRADES_GETATTR = "upgrades_downgrades_getattr"

    INSIDER_PURCHASES = "insider_purchases"
    INSIDER_ROSTER_HOLDERS = "insider_roster_holders"
    INSIDER_TRANSACTIONS = "insider_transactions"
    INSTITUTIONAL_HOLDERS = "institutional_holders"
    MAJOR_HOLDERS = "major_holders"
    MUTUAL_FUND_HOLDERS = "mutual_fund_holders"
    RECOMMENDATIONS = "recommendations"
    UPGRADES_DOWNGRADES = "upgrades_downgrades"

    # INFO NOT NONE
    FAST_INFO_GETATTR = "fast_info_getattr"
    INFO_TRADING_SESSION = "info_trading_session"

    CANDLE_MONTH = "candle_month"
    CANDLE_WEEK = "candle_week"
    CANDLE_DAY = "candle_day"
    CANDLE_HOUR = "candle_hour"
    CANDLE_MINUTE_5 = "candle_minute_5"
    CANDLE_MINUTE_1 = "candle_minute_1"


@dataclass
class TickerUpdater:
    def __init__(self, session: sess.Session, symbol: str):
        self.session = session
        self.symbol = symbol
        self.ticker_service = None
        self.function_map = {}
        self.errors = []
        self._set_is_index()

    def _set_is_index(self):
        self.is_index = self.symbol.startswith("^")

    def update_ticker(self):
        # __ start tracking the elapsed time __
        start_time = time()

        # __ get all the data for a sample ticker from yahoo finance API __
        stock = yf.Ticker(self.symbol)

        # __ update the database with new data __
        ticker_service = TickerService(session=self.session, symbol=self.symbol, commit_enable=True)
        ticker_service.set_stock(stock=stock)
        self.ticker_service = ticker_service
        self.set_mapping()  # Set the mapping of ticker update statuses to functions

        # __ ticker __
        info, bool_ = self.map_and_execute_function(TickerUpdaterStatus.INFO_GETATTR)

        status = self.errors[-1]["status"] if not bool_ else None
        error = self.errors[-1]["error"] if not bool_ else None
        self.ticker_service.set_info(info=info)

        # __ handle ticker update/insert __
        success_ticker = ticker_service.handle_ticker(info=info, error=error, status=status)
        if not success_ticker:
            self.execute_function(None, ticker_service.final_update_ticker)
            LOGGER.warning(f"{self.symbol} - Ticker not updated - sleeping 30 seconds")
            sleep(30)
            return False

        if not self.is_index:
            self.map_and_execute_function(TickerUpdaterStatus.BALANCE_SHEET_ANNUAL)
            self.map_and_execute_function(TickerUpdaterStatus.BALANCE_SHEET_QUARTERLY)
            self.map_and_execute_function(TickerUpdaterStatus.CASH_FLOW_ANNUAL)
            self.map_and_execute_function(TickerUpdaterStatus.CASH_FLOW_QUARTERLY)
            self.map_and_execute_function(TickerUpdaterStatus.FINANCIALS_ANNUAL)
            self.map_and_execute_function(TickerUpdaterStatus.FINANCIALS_QUARTERLY)
            self.map_and_execute_function(TickerUpdaterStatus.ACTIONS)
            self.map_and_execute_function(TickerUpdaterStatus.CALENDAR)
            self.map_and_execute_function(TickerUpdaterStatus.EARNINGS_DATES)
            self.map_and_execute_function(TickerUpdaterStatus.INSIDER_PURCHASES)
            self.map_and_execute_function(TickerUpdaterStatus.INSIDER_ROSTER_HOLDERS)
            self.map_and_execute_function(TickerUpdaterStatus.INSIDER_TRANSACTIONS)
            self.map_and_execute_function(TickerUpdaterStatus.INSTITUTIONAL_HOLDERS)
            self.map_and_execute_function(TickerUpdaterStatus.MAJOR_HOLDERS)
            self.map_and_execute_function(TickerUpdaterStatus.MUTUAL_FUND_HOLDERS)
            self.map_and_execute_function(TickerUpdaterStatus.RECOMMENDATIONS)
            self.map_and_execute_function(TickerUpdaterStatus.UPGRADES_DOWNGRADES)

        if info is not None:
            self.map_and_execute_function(TickerUpdaterStatus.INFO_COMPANY_ADDRESS)
            self.map_and_execute_function(TickerUpdaterStatus.INFO_TARGET_PRICE_AND_RECOMMENDATION)
            self.map_and_execute_function(TickerUpdaterStatus.INFO_GOVERNANCE)
            self.map_and_execute_function(TickerUpdaterStatus.INFO_CASH_AND_FINANCIAL_RATIOS)
            self.map_and_execute_function(TickerUpdaterStatus.INFO_MARKET_AND_FINANCIAL_METRICS)
            self.map_and_execute_function(TickerUpdaterStatus.INFO_GENERAL_STOCK)
            self.map_and_execute_function(TickerUpdaterStatus.INFO_TRADING_SESSION)
            if not self.is_index:
                self.map_and_execute_function(TickerUpdaterStatus.SECTOR_INDUSTRY_HISTORY)

        before_candle_time = time()

        # __ handle candle data update/insert __
        self.map_and_execute_function(TickerUpdaterStatus.CANDLE_MONTH)
        self.map_and_execute_function(TickerUpdaterStatus.CANDLE_WEEK)
        self.map_and_execute_function(TickerUpdaterStatus.CANDLE_DAY)
        self.map_and_execute_function(TickerUpdaterStatus.CANDLE_HOUR)
        self.map_and_execute_function(TickerUpdaterStatus.CANDLE_MINUTE_5)
        self.map_and_execute_function(TickerUpdaterStatus.CANDLE_MINUTE_1)

        # intervals = list(CandleDataInterval)
        # for interval in intervals:
        #     self.execute_function(None, ticker_service.handle_candle_data, interval=interval)

        if len(self.errors) > 0:
            yf_errors = self.check_yfinance_exceptions()

        self.execute_function(None, ticker_service.final_update_ticker)

        # __ stop tracking the elapsed time and print the difference __
        end_time = time()
        before_candle_time_secs = before_candle_time - start_time
        total_time_secs = end_time - start_time
        total_time = seconds_to_time(total_time_secs)
        LOGGER.info(f"{self.symbol} - Total time: {total_time['minutes']} min {total_time['seconds']} sec\n")
        return before_candle_time_secs, total_time_secs

    def set_mapping(self):
        """
        self.execute_function(None, ticker_service.final_update_ticker)

        self.execute_function(None, ticker_service.handle_balance_sheet, period_type="annual")
        self.execute_function(None, ticker_service.handle_balance_sheet, period_type="quarterly")
        self.execute_function(None, ticker_service.handle_cash_flow, period_type="annual")
        self.execute_function(None, ticker_service.handle_cash_flow, period_type="quarterly")
        self.execute_function(None, ticker_service.handle_financials, period_type="annual")
        self.execute_function(None, ticker_service.handle_financials, period_type="quarterly")
        self.execute_function(None, ticker_service.handle_actions)
        self.execute_function(None, ticker_service.handle_calendar)
        self.execute_function(None, ticker_service.handle_earnings_dates)
        self.execute_function(None, ticker_service.handle_insider_purchases)
        self.execute_function(None, ticker_service.handle_insider_roster_holders)
        self.execute_function(None, ticker_service.handle_insider_transactions)
        self.execute_function(None, ticker_service.handle_institutional_holders)
        self.execute_function(None, ticker_service.handle_major_holders)
        self.execute_function(None, ticker_service.handle_mutual_fund_holders)
        self.execute_function(None, ticker_service.handle_recommendations)
        self.execute_function(None, ticker_service.handle_upgrades_downgrades)

        self.execute_function(None, ticker_service.handle_info_company_address, info_data=info)
        self.execute_function(None, ticker_service.handle_info_target_price_and_recommendation, info_data=info)
        self.execute_function(None, ticker_service.handle_info_governance, info_data=info)
        self.execute_function(None, ticker_service.handle_info_cash_and_financial_ratios, info_data=info)
        self.execute_function(None, ticker_service.handle_info_market_and_financial_metrics, info_data=info)
        self.execute_function(None, ticker_service.handle_info_general_stock, info_data=info)
        self.execute_function(None, ticker_service.handle_sector_industry_history, info_data=info)
        self.execute_function(None,
                                      ticker_service.handle_info_trading_session,
                                      info=stock.info,
                                      basic_info=stock.fast_info,
                                      history_metadata=stock.history_metadata)

        self.execute_function(None,
                                  ticker_service.handle_candle_data,
                                  interval=interval)

        self.execute_function(None, ticker_service.final_update_ticker)
        """
        self.function_map = {
            TickerUpdaterStatus.INFO_GETATTR: partial(self.execute_function, None, getattr, self.ticker_service.stock, "info"),
            TickerUpdaterStatus.BALANCE_SHEET_ANNUAL: partial(self.execute_function, None, self.ticker_service.handle_balance_sheet, period_type="annual"),
            TickerUpdaterStatus.BALANCE_SHEET_QUARTERLY: partial(self.execute_function, None, self.ticker_service.handle_balance_sheet, period_type="quarterly"),
            TickerUpdaterStatus.CASH_FLOW_ANNUAL: partial(self.execute_function, None, self.ticker_service.handle_cash_flow, period_type="annual"),
            TickerUpdaterStatus.CASH_FLOW_QUARTERLY: partial(self.execute_function, None, self.ticker_service.handle_cash_flow, period_type="quarterly"),
            TickerUpdaterStatus.FINANCIALS_ANNUAL: partial(self.execute_function, None, self.ticker_service.handle_financials, period_type="annual"),
            TickerUpdaterStatus.FINANCIALS_QUARTERLY: partial(self.execute_function, None, self.ticker_service.handle_financials, period_type="quarterly"),
            TickerUpdaterStatus.ACTIONS: partial(self.execute_function, None, self.ticker_service.handle_actions),
            TickerUpdaterStatus.EARNINGS_DATES: partial(self.execute_function, None, self.ticker_service.handle_earnings_dates),
            TickerUpdaterStatus.CALENDAR: partial(self.execute_function, None, self.ticker_service.handle_calendar),
            TickerUpdaterStatus.INSIDER_PURCHASES: partial(self.execute_function, None, self.ticker_service.handle_insider_purchases),
            TickerUpdaterStatus.INSIDER_ROSTER_HOLDERS: partial(self.execute_function, None, self.ticker_service.handle_insider_roster_holders),
            TickerUpdaterStatus.INSIDER_TRANSACTIONS: partial(self.execute_function, None, self.ticker_service.handle_insider_transactions),
            TickerUpdaterStatus.INSTITUTIONAL_HOLDERS: partial(self.execute_function, None, self.ticker_service.handle_institutional_holders),
            TickerUpdaterStatus.MAJOR_HOLDERS: partial(self.execute_function, None, self.ticker_service.handle_major_holders),
            TickerUpdaterStatus.MUTUAL_FUND_HOLDERS: partial(self.execute_function, None, self.ticker_service.handle_mutual_fund_holders),
            TickerUpdaterStatus.RECOMMENDATIONS: partial(self.execute_function, None, self.ticker_service.handle_recommendations),
            TickerUpdaterStatus.UPGRADES_DOWNGRADES: partial(self.execute_function, None, self.ticker_service.handle_upgrades_downgrades),
            TickerUpdaterStatus.INFO_COMPANY_ADDRESS: partial(self.execute_function, None, self.ticker_service.handle_info_company_address),
            TickerUpdaterStatus.SECTOR_INDUSTRY_HISTORY: partial(self.execute_function, None, self.ticker_service.handle_sector_industry_history),
            TickerUpdaterStatus.INFO_TARGET_PRICE_AND_RECOMMENDATION: partial(self.execute_function, None, self.ticker_service.handle_info_target_price_and_recommendation),
            TickerUpdaterStatus.INFO_GOVERNANCE: partial(self.execute_function, None, self.ticker_service.handle_info_governance),
            TickerUpdaterStatus.INFO_CASH_AND_FINANCIAL_RATIOS: partial(self.execute_function, None, self.ticker_service.handle_info_cash_and_financial_ratios),
            TickerUpdaterStatus.INFO_MARKET_AND_FINANCIAL_METRICS: partial(self.execute_function, None, self.ticker_service.handle_info_market_and_financial_metrics),
            TickerUpdaterStatus.INFO_GENERAL_STOCK: partial(self.execute_function, None, self.ticker_service.handle_info_general_stock),
            TickerUpdaterStatus.INFO_TRADING_SESSION: partial(self.execute_function, None, self.ticker_service.handle_info_trading_session),
            TickerUpdaterStatus.CANDLE_MONTH: partial(self.execute_function, None, self.ticker_service.handle_candle_data, interval=CandleDataInterval.MONTH),
            TickerUpdaterStatus.CANDLE_WEEK: partial(self.execute_function, None, self.ticker_service.handle_candle_data, interval=CandleDataInterval.WEEK),
            TickerUpdaterStatus.CANDLE_DAY: partial(self.execute_function, None, self.ticker_service.handle_candle_data, interval=CandleDataInterval.DAY),
            TickerUpdaterStatus.CANDLE_HOUR: partial(self.execute_function, None, self.ticker_service.handle_candle_data, interval=CandleDataInterval.HOUR),
            TickerUpdaterStatus.CANDLE_MINUTE_5: partial(self.execute_function, None, self.ticker_service.handle_candle_data, interval=CandleDataInterval.MINUTE_5),
            TickerUpdaterStatus.CANDLE_MINUTE_1: partial(self.execute_function, None, self.ticker_service.handle_candle_data, interval=CandleDataInterval.MINUTE_1),
        }

    def map_and_execute_function(self, ticker_update_status: TickerUpdaterStatus) -> (Optional[pd.DataFrame], bool):
        """ Maps the ticker update status to the corresponding function and executes it.
        Args:
            ticker_update_status: The status of the ticker update.
        """
        if ticker_update_status in self.function_map:
            return self.function_map[ticker_update_status]()
        else:
            LOGGER.error(f"Ticker update status {ticker_update_status} not found in function map.")
            return None, False

    def execute_function(self,
                         default,
                         function,
                         *args,
                         **kwargs) -> (Optional[pd.DataFrame], bool):
        """ Executes a function with error handling and logging.
        Args:
            default: The default value to return in case of an error.
            function: The function to execute.
            *args: Arguments to pass to the function.
        Returns:
            A tuple containing the result of the function or the default value, and a boolean indicating success.
        """
        func_name = getattr(function, '__name__', '<anonymous>')

        try:
            result = function(*args, **kwargs)
            if result is None or (isinstance(result, pd.DataFrame) and result.empty) or(isinstance(result, bool) and not result):
                self.errors.append({"label": func_name, "status": "Empty DataFrame", "error": "Function returned an empty DataFrame."})
                return default, False
            return result, True
        except RuntimeError as e:
            LOGGER.error(f"{e}")
            self.errors.append({"label": func_name, "status":"Runtime Error", "error": str(e)})
            return default, False
        except Exception as e:
            LOGGER.error(f"{e}")
            self.errors.append({"label": func_name, "status": "Exception", "error": str(e)})
            return default, False

    @staticmethod
    def check_yfinance_exceptions() -> (bool, str):
        """Check for yfinance exceptions and handle them accordingly."""
        yf_errors = []
        sleep(2.5)  # Sleep to wait for the exception to be raised in the ErrorHandler
        exceptions = error_handler.get_exceptions()  # Check and raise exception in the main thread
        error_handler.clean_exceptions()
        for exc in exceptions:
            try:
                raise exc
            except RuntimeError as e:
                lines = str(e).split('\n')
                sublines = lines[1].split('\n') if len(lines) > 1 else []

                if "yfinance ERROR:" in lines[0]:
                    for line in sublines:
                        if "possibly delisted" in line:
                            match = re.match(r'^([^:]+):\s*\$[^:]+:\s*(.+)$', line)
                            if not match:
                                LOGGER.warning(f"No match found in line: {line}")
                                continue
                            ticker = match.group(1).strip()
                            remainder = match.group(2).strip()
                            parts = [part.strip() for part in remainder.split(';')]

                            delisted_status = parts[0] if len(parts) > 0 else None
                            earnings_info = parts[1] if len(parts) > 1 else None

                            yf_errors.append({
                                "ticker": ticker,
                                "status": delisted_status,
                                "error": earnings_info
                            })
                        else:
                            LOGGER.warning(f"Case not handled: {line}")

        return yf_errors

