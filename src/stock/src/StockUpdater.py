import yfinance as yf
import pandas as pd
from time import time, sleep
from datetime import datetime, timedelta
from sqlalchemy.orm import session as sess
from sqlalchemy import text

from src.common.tools.library import seconds_to_time, safe_execute
from src.stock.src.TickerService import TickerService
from src.stock.src.database import session_local
from src.stock.src.CandleService import CandleDataInterval, CandleDataDay
from src.stock.src.TickerLister import TickerLister

from logger_setup import LOGGER
import logging


class RaiseOnErrorHandler(logging.Handler):
    def emit(self, record):
        log_message = self.format(record)
        if record.levelname == "ERROR":
            raise RuntimeError(f"yfinance ERROR: {log_message}")


# Configure the logger for yfinance
logger = logging.getLogger("yfinance")
logger.setLevel(logging.ERROR)
logger.addHandler(RaiseOnErrorHandler())


class StockUpdater:
    def __init__(self, session: sess.Session):
        self.session = session

    @staticmethod
    def execute_function(default, function, *args, label: str = ''):
        try:
            return function(*args)
        except RuntimeError as e:
            if label in ["info_trading_session", "calendar", "actions"]: # TODO: handle each case separately
                LOGGER.warning(f"{label} - {e}")
                return default
            LOGGER.error(f"{e}")
            return default
        except Exception as e:
            LOGGER.error(f"{e}")
            return default

    def update_all_tickers(self, symbols: list[str]):
        # __ start tracking the elapsed time __
        start_time = time()

        # __ update all tickers __
        for symbol in symbols:
            try:
                self.update_ticker(symbol=symbol)
            except RuntimeError as e:
                LOGGER.error(f"{e}")
            except Exception as e:
                LOGGER.warning(f"{symbol} - Error: {e}")
                sleep(1)

        # __ stop tracking the elapsed time and print the stats __
        end_time = time()
        total_time = seconds_to_time(end_time - start_time)
        LOGGER.info(f"{'Total elapsed time:'.ljust(25)} {total_time['hours']} hours {total_time['minutes']} min {total_time['seconds']} sec")
        LOGGER.info(f"{'Total tickers:'.ljust(25)} {len(symbols)}")
        LOGGER.info(f"{'Average time per ticker:'.ljust(25)} {round((end_time - start_time) / len(symbols), 3)} sec")

    def update_ticker(self, symbol: str):
        # __ start tracking the elapsed time __
        start_time = time()
        LOGGER.info(f"{symbol.rjust(5)} - Start updating...")

        # __ check if the symbol is an index __
        is_index = symbol.startswith("^")

        # __ get all the data for a sample ticker from yahoo finance API __
        stock = yf.Ticker(symbol)

        # __ update the database with new data __
        ticker_service = TickerService(session=self.session, symbol=symbol, commit_enable=True)

        # __ ticker __
        info = safe_execute(None, lambda: getattr(stock, "info"))

        # __ handle ticker update/insert __
        success_ticker = ticker_service.handle_ticker(info=stock.info) if info is not None else None
        if not success_ticker:
            self.execute_function(None, lambda: ticker_service.final_update_ticker())
            return False

        if not is_index:
            self.execute_function(None, lambda: ticker_service.handle_balance_sheet(
                balance_sheet=stock.balance_sheet,
                period_type="annual"))
            self.execute_function(None, lambda: ticker_service.handle_balance_sheet(
                balance_sheet=stock.quarterly_balance_sheet,
                period_type="quarterly"))
            self.execute_function(None, lambda: ticker_service.handle_cash_flow(
                cash_flow=stock.cashflow,
                period_type="annual"))
            self.execute_function(None, lambda: ticker_service.handle_cash_flow(
                cash_flow=stock.quarterly_cashflow,
                period_type="quarterly"))
            self.execute_function(None, lambda: ticker_service.handle_financials(
                financials=stock.financials,
                period_type="annual"))
            self.execute_function(None, lambda: ticker_service.handle_financials(
                financials=stock.quarterly_financials,
                period_type="quarterly"))
            self.execute_function(None, lambda: ticker_service.handle_actions(actions=stock.actions), label='actions')
            self.execute_function(None, lambda: ticker_service.handle_calendar(calendar=stock.calendar), label='calendar')

            # __ earnings dates __
            earning_dates = safe_execute(None, lambda: getattr(stock, "earnings_dates"))
            self.execute_function(None, lambda: ticker_service.handle_earnings_dates(
                earnings_dates=stock.earnings_dates)) if earning_dates is not None else None

        # __ info __
        if info is not None:
            self.execute_function(None, lambda: ticker_service.handle_info_company_address(info_data=stock.info))
            if not is_index:
                self.execute_function(None, lambda: ticker_service.handle_sector_industry_history(info_data=stock.info))
            self.execute_function(None, lambda: ticker_service.handle_info_target_price_and_recommendation(info_data=stock.info))
            self.execute_function(None, lambda: ticker_service.handle_info_governance(info_data=stock.info))
            self.execute_function(None, lambda: ticker_service.handle_info_cash_and_financial_ratios(info_data=stock.info))
            self.execute_function(None, lambda: ticker_service.handle_info_market_and_financial_metrics(info_data=stock.info))

            isin = safe_execute(None, lambda: getattr(stock, "isin"))
            history_metadata = safe_execute(None, lambda: getattr(stock, "history_metadata"))
            if isin is not None and history_metadata is not None:
                self.execute_function(None, lambda: ticker_service.handle_info_general_stock(
                    isin=stock.isin,
                    info_data=stock.info,
                    history_metadata=history_metadata))

        if not is_index:
            insider_purchases = safe_execute(None, lambda: getattr(stock, "insider_purchases"))
            self.execute_function(None, lambda: ticker_service.handle_insider_purchases(
                insider_purchases=insider_purchases)) if insider_purchases is not None else None

            insider_roster_holders = safe_execute(None, lambda: getattr(stock, "insider_roster_holders"))
            self.execute_function(None, lambda: ticker_service.handle_insider_roster_holders(
                insider_roster_holders=insider_roster_holders), label="insider_roster_holders") if insider_roster_holders is not None else None

            insider_transactions = safe_execute(None, lambda: getattr(stock, "insider_transactions"))
            self.execute_function(None, lambda: ticker_service.handle_insider_transactions(
                insider_transactions=insider_transactions)) if insider_transactions is not None else None

            institutional_holders = safe_execute(None, lambda: getattr(stock, "institutional_holders"))
            self.execute_function(None, lambda: ticker_service.handle_institutional_holders(
                institutional_holders=institutional_holders)) if institutional_holders is not None else None

            major_holders = safe_execute(None, lambda: getattr(stock, "major_holders"))
            self.execute_function(None, lambda: ticker_service.handle_major_holders(
                major_holders=major_holders), label="major_holders") if major_holders is not None else None

            mutual_fund_holders = safe_execute(None, lambda: getattr(stock, "mutualfund_holders"))
            self.execute_function(None, lambda: ticker_service.handle_mutual_fund_holders(
                mutual_fund_holders=mutual_fund_holders)) if mutual_fund_holders is not None else None

            recommendations = safe_execute(None, lambda: getattr(stock, "recommendations"))
            self.execute_function(None, lambda: ticker_service.handle_recommendations(
                recommendations=stock.recommendations)) if recommendations is not None else None

            upgrades_downgrades = safe_execute(None, lambda: getattr(stock, "upgrades_downgrades"))
            self.execute_function(None, lambda: ticker_service.handle_upgrades_downgrades(
                upgrades_downgrades=stock.upgrades_downgrades)) if upgrades_downgrades is not None else None

        if info is not None:
            basic_info = safe_execute(None, lambda: getattr(stock, "basic_info"))
            if basic_info is not None:
                self.execute_function(None, lambda: ticker_service.handle_info_trading_session(
                    info=stock.info,
                    basic_info=stock.basic_info,
                    history_metadata=stock.history_metadata),
                    label="info_trading_session")

        # __ handle candle data update/insert __
        intervals = list(CandleDataInterval)
        for interval in intervals:
            self.execute_function(None, lambda: ticker_service.handle_candle_data(interval=interval))

        self.execute_function(None, lambda: ticker_service.final_update_ticker())

        # __ stop tracking the elapsed time and print the difference __
        end_time = time()
        total_time = seconds_to_time(end_time - start_time)
        LOGGER.info(f"{symbol} - Total time: {total_time['minutes']} min {total_time['seconds']} sec\n")

