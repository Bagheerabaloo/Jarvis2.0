import yfinance as yf
import pandas as pd
import pytz
import logging
import re
from time import sleep
from datetime import datetime, date
from typing import Type, Optional, List
from dataclasses import dataclass, field
from typing import Optional
from sqlalchemy.orm import session as sess
from sqlalchemy.sql import and_
from sqlalchemy import select, func
from sqlalchemy.sql import literal
from sqlalchemy.inspection import inspect

from src.common.tools.library import safe_execute

from src.stock.src.CandleService import *
from src.stock.src.TickerServiceBase import Ticker
from src.stock.src.RaiseOnErrorHandler import RaiseOnErrorHandler
from src.stock.YFinanceDataError import YFinanceDataError

from logger_setup import LOGGER, error_handler

pd.set_option('future.no_silent_downcasting', True)


@dataclass
class CandleBulkService:
    session: sess.Session
    symbols: list[str]
    commit_enable: bool = True
    interval_map: dict = field(default_factory=lambda: {
        CandleDataInterval.DAY: '1d',
        CandleDataInterval.HOUR: '1h',
        CandleDataInterval.MINUTE_5: '5m',
        CandleDataInterval.MINUTE_1: '1m',
        CandleDataInterval.WEEK: '1wk',
        CandleDataInterval.MONTH: '1mo'
    })
    interval_model_map: dict = field(default_factory=lambda: {
        CandleDataInterval.DAY: CandleDataDay,
        CandleDataInterval.HOUR: CandleData1Hour,
        CandleDataInterval.MINUTE_5: CandleData5Minutes,
        CandleDataInterval.MINUTE_1: CandleData1Minute,
        CandleDataInterval.WEEK: CandleDataWeek,
        CandleDataInterval.MONTH: CandleDataMonth
    })

    @staticmethod
    def is_intraday_interval(interval: CandleDataInterval) -> bool:
        """
        Check if the given interval is an intraday interval.

        :param interval: The interval to check.
        :return: True if the interval is intraday, False otherwise.
        """
        return interval in {CandleDataInterval.MINUTE_1, CandleDataInterval.MINUTE_5, CandleDataInterval.HOUR}

    @staticmethod
    def format_model_class_name(model_class: Type[Base]) -> str:
        """
        Format the model class name for logging.

        :param model_class: The SQLAlchemy model class.
        :return: Formatted model class name.
        """
        return ' '.join([x.capitalize() for x in model_class.__tablename__.replace('_', ' ').split(' ')]).rjust(25)

    @staticmethod
    def select_period_based_on_interval(time_difference: int, interval: CandleDataInterval) -> str:
        """
        Select the appropriate period for yfinance based on the time difference and interval.

        :param time_difference: The difference in time units (days, hours, minutes).
        :param interval: The interval for the candle data ('1d', '1h', '5m', '1m').
        :return: The appropriate period string for yfinance.
        """
        period_options = {
            CandleDataInterval.MONTH: [
                (1, '3mo'),     # until 1 month you can get 3mo data
                (4, '6mo'),     # until 4 months you can get 6mo data
                (10, '1y'),     # until 10 months you can get 1y data
                (20, '2y'),     # until 20 months you can get 2y data
                (50, '5y'),     # until 50 months you can get 5y data
                (100, '10y')    # until 100 months you can get 10y data
            ],
            CandleDataInterval.WEEK: [
                # (3, '1mo'),     # until 3 weeks you can get 1mo data
                (12, '3mo'),    # until 12 weeks you can get 3mo data
                (24, '6mo'),    # until 24 weeks you can get 6mo data
                (50, '1y'),     # until 50 weeks you can get 1y data
                (100, '2y'),    # until 100 weeks you can get 2y data
                (250, '5y'),    # until 250 weeks you can get 5y data
                (500, '10y')    # until 500 weeks you can get 10y data
            ],
            CandleDataInterval.DAY: [
                (4, '5d'),      # until 4 days you can get 5d data
                (27, '1mo'),    # until 27 days you can get 1mo data
                (85, '3mo'),    # until 12 weeks you can get 3mo data
                (170, '6mo'),   # until 5 months and 2 weeks you can get 6mo data
                (350, '1y'),    # until 11 months and 2 weeks you can get 1y data
                (700, '2y'),    # until 1 year and 11 months you can get 2y data
                (1825, '5y'),   # until 5 years you can get 5y data
                (3650, '10y')   # until 10 years you can get 10y data
            ],
            CandleDataInterval.HOUR: [
                (72, '5d'),     # until 72 hours (3 days) you can get 5d data
                (600, '1mo'),   # until 600 hours (25 days) you can get 1mo data
                (2000, '3mo'),  # until 2000 hours (2.5 months) you can get 3mo data
                (4000, '6mo'),  # until 4000 hours (5 months) you can get 6mo data
                (8500, '1y'),   # until 8500 hours (11 months and 2 weeks) you can get 1y data
                (17520, '2y')   # until 17520 hours (2 years) you can get 2y data
            ],
            CandleDataInterval.MINUTE_5: [
                (300, '1d'),    # until 300 minutes (5 hours) you can get 1d data
                (5000, '5d'),   # until 5000 minutes (3.5 days) you can get 5d data
                (38880, '1mo')  # until 38880 minutes (27 days) you can get 1mo data
            ],
            CandleDataInterval.MINUTE_1: [
                (300, '1d'),    # until 300 minutes (5 hours) you can get 1d data
                (5000, '5d'),   # until 5000 minutes (3.5 days) you can get 5d data
                (38880, '1mo')  # until 38880 minutes (27 days) you can get 1mo data
            ]
        }

        # Default to 'max' period if no matching period is found
        default_period = 'max'

        # Get the period list for the given interval
        periods = period_options.get(interval, [])

        # Find the appropriate period using next
        selected_period = next((period for limit, period in periods if time_difference <= limit), default_period)

        return selected_period

    @staticmethod
    def cap_period_based_on_interval(interval: CandleDataInterval, current_period: str) -> str:
        """
        Cap the period based on the given interval using predefined rules, but only if the current period exceeds the cap.

        :param interval: The interval for which the data is fetched (e.g., '1h', '5m', '1m').
        :param current_period: The current period being considered (e.g., '1mo', '1y').
        :return: The capped period based on the interval or the original period if it is already within the allowed range.
        """
        # Define the capping rules
        interval_cap_rules = {
            CandleDataInterval.HOUR: '2y',
            CandleDataInterval.MINUTE_5: '1mo',
            CandleDataInterval.MINUTE_1: '5d'
        }

        # Valid periods list in increasing order
        valid_periods = ['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max']

        # Get the cap period for the given interval
        cap_period = interval_cap_rules.get(interval, current_period)

        # Get the indices of the current period and the cap period in the valid periods list
        current_index = valid_periods.index(current_period)
        cap_index = valid_periods.index(cap_period)

        # Return the capped period only if the current period exceeds the cap period
        if current_index <= cap_index:
            return current_period
        else:
            return cap_period

    def commit(self):
        if self.commit_enable:
            LOGGER.debug(f"{'Commit'.rjust(50)} - COMMITTED successfully.")
            self.session.commit()
        else:
            self.session.rollback()

    def filter_candles_max_date_quantile(self,
                                         model_class: Type[Base],
                                         tickers_df: pd.DataFrame,
                                         min_quantile: float = 0.1
                                         ) -> pd.DataFrame:
        # __ fetch the oldest candle data date for the specified interval for all tickers_id__
        max_dates_list = (
            self.session
            .query(Ticker.symbol, func.max(model_class.date).label("max_date"))
            .join(Ticker, model_class.ticker_id == Ticker.id)
            .filter(model_class.ticker_id.in_(tickers_df["id"].tolist()))
            .group_by(model_class.ticker_id, Ticker.symbol)
            .all()
        )

        max_dates_df = pd.DataFrame(max_dates_list, columns=["symbol", "max_date"])

        q1 = max_dates_df["max_date"].quantile(min_quantile)
        q3 = max_dates_df["max_date"].quantile(1)
        iqr = q3 - q1

        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        LOGGER.info(f"Lower bound: {lower_bound}, Upper bound: {upper_bound}")

        filtered_df = max_dates_df[(max_dates_df["max_date"] >= lower_bound) & (max_dates_df["max_date"] <= upper_bound)]
        return max_dates_df


    def get_period(self, last_date: date = None, interval: CandleDataInterval = CandleDataInterval.DAY) -> str:
        """
        Get the appropriate period for fetching candle data based on the last recorded date.

        :param last_date: The last reference date
        :param interval: The interval for fetching the candle data ('1d', '1h', '5m', '1m').
        :return: A valid period string for yfinance.
        """
        # __ calculate the number of days from the last candle to today __
        if last_date:
            now = datetime.now(pytz.UTC)

            # Convert to datetime if necessary and ensure it's timezone-aware (UTC)
            # if isinstance(last_date, datetime) and last_date.tzinfo is None:
            #     last_date = last_date.replace(tzinfo=pytz.UTC)

            # __ determine the difference in the appropriate units __
            if interval == CandleDataInterval.DAY:
                time_difference = (now.date() - last_date).days
            elif interval == CandleDataInterval.WEEK:
                time_difference = int((now.date() - last_date).days // 7)  # weeks
            elif interval == CandleDataInterval.MONTH:
                time_difference = (now.year - last_date.year) * 12 + now.month - last_date.month  # months
            elif interval == CandleDataInterval.HOUR:
                time_difference = int((now - last_date).total_seconds() // 3600)  # hours
            elif interval in [CandleDataInterval.MINUTE_1, CandleDataInterval.MINUTE_5]:
                time_difference = int((now - last_date).total_seconds() // 60)  # minutes
            else:
                # __ default to days if interval is unknown
                time_difference = (now.date() - last_date).days
        else:
            # __ if there's no last candle, set days_difference to cover the max period __
            time_difference = float('inf')

        # __ select the appropriate period based on the time difference and interval __
        period = self.select_period_based_on_interval(time_difference, interval)

        # __ print the selected period for debugging __
        LOGGER.debug(f"{'CandleData'.rjust(50)} - Selected period based on {time_difference} \"{interval}\" units: {period}")

        return period

    def download_candle_data(self, symbols, interval_str: str, period: str) -> Optional[pd.DataFrame]:
        """
        Download candle data for the given interval and period.

        :param symbols: The list of symbols for which to download the candle data.
        :param interval_str: The interval for the candle data.
        :param period: The period for the candle data.
        :return: DataFrame containing the candle data.
        """
        # TODO: add a protection when downloading week candles
        if len(symbols) == 0:
            return None

        # __ download the candle data from Yahoo Finance __
        try:
            download = yf.download(
                    tickers=symbols,
                    interval=interval_str,
                    period=period,
                    # progress=False
                )

            # Ensure all errors are processed before proceeding
            sleep(2)  # Give enough time for the timer to trigger
            error_handler.check_for_exception()  # Check and raise exception in the main thread

        except RuntimeError as e:
            # LOGGER.info(str(e))
            lines = str(e).split('\n\n')
            sublines = lines[1].split('\n') if len(lines) > 1 else []

            if "yfinance ERROR:" in lines[0] and "Failed download" in lines[1]:
                num_failed = int(sublines[0].split()[0])
                failed_tickers = []
                def extract_tickers(log_message):
                    # Regular expression to match tickers inside square brackets
                    match = re.search(r"\[([^\]]+)\]", log_message)
                    if match:
                        tickers = match.group(1).replace("'", "").split(", ")
                        return [ticker.strip() for ticker in tickers]  # Clean up whitespace
                    return []
                for subline in sublines[1:]:
                    if "YFChartError" in subline:
                        extracted_tickers = extract_tickers(subline)
                        extracted_text = lambda s: str(m.group(1)) if (m := re.search(r"YFChartError\((.*?)\)", s)) else None
                        self.mark_failed_tickers(extracted_tickers, reason=extracted_text(subline))
                        failed_tickers += extracted_tickers
                    elif "YFInvalidPeriodError" in subline:
                        extracted_tickers = extract_tickers(subline)
                        extracted_text = lambda s: str(m.group(1)) if (m := re.search(r"YFInvalidPeriodError\((.*?)\)", s)) else None
                        # TODO: handle this error differently
                        self.mark_failed_tickers(extracted_tickers, reason=extracted_text(subline))
                        failed_tickers += extracted_tickers
                    elif "JSONDecodeError" in subline:
                        raise YFinanceDataError(f"JSONDecodeError: {subline}")
                if len(failed_tickers) != num_failed:
                    raise Exception

                download = self.download_candle_data([x for x in symbols if x not in failed_tickers], interval_str, period)
            else:
                LOGGER.error(f"{'Candle Data'.rjust(25)} - Error downloading candle data.")
                return None
        except Exception as e:
            LOGGER.error(f"{'Candle Data'.rjust(25)} - Error downloading candle data.")
            return None

        return download

    @ staticmethod
    def post_download(download: pd.DataFrame) -> pd.DataFrame:
        # __ check if the download was successful __
        if isinstance(download.columns, pd.MultiIndex):
            # __ if the columns are MultiIndex, swap the levels and sort them __
            data = download.copy()  #  Create a copy to avoid modifying the original DataFrame in-place
            data.columns = data.columns.swaplevel(0, 1)
            data = data.sort_index(axis=1)  # Sort the columns to ensure consistency
            return data

        return download

    def mark_failed_tickers(self, failed_tickers: List[str], reason: str = None) -> None:
        """
        Mark the failed tickers in the database.

        :param failed_tickers: The list of tickers that failed to download.
        :param reason: Optional reason for the failure.
        """

        for symbol in failed_tickers:
            ticker = self.get_ticker_by_symbol(symbol)
            if ticker:
                ticker.failed_candle_download = True
                ticker.last_update = datetime.now()
                if reason:
                    reason = reason.replace("%", "")
                    ticker.yf_error = reason
                self.commit()

    def handle_failed_download(self, symbols: List[str], interval_str: str, period: str, depth=0, max_depth=10) -> list[str]:
        """
        Handle a failed download of candle data by recursively retrying with a bisection-like method.

        :param symbols: List of symbols for which to download the candle data.
        :param interval_str: The interval for the candle data.
        :param period: The period for the candle data.
        :param depth: Current recursion depth (default: 0).
        :param max_depth: Maximum recursion depth to prevent infinite loops (default: 5).
        :return: List of failed tickers.
        """
        if not symbols:
            return []

        # Base case: Stop if depth exceeds max_depth
        if depth > max_depth:
            LOGGER.error(f"Max recursion depth reached with symbols: {symbols}")
            return []

        LOGGER.debug(f"Recursion depth: {depth}, symbols: {symbols}")

        try:
            # Try downloading the entire batch at once
            yf.download(
                tickers=symbols,  # Pass all symbols in one request
                interval=interval_str,
                period=period,
                progress=False
            )
            return []  # Success, no failed tickers

        except RuntimeError as e:
            LOGGER.debug(f"Download failed for batch {symbols}: {e}")

            # If there's only one symbol, mark it as failed
            if len(symbols) == 1:
                return symbols

            # Otherwise, split the batch and try recursively
            mid = len(symbols) // 2
            first_half, second_half = symbols[:mid], symbols[mid:]

            sleep(3)  # Avoid API rate limiting

            failed_first = self.handle_failed_download(first_half, interval_str, period, depth + 1, max_depth)
            failed_second = self.handle_failed_download(second_half, interval_str, period, depth + 1, max_depth)

            return failed_first + failed_second  # Combine all failed tickers

    def prepare_candle_data(self, candles: pd.DataFrame, interval: CandleDataInterval) -> pd.DataFrame:
        """
        Prepare the candle data DataFrame for insertion or update.

        :param candle_data: DataFrame containing the candle data.
        :param interval: Interval for the candle data.
        :return: Prepared DataFrame for insertion or update.
        """

        # __ drop entries where 'Open', 'High', 'Low' or 'Close' is NaN __
        candles = candles.dropna(subset=['Open', 'High', 'Low', 'Close'])

        # __ reset the index and convert it to date __
        candles.reset_index(inplace=True)
        if 'Date' in candles.columns:
            candles = candles.rename(columns={'Date': 'date'})
        elif 'Datetime' in candles.columns:
                candles = candles.rename(columns={'Datetime': 'date'})
        candles['date'] = pd.to_datetime(candles['date'])
        candles.reset_index(inplace=True, drop=True)

        # __ ensure that all datetime columns are timezone-aware __
        # candle_data['date'] = candle_data['date'].dt.tz_localize('UTC')

        # Add time zone column to preserve the time zone information
        if self.is_intraday_interval(interval):
            candles['time_zone'] = candles['date'].apply(lambda x: x.tzinfo.zone if x.tzinfo else 'UTC')
            candles['date'] = candles['date'].dt.tz_convert('Europe/London')

        # __ rename columns to match the model attributes
        candles.columns = [col.lower().replace(' ', '_') for col in candles.columns]

        # __ handle NaT values in the 'date' column __
        candles['date'] = candles['date'].fillna(pd.Timestamp('1970-01-01'))

        return candles

    def prepare_filters(self, model_class: Type[Base], additional_filters: Optional[List]) -> List:
        """
        Prepare the filter criteria for the query.

        :param model_class: The SQLAlchemy model class.
        :param additional_filters: Optional list of additional filters for the query.
        :return: List of filter criteria.
        """
        filters = [model_class.ticker_id == literal(self.ticker.id)]
        if additional_filters:
            filters.extend(additional_filters)
        return filters

    def get_latest_candle_by_symbol(self, symbol: str, model_class: Type[Base]):
        subquery = (
            select(func.max(model_class.date))
            .join(Ticker, model_class.ticker_id == Ticker.id)
            .where(Ticker.symbol == symbol)
            .scalar_subquery()
        )

        query = (
            select(model_class)
            .join(Ticker, model_class.ticker_id == Ticker.id)
            .where((Ticker.symbol == symbol) & (model_class.date == subquery))
        )

        last_candle = self.session.execute(query).scalars().all()
        if not last_candle or len(last_candle) == 0:
            return None

        return last_candle[0]

    def get_ticker_by_symbol(self, symbol: str):
        query = select(Ticker).where(Ticker.symbol == symbol)
        return self.session.execute(query).scalar_one_or_none()

    def create_candle_data_list_of_records(self, ticker: Ticker, df: pd.DataFrame, model_class: Type[Base], interval: CandleDataInterval) -> List[Type[Base]]:
        """
        Create a list of CandleData instances from a DataFrame.
        :param ticker_id: The ID of the ticker for the candle data.
        :param df:  DataFrame containing the candle data.
        :param model_class: The model class for the candle data.
        :param interval: Interval for the candle data.
        :return: List of CandleData instances.
        """
        last_update = datetime.now()
        new_records = []

        for _, row in df.iterrows():
            record_params = {
                'ticker_id': ticker.id,
                'last_update': last_update,
                'date': row['date'],
                'open': row['open'],
                'high': row['high'],
                'low': row['low'],
                'close': row['close'],
                'adj_close': row['adj_close'],
                'volume': row['volume']
            }

            if self.is_intraday_interval(interval):
                record_params['time_zone'] = row.get('time_zone', 'UTC')

            new_records.append(model_class(**record_params))

        return new_records

    def update_all_tickers_candles(self):
        # __ get all Ticker objects from DB __
        query = self.session.query(Ticker.id, Ticker.symbol).filter(Ticker.symbol.in_(self.symbols))
        tickers_df = pd.DataFrame(query.all(), columns=["id", "symbol"])

        intervals = list(CandleDataInterval)
        # intervals = [CandleDataInterval.MINUTE_5]
        for interval in intervals:
            # __ fetch the appropriate model for the interval __
            model_class = self.interval_model_map.get(interval)

            # __ prepare the model class name for the bulk update __
            model_class_name = self.format_model_class_name(model_class=model_class)

            # __ filter tickers based on the max date quantile __
            filtered_df = self.filter_candles_max_date_quantile(model_class=model_class, tickers_df=tickers_df, min_quantile=0)

            # __ get the symbols to update __
            symbols_to_update = filtered_df["symbol"].tolist()

            # __ log how many tickers are left after filtering __
            LOGGER.info(f"{'Total tickers after quantile filtering:'.ljust(35)} {len(filtered_df)}")

            # __ get min date __
            min_date = filtered_df["max_date"].min()

            # __ get the period for fetching the new candle data __
            period = self.get_period(last_date=min_date, interval=interval)

            # __ cap the period based on the interval __
            period = self.cap_period_based_on_interval(interval, period)

            # __ get the interval string for the given interval __
            interval_str = self.interval_map.get(interval)

            # __ download the candle data for the specified interval and period __
            try:
                candle_data = self.download_candle_data(symbols=symbols_to_update, interval_str=interval_str, period=period)
            except YFinanceDataError:
                LOGGER.info(f"{'Candle Data'.rjust(25)} - Retrying download after 90 seconds.")
                sleep(90)
                candle_data = self.download_candle_data(symbols=symbols_to_update, interval_str=interval_str, period=period)

            candle_data = self.post_download(candle_data)

            for ticker in candle_data.columns.levels[0]:
                candles = candle_data[ticker].copy()
                candles.columns = candles.columns.get_level_values(0)
                self.update_ticker_candles(candles, ticker, interval, model_class, model_class_name)

            sleep_time = 2.5 + ((len(symbols_to_update) - 1) / 499) * (20 - 0.5)
            LOGGER.info(f"Sleeping for {round(sleep_time, 2)} seconds...")
            sleep(sleep_time)

    def update_ticker_candles(self,
                              candles: pd.DataFrame,
                              ticker: str,
                              interval: CandleDataInterval,
                              model_class: Type[Base] = None,
                              model_class_name: str = None) -> None:
        # # __ if the interval is '1d', '1wk', or '1mo', localize the 'date' column to UTC __
        # if not self.is_intraday_interval(interval=interval) and candles is not None:
        #     candles.index = candles.index.tz_localize('UTC')

        # __ check if the candle data is empty __
        if candles is None or candles.empty:
            LOGGER.warning(f"{ticker.rjust(10)} - {'Candle Data'.rjust(25)} - no new candle data available")
            return

        # __ get ticker_object __
        ticker_obj = self.get_ticker_by_symbol(ticker)

        # __ prepare the candle data for insertion or update __
        candles = self.prepare_candle_data(candles, interval)
        last_candle = self.get_latest_candle_by_symbol(ticker, model_class)

        # __ if there's no last date, bulk update all the data __
        if not last_candle:
            # __ convert DataFrame rows to a list of CandleData instances __
            new_records = self.create_candle_data_list_of_records(ticker=ticker_obj, df=candles, model_class=model_class, interval=interval)
            # __ perform the bulk insert __
            self.session.bulk_save_objects(new_records)
            LOGGER.info(f"{ticker.rjust(10)} (id: {str(ticker_obj.id).rjust(5)}) - {model_class_name} - {len(new_records)} records inserted.")
            return None

        # __ filter out candles that are older than the last_candle's date __
        last_date = last_candle.date
        if isinstance(last_date, datetime):
            last_date = last_date.date()  # Convert to date if it's a datetime

        # __ adjust filtering logic based on interval __
        if not self.is_intraday_interval(interval=interval):
            candles = candles[candles['date'].dt.date >= last_date]
            last_data_df = candles[candles['date'].dt.date == last_date]
            new_data_df = candles[candles['date'].dt.date >= last_date]
        else:
            # For intervals like '1h', '5m', etc., use direct datetime comparison
            candles = candles[candles['date'] >= last_candle.date]
            last_data_df = candles[candles['date'] == last_candle.date]
            new_data_df = candles[candles['date'] >= last_candle.date]

        # # __ update today's record if it already exists __
        # if not last_data_df.empty:
        #     self.update_last_candle(last_data_df, model_class_name=model_class_name, interval=interval, existing_record=last_candle)

        # __ delete the last record if it exists __
        try:
            with self.session.begin_nested():
                if not last_data_df.empty:
                    self.session.delete(last_candle)
                    self.session.flush()

                # __ insert the remaining new records __
                if not new_data_df.empty:
                    # __ convert DataFrame rows to a list of CandleData instances __
                    new_records = self.create_candle_data_list_of_records(ticker=ticker_obj, df=new_data_df, model_class=model_class, interval=interval)
                    # __ perform the bulk insert __
                    self.session.bulk_save_objects(new_records)
                    LOGGER.info(f"{ticker.rjust(10)} (id: {str(ticker_obj.id).rjust(5)}) - {model_class_name} - {len(new_records)} records inserted.")

                self.commit()
        except Exception as e:
            self.session.rollback()
            print(f"Error: {e}")