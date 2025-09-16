import yfinance as yf
import pandas as pd
import pytz
from datetime import datetime
from typing import Type, Optional, List
from dataclasses import dataclass, field

from src.common.tools.library import safe_execute

from src.stock.src.TickerServiceBase import TickerServiceBase
from src.stock.src.db.models import CandleDataMonth, CandleDataWeek, CandleDataDay, CandleData1Hour, CandleData5Minutes, CandleData1Minute
from src.stock.src.CandleDataInterval import CandleDataInterval
from src.stock.src.db.database import Base

from logger_setup import LOGGER

pd.set_option('future.no_silent_downcasting', True)


@dataclass
class CandleService(TickerServiceBase):
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

    """ Handle the insertion or update of candlestick data for a given ticker in the DB"""
    def handle_candle_data(self, interval: CandleDataInterval) -> int:
        """
        Handle the bulk update or insertion of candlestick data into the database.

        :param interval: The interval for the candle data.
        """

        # __ fetch the appropriate model for the interval __
        model_class = self.interval_model_map.get(interval)

        # __ prepare the model class name for the bulk update __
        model_class_name = self.format_model_class_name(model_class=model_class)

        # __ fetch the latest candle data for the specified interval __
        filters = self.prepare_filters(model_class, additional_filters=None)
        last_candle = self.session.query(model_class).filter(*filters).order_by(model_class.date.desc()).first()  # TODO: use self.fetch_last_records

        # __ get the period for fetching the new candle data __
        period = self.get_period(last_candle=last_candle, interval=interval)

        # __ download the candle data for the specified interval and period __
        candle_data = self.download_candle_data(interval=interval, period=period)

        # TODO: insert here a check for None or empty DataFrame

        # __ post-process the downloaded data __
        candle_data = self.post_download(candle_data, ticker=self.ticker.symbol)

        # __ check if the candle data is empty __
        if candle_data is None or candle_data.empty:
            LOGGER.warning(f"{self.ticker.symbol} - {'Candle Data'.rjust(50)} - no new candle data available")
            return 0

        # __ get length of the candle data __
        candle_data_len = candle_data.shape[0] if isinstance(candle_data, pd.DataFrame) else 0

        # __ prepare the candle data for insertion or update __
        candle_data = self.prepare_candle_data(candle_data, interval)

        # __ if there's no last date, bulk update all the data __
        if not last_candle:
            # __ convert DataFrame rows to a list of CandleData instances __
            new_records = self.create_candle_data_list_of_records(df=candle_data, model_class=model_class, interval=interval)
            # __ perform the bulk insert __
            self.bulk_insert_records(records_to_insert=new_records, model_class_name=model_class_name)
            return candle_data_len

        # __ filter out candles that are older than the last_candle's date __
        last_date = last_candle.date
        if isinstance(last_date, datetime):
            last_date = last_date.date()  # Convert to date if it's a datetime

        # __ adjust filtering logic based on interval __
        if not self.is_intraday_interval(interval=interval):
            candle_data = candle_data[candle_data['date'].dt.date >= last_date]
            last_data_df = candle_data[candle_data['date'].dt.date == last_date]
            new_data_df = candle_data[candle_data['date'].dt.date >= last_date]
        else:
            # For intervals like '1h', '5m', etc., use direct datetime comparison
            candle_data = candle_data[candle_data['date'] >= last_candle.date]
            last_data_df = candle_data[candle_data['date'] == last_candle.date]
            new_data_df = candle_data[candle_data['date'] >= last_candle.date]

        # # __ update today's record if it already exists __
        # if not last_data_df.empty:
        #     self.update_last_candle(last_data_df, model_class_name=model_class_name, interval=interval, existing_record=last_candle)

        # __ delete the last record if it exists __
        if not last_data_df.empty:
            self.session.delete(last_candle)
            self.session.commit()

        # __ insert the remaining new records __
        if not new_data_df.empty:
            # __ convert DataFrame rows to a list of CandleData instances __
            new_records = self.create_candle_data_list_of_records(df=new_data_df, model_class=model_class, interval=interval)
            # __ perform the bulk insert __
            self.bulk_insert_records(records_to_insert=new_records, model_class_name=model_class_name)

        return candle_data_len

    def download_candle_data(self, interval: CandleDataInterval, period: str) -> Optional[pd.DataFrame]:
        """
        Download candle data for the given interval and period.

        :param interval: The interval for the candle data.
        :param period: The period for the candle data.
        :return: DataFrame containing the candle data.
        """
        # __ cap the period based on the interval __
        period = self.cap_period_based_on_interval(interval, period)

        # __ get the interval string for the given interval __
        interval_str = self.interval_map.get(interval)

        # TODO: add a protection when downloading week candles

        # __ download the candle data from Yahoo Finance __
        candle_data = safe_execute(
            None,
            lambda: yf.download(
                tickers=self.ticker.symbol,
                interval=interval_str,
                period=period,
                progress=False,
                auto_adjust=False
            )
        )

        # __ if the interval is '1d', '1wk', or '1mo', localize the 'date' column to UTC __
        if not self.is_intraday_interval(interval=interval) and candle_data is not None:
            candle_data.index = candle_data.index.tz_localize('UTC')

        return candle_data

    @ staticmethod
    def post_download(download: pd.DataFrame, ticker: str) -> pd.DataFrame:
        if isinstance(download.columns, pd.MultiIndex):
            # __ if the columns are MultiIndex, swap the levels and sort them __
            data = download.copy()  #  Create a copy to avoid modifying the original DataFrame in-place
            data.columns = data.columns.swaplevel(0, 1)
            data = data.sort_index(axis=1)  # Sort the columns to ensure consistency
            data = data[ticker].copy()
            return data

        return download

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

    def get_period(self, last_candle: Type[Base] = None, interval: CandleDataInterval = CandleDataInterval.DAY) -> str:
        """
        Get the appropriate period for fetching candle data based on the last recorded date.

        :param last_candle: The last recorded candle data.
        :param interval: The interval for fetching the candle data ('1d', '1h', '5m', '1m').
        :return: A valid period string for yfinance.
        """
        # __ calculate the number of days from the last candle to today __
        if last_candle:
            last_date = last_candle.date

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
        LOGGER.debug(f"{self.ticker.symbol} - {'CandleData'.rjust(50)} - Selected period based on {time_difference} \"{interval}\" units: {period}")

        return period

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
                (3, '1mo'),     # until 3 weeks you can get 1mo data
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

    def prepare_candle_data(self, candle_data: pd.DataFrame, interval: CandleDataInterval) -> pd.DataFrame:
        """
        Prepare the candle data DataFrame for insertion or update.

        :param candle_data: DataFrame containing the candle data.
        :param interval: Interval for the candle data.
        :return: Prepared DataFrame for insertion or update.
        """
        # __ reset the index and convert it to date __
        candle_data.index.name = 'date'
        candle_data.reset_index(inplace=True)
        candle_data['date'] = pd.to_datetime(candle_data['date'])

        # __ ensure that all datetime columns are timezone-aware __
        # candle_data['date'] = candle_data['date'].dt.tz_localize('UTC')

        # Add time zone column to preserve the time zone information
        if self.is_intraday_interval(interval):
            candle_data['time_zone'] = candle_data['date'].apply(lambda x: x.tzinfo.tzname(x) if x.tzinfo else 'UTC')

        # __ rename columns to match the model attributes
        candle_data.columns = [col.lower().replace(' ', '_') for col in candle_data.columns]

        # __ handle NaT values in the 'date' column __
        candle_data['date'] = candle_data['date'].fillna(pd.Timestamp('1970-01-01'))

        return candle_data

    def create_candle_data_list_of_records(self, df: pd.DataFrame, model_class: Type[Base], interval: CandleDataInterval) -> List[Type[Base]]:
        """
        Create a list of CandleData instances from a DataFrame.
        :param df:  DataFrame containing the candle data.
        :param model_class: The model class for the candle data.
        :param interval: Interval for the candle data.
        :return: List of CandleData instances.
        """
        last_update = datetime.now()
        new_records = []

        for _, row in df.iterrows():
            record_params = {
                'ticker_id': self.ticker.id,
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

    def update_last_candle(self,
                           today_data_df: pd.DataFrame,
                           model_class_name: str,
                           interval: CandleDataInterval,
                           existing_record: Type[Base]) -> None:
        """
        Update today's candle data if it already exists in the database.

        :param today_data_df: DataFrame containing today's candle data.
        :param model_class_name: The model class name for the candle data.
        :param interval: Interval for the candle data.
        :param existing_record: Existing record in the database.
        """
        # Prepare new data
        new_data = today_data_df.iloc[0].to_dict()
        new_data['ticker_id'] = self.ticker.id
        new_data['last_update'] = datetime.now()
        if self.is_intraday_interval(interval=interval):
            new_data['time_zone'] = new_data['date'].tzinfo.zone if new_data['date'].tzinfo else 'UTC'

        try:
            changes_log = self.compare_and_log_changes(existing_record, new_data, model_class_name.rjust(50))
            if changes_log:
                for key, value in new_data.items():
                    setattr(existing_record, key, value)
                self.session.commit()
                LOGGER.warning(f"{self.ticker.symbol} - {model_class_name.rjust(50)} - UPDATED record for \"{interval}\" on {new_data['date']}.")
                for change in changes_log:  # TODO: reuse the generic function
                    LOGGER.debug(change)
            else:
                LOGGER.warning(f"{self.ticker.symbol} - {model_class_name.rjust(50)} - No changes detected for \"{interval}\" on {new_data['date']}.")

        except Exception as e:
            self.session.rollback()
            LOGGER.error(f"Error occurred while updating today's candle: {e}")