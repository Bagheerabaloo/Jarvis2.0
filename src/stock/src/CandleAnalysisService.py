from dataclasses import field
import pandas as pd
import pandas_ta as ta
from typing import List, Optional

from sqlalchemy.orm import session as sess

from stock.src.models import CandleDataDay, CandleDataWeek, CandleDataMonth, CandleData1Hour, CandleData5Minutes, CandleData1Minute
from stock.src.models import CandleAnalysisCandlestickDay, CandleAnalysisIndicatorsDay, CandleAnalysisTrendMethod1Day
from stock.src.TickerServiceBase import Ticker
from stock.src.CandleDataInterval import CandleDataInterval


import matplotlib
from src.common.tools.library import *

matplotlib.use('TkAgg')


@dataclass
class CandleAnalysisService:
    symbol: str                                     # The ticker symbol
    session: sess.Session                           # The database session
    interval: CandleDataInterval                    # The time interval for the candlestick data
    candle_data: pd.DataFrame = field(init=False)   # DataFrame to store the candlestick data

    # Mapping of intervals to corresponding models
    interval_model_map: dict = field(default_factory=lambda: {
        CandleDataInterval.DAY: CandleDataDay,
        CandleDataInterval.WEEK: CandleDataWeek,
        CandleDataInterval.MONTH: CandleDataMonth,
        CandleDataInterval.HOUR: CandleData1Hour,
        CandleDataInterval.MINUTE_5: CandleData5Minutes,
        CandleDataInterval.MINUTE_1: CandleData1Minute
    })

    def __post_init__(self):
        """ Load the candlestick data from the database upon initialization. """
        self.candle_data = self.load_candle_data()

    def load_candle_data(self) -> Optional[pd.DataFrame]:
        """
        Load candlestick data from the database based on the ticker and interval.

        :return: DataFrame containing the candlestick data.
        """
        # # __ sqlAlchemy __ create new session
        # session = session_local()

        try:
            # Get the candlestick model corresponding to the interval
            model_class = self.interval_model_map.get(self.interval)
            if not model_class:
                raise ValueError(f"Unsupported interval: {self.interval}")

            # Query to get the candlestick data
            query = self.session.query(model_class).filter(model_class.ticker.has(symbol=self.symbol)).order_by(model_class.date)

            # Convert the query results to a DataFrame
            df = pd.read_sql(query.statement, self.session.bind)

            print(f"        - {df.shape[0]} candles loaded for {self.symbol} with interval {self.interval.value}")
            return df
        except Exception as e:
            print(f"        - Error loading candlestick data: {e}")
            return pd.DataFrame()  # Return an empty DataFrame in case of error
        # finally:
        #     # Close the database session
        #     self.session.close()

    """ Perform Candlestick Analysis"""
    def analyze(self) -> pd.DataFrame:
        """
        Example method to perform specific analysis on the candlestick data.
        """
        if self.candle_data.empty:
            print("     Candlestick data not available for analysis")
            return None

        candle_data = self.candle_data.copy().rename(
            columns={"date": "Date", "open": "Open", "high": "High", "low": "Low", "close": "Close"}
        )
        candle_data = self.add_volatility(candle_data=candle_data)
        candle_data = self.add_moving_averages(candle_data)
        candle_data = self.add_candlestick_info(candle_data=candle_data)
        #   __ add oscillators __
        candle_data = self.add_relative_strength_index(candle_data=candle_data, period=14)
        # candle_data = self.add_trend(candles=candle_data)
        #   __ add trend analysis __
        candle_data = self.add_trend_atr(candles=candle_data)
        candle_data['Session'] = range(len(candle_data))
        candle_data = self.add_extreme_points(candles=candle_data)
        candle_data = self.add_local_max(candles=candle_data, window=13)
        candle_data = self.add_local_max(candles=candle_data, window=27)
        candle_data = self.add_local_max(candles=candle_data, window=51)
        candle_data = self.add_local_max(candles=candle_data, window=101)
        candle_data = self.add_local_min(candles=candle_data, window=13)
        candle_data = self.add_local_min(candles=candle_data, window=27)
        candle_data = self.add_local_min(candles=candle_data, window=51)
        candle_data = self.add_local_min(candles=candle_data, window=101)

        # __ plot the candlestick data __
        # candle_data = candle_data.iloc[-1000:].reset_index(drop=True)
        # candle_plot = CandlePlot(
        #     symbol=self.symbol,
        #     interval=self.interval,
        #     candle_data=candle_data,
        #     oscillator_column="ATR%"
        # )
        #
        # candle_plot.plot_candle_data(
        #     plot_ma=True,
        #     plot_oscillator=True,
        #     plot_sensitivity_channel=False,
        #     plot_intervals=False,
        #     plot_trend_lines=False,
        #     plot_local_max=True,
        # )

        print(f"        - Candlestick data analysis completed")
        self.candle_data = candle_data
        return candle_data

    def handle_candle_analysis_data(self, df: pd.DataFrame) -> None:
        """
        Handle the bulk update or insertion of candlestick analysis data into the database.

        :param df: DataFrame containing the candlestick analysis data to be processed.
        """
        # __ CANDLESTICK __
        # __ select columns related to candlestick patterns __

        if df.empty:
            print("        - No candlestick analysis data to handle.")
            return None

        candlestick_columns = ['id', 'ticker_id', 'bullish', 'bodyDelta',
                               'shadowDelta', '%body', '%upperShadow', '%lowerShadow', 'longBody',
                               'shadowImbalance', 'shavenHead', 'shavenBottom', 'doji', 'spinningTop',
                               'umbrellaLine', 'umbrellaLineInverted', 'midBody', 'bodyATR%',
                               'body2ATR%', 'longATRCandle', 'body2ATR%2shadowImbalanceRatio',
                               'longCandleLight', 'longCandleBullishLight', 'longCandleBearishLight',
                               'longCandle', 'longCandleBullish', 'longCandleBearish',
                               'engulfingBullish', 'engulfingBearish', 'darkCloudCover',
                               'darkCloudCoverLight', 'piercingPattern', 'piercingPatternLight',
                               'onNeckPattern', 'inNeckPattern', 'thrustingPattern', 'star',
                               'eveningStar', 'morningStar']
        # __ filter the DataFrame to only include the candlestick columns __
        candlestick_data = df[candlestick_columns]
        # __ handle the candlestick analysis data update or insertion __
        self.handle_candle_analysis_candlestick_data(df=candlestick_data)

        # __ INDICATORS __
        # __ select columns related to indicators __
        indicator_columns = ['id', 'prevClose', 'TR', 'ATR', 'ATR%', 'MA50',
                             'MA100', 'MA200', 'MA200Distance%', 'RSI']
        # __ filter the DataFrame to only include the indicator columns __
        indicator_data = df[indicator_columns]
        # __ handle the indicator analysis data update or insertion __
        self.handle_candle_analysis_indicators_data(df=indicator_data)

        # __ TREND METHOD 1 __
        # __ select columns related to trend analysis __
        trend_method_1_columns = ['id', 'TrendAllTimeHigh', 'TrendDownFromAllTimeHigh',
                                  'TrendDaysFromAllTimeHigh', 'currMax', 'currMin', 'DownFromHigh',
                                  'UpFromLow', 'Trend', 'TrendChange', 'reversing', 'Session', 'block',
                                  'currMin_min', 'min_1', 'min_2', 'session_min_1', 'session_min_2',
                                  'currMax_max', 'max_1', 'max_2', 'session_max_1', 'session_max_2']
        # __ filter the DataFrame to only include the trend columns __
        trend_method_1_data = df[trend_method_1_columns]
        # __ handle the trend analysis data update or insertion __
        self.handle_candle_analysis_trend_method_1_data(df=trend_method_1_data)

    def handle_candle_analysis_candlestick_data(self, df: pd.DataFrame) -> None:
        """
        Handle the bulk update or insertion of candlestick analysis data into the database.

        :param df: DataFrame containing the candlestick analysis data to be processed.
        """
        # __ fetch the ticker_id from the ticker symbol __
        ticker = self.session.query(Ticker).filter(Ticker.symbol == self.symbol).first()
        if not ticker:
            print(f"        - Ticker with symbol '{self.symbol}' not found.")
            return

        ticker_id = ticker.id

        # __ fetch all the candle_data_day_id for the specified ticker_id __
        candle_data_ids = self.session.query(CandleDataDay.id).filter(CandleDataDay.ticker_id == ticker_id).all()
        candle_data_ids = [candle_data_id[0] for candle_data_id in candle_data_ids]  # Flatten the list of tuples

        # __ delete existing CandleAnalysisCandleStickDay records linked to those candle_data_day_ids __
        self.session.query(CandleAnalysisCandlestickDay).filter(CandleAnalysisCandlestickDay.candle_data_day_id.in_(candle_data_ids)).delete(synchronize_session=False)

        # __ create instances of CandleAnalysisDay from the DataFrame
        new_records = self.dataframe_to_candle_analysis_candlestick_day(candle_data=df)

        # __ perform bulk insert __
        self.session.bulk_save_objects(new_records)
        self.session.commit()

        print(f"        - Candlestick patterns - inserted {len(new_records)} new records.")

    def handle_candle_analysis_indicators_data(self, df: pd.DataFrame) -> None:
        """
        Handle the bulk update or insertion of indicators analysis data into the database.

        :param df: DataFrame containing the candlestick analysis data to be processed.
        """
        # __ fetch the ticker_id from the ticker symbol __
        ticker = self.session.query(Ticker).filter(Ticker.symbol == self.symbol).first()
        if not ticker:
            print(f"Ticker with symbol '{self.symbol}' not found.")
            return

        ticker_id = ticker.id

        # __ fetch all the candle_data_day_id for the specified ticker_id __
        candle_data_ids =  (self.session
                           .query(CandleDataDay.id)
                           .filter(CandleDataDay.ticker_id == ticker_id).all()
                           )
        candle_data_ids = [candle_data_id[0] for candle_data_id in candle_data_ids]  # Flatten the list of tuples

        # __ delete existing CandleAnalysisCandleStickDay records linked to those candle_data_day_ids __
        (self.session
         .query(CandleAnalysisIndicatorsDay)
         .filter(CandleAnalysisIndicatorsDay.candle_data_day_id.in_(candle_data_ids))
         .delete(synchronize_session=False)
         )

        # __ create instances of CandleAnalysisDay from the DataFrame
        new_records = self.dataframe_to_candle_analysis_indicators_day(candle_data=df)

        # __ perform bulk insert __
        self.session.bulk_save_objects(new_records)
        self.session.commit()

        print(f"        - Indicators - inserted {len(new_records)} new records.")

    def handle_candle_analysis_trend_method_1_data(self, df: pd.DataFrame) -> None:
        """
        Handle the bulk update or insertion of trend method 1 analysis data into the database.

        :param df: DataFrame containing the candlestick analysis data to be processed.
        """
        # __ fetch the ticker_id from the ticker symbol __
        ticker = self.session.query(Ticker).filter(Ticker.symbol == self.symbol).first()
        if not ticker:
            print(f"Ticker with symbol '{self.symbol}' not found.")
            return

        ticker_id = ticker.id

        # __ fetch all the candle_data_day_id for the specified ticker_id __
        candle_data_ids = (self.session
                           .query(CandleDataDay.id)
                           .filter(CandleDataDay.ticker_id == ticker_id).all()
                           )
        candle_data_ids = [candle_data_id[0] for candle_data_id in candle_data_ids]  # Flatten the list of tuples

        # __ delete existing CandleAnalysisCandleStickDay records linked to those candle_data_day_ids __
        (self.session
         .query(CandleAnalysisTrendMethod1Day)
         .filter(CandleAnalysisTrendMethod1Day.candle_data_day_id.in_(candle_data_ids))
         .delete(synchronize_session=False)
         )

        # __ create instances of CandleAnalysisDay from the DataFrame
        new_records = self.dataframe_to_candle_analysis_trend_method_1_day(candle_data=df)

        # __ perform bulk insert __
        self.session.bulk_save_objects(new_records)
        self.session.commit()

        print(f"        - Trend Method 1 - inserted {len(new_records)} new records.")

    @staticmethod
    def dataframe_to_candle_analysis_candlestick_day(candle_data: pd.DataFrame) -> List[CandleAnalysisCandlestickDay]:
        """
        Convert a DataFrame into a list of CandleAnalysisCandlestickDay instances for bulk insertion into the database.

        :param candle_data: DataFrame containing the data to be converted.
        :return: List of CandleAnalysisCandlestickDay instances.
        """
        return [
            CandleAnalysisCandlestickDay(
                candle_data_day_id=row['id'],  # Primary key linking to CandleDataDay
                bullish=row['bullish'],  # Bullish flag
                body_delta=row['bodyDelta'],  # Body delta
                shadow_delta=row['shadowDelta'],  # Shadow delta
                percent_body=row['%body'],  # Body percentage
                percent_upper_shadow=row['%upperShadow'],  # Upper shadow percentage
                percent_lower_shadow=row['%lowerShadow'],  # Lower shadow percentage
                long_body=row['longBody'],  # Long body flag
                shadow_imbalance=row['shadowImbalance'],  # Shadow imbalance
                shaven_head=row['shavenHead'],  # Shaven head flag
                shaven_bottom=row['shavenBottom'],  # Shaven bottom flag
                doji=row['doji'],  # Doji flag
                spinning_top=row['spinningTop'],  # Spinning top flag
                umbrella_line=row['umbrellaLine'],  # Umbrella line flag
                umbrella_line_inverted=row['umbrellaLineInverted'],  # Inverted umbrella line flag
                mid_body=row['midBody'],  # Mid-body value
                body_atr_percent=row['bodyATR%'],  # Body ATR percentage
                body2atr_percent=row['body2ATR%'],  # Body 2ATR percentage
                long_atr_candle=row['longATRCandle'],  # Long ATR candle flag
                body2atr2_shadow_imbalance_ratio=row['body2ATR%2shadowImbalanceRatio'],  # Ratio of body 2ATR percentage to shadow imbalance
                long_candle_light=row['longCandleLight'],  # Long candle light flag
                long_candle_bullish_light=row['longCandleBullishLight'],  # Long candle bullish light flag
                long_candle_bearish_light=row['longCandleBearishLight'],  # Long candle bearish light flag
                long_candle=row['longCandle'],  # Long candle flag
                long_candle_bullish=row['longCandleBullish'],  # Long candle bullish flag
                long_candle_bearish=row['longCandleBearish'],  # Long candle bearish flag
                engulfing_bullish=row['engulfingBullish'],  # Engulfing bullish flag
                engulfing_bearish=row['engulfingBearish'],  # Engulfing bearish flag
                dark_cloud_cover=row['darkCloudCover'],  # Dark cloud cover pattern
                dark_cloud_cover_light=row['darkCloudCoverLight'],  # Light dark cloud cover pattern
                piercing_pattern=row['piercingPattern'],  # Piercing pattern
                piercing_pattern_light=row['piercingPatternLight'],  # Light piercing pattern
                on_neck_pattern=row['onNeckPattern'],  # On neck pattern
                in_neck_pattern=row['inNeckPattern'],  # In neck pattern
                thrusting_pattern=row['thrustingPattern'],  # Thrusting pattern
                star=row['star'],  # Star pattern
                evening_star=row['eveningStar'],  # Evening star pattern
                morning_star=row['morningStar'],  # Morning star pattern
            )
            for _, row in candle_data.iterrows()
        ]

    @staticmethod
    def dataframe_to_candle_analysis_indicators_day(candle_data: pd.DataFrame) -> List[CandleAnalysisIndicatorsDay]:
        """
        Convert a DataFrame into a list of CandleAnalysisIndicatorsDay instances for bulk insertion into the database.

        :param candle_data: DataFrame containing the data to be converted.
        :return: List of CandleAnalysisIndicatorsDay instances.
        """
        return [
            CandleAnalysisIndicatorsDay(
                candle_data_day_id=row['id'],  # Primary key linking to CandleDataDay
                prev_close=row['prevClose'],  # Previous close price
                tr=row['TR'],  # True Range
                atr=row['ATR'],  # Average True Range
                atr_percent=row['ATR%'],  # ATR as a percentage
                ma50=row['MA50'],  # 50-day moving average
                ma100=row['MA100'],  # 100-day moving average
                ma200=row['MA200'],  # 200-day moving average
                ma200_distance_percent=row['MA200Distance%'],  # Distance from 200-day moving average as a percentage
                rsi=row['RSI']  # Relative Strength Index
            )
            for _, row in candle_data.iterrows()
        ]

    @staticmethod
    def dataframe_to_candle_analysis_trend_method_1_day(candle_data: pd.DataFrame) -> List[CandleAnalysisTrendMethod1Day]:
        """
        Convert a DataFrame into a list of CandleAnalysisTrendMethod1Day instances for bulk insertion into the database.

        :param candle_data: DataFrame containing the data to be converted.
        :return: List of CandleAnalysisTrendMethod1Day instances.
        """
        return [
            CandleAnalysisTrendMethod1Day(
                candle_data_day_id=row['id'],
                trend_all_time_high=row['TrendAllTimeHigh'],
                trend_down_from_all_time_high=row['TrendDownFromAllTimeHigh'],
                trend_days_from_all_time_high=row['TrendDaysFromAllTimeHigh'],
                curr_max=row['currMax'],
                curr_min=row['currMin'],
                down_from_high=row['DownFromHigh'],
                up_from_low=row['UpFromLow'],
                trend=row['Trend'],
                trend_change=row['TrendChange'],
                reversing=row['reversing'],
                session=row['Session'],
                block=row['block'],
                curr_min_min=row['currMin_min'],
                min_1=row['min_1'],
                min_2=row['min_2'],
                session_min_1=row['session_min_1'],
                session_min_2=row['session_min_2'],
                curr_max_max=row['currMax_max'],
                max_1=row['max_1'],
                max_2=row['max_2'],
                session_max_1=row['session_max_1'],
                session_max_2=row['session_max_2'],
                last_update=datetime.now()
            )
            for _, row in candle_data.iterrows()
        ]

    def add_candlestick_info(self, candle_data: pd.DataFrame) -> pd.DataFrame:
        """
        Add additional properties to the candlestick data for analysis.

        :param candle_data:     DataFrame containing the candlestick data.
        :return:                DataFrame containing the candlestick data with additional properties.
        """
        # get current time to measure the time it takes to run the analysis
        start_time = time()

        # __ add one candle properties __
        candle_data = self.add_one_candle_properties(candle_data=candle_data)
        one_candle_properties_time = time()

        # __ add candles patterns __
        candle_data = self.add_two_candles_patterns(candle_data=candle_data)
        two_candles_patterns_time = time()

        # _____ Selection _____
        # candle_data[['Date', 'ATR%', 'bodyATR%', 'body2ATR%', 'longATRCandle', '%body', 'shadowImbalance', 'body2ATR%2shadowImbalanceRatio', 'longCandle', 'longCandleBullish', 'longCandleBearish']]

        # __ print the time it took to run each analysis __
        # print(f"{'      - One Candle Properties:'.ljust(25)} {round(one_candle_properties_time - start_time, 3)} sec")
        # print(f"{'      - Two Candles Patterns:'.ljust(25)} {round(two_candles_patterns_time - one_candle_properties_time, 3)} sec")
        print(f"        - One Candle Properties: {round(one_candle_properties_time - start_time, 3)} sec")
        print(f"        - Two Candles Patterns: {round(two_candles_patterns_time - one_candle_properties_time, 3)} sec")

        return candle_data

    """ Add Candlestick Patterns"""
    @ staticmethod
    def add_one_candle_properties(candle_data: pd.DataFrame) -> pd.DataFrame:
        """
        Add properties to the candlestick data for each individual candle.

        ##### one Candle Properties #####

        :bullish                --> If Close is strictly greater than Open - The color of the candle (red or green)
        :bodyDelta              --> The extension of the body (Close - Open) (absolute value)
        :shadowDelta            --> The extension of the full candle (High - Low)
        :%body                  --> The ratio between the body extension and the full candle extension in % (bodyDelta/shadowDelta)
        :%upperShadow           --> The ratio between the upper shadow and the full candle extension in %
        :%lowerShadow           --> The ratio between the lower shadow and the full candle extension in %
        :longBody               --> %body > 70% - A candle that is constituted mainly by the body
        :shadowImbalance        --> It's the ratio of the longest shadow (upper or lower) and the shortest shadow.
                                It's range goes from 1 (perfectly balanced) to infinity (completely imbalanced)
        :shavenHead             --> If the candle has no upper shadow (%upperShadow = 0)
        :shavenBottom           --> If the candle has no lower shadow (%lowerShadow = 0)
        :doji                   --> If the candle has no body (%body = 0)
        :spinningTop            --> If the % of the body is below 30%
        :umbrellaLine           --> If the candle is a spinningTop and the upperShadow is less than half the body
                                    --> from here we have Hammer and Hanging Man
        :umbrellaLineInverted   --> If the candle is a spinningTop and the lowerShadow is less than half the body
                                    --> from here we have the Shooting Star and the Inverted Hammer
        :midBody                --> Price of the mid-point between Close and Open
        :hammer                 --> If the candle is a Hammer and the trend is down and the candle is not reversing


        ##### one Candle Properties (volatility needed) #####

        :bodyATR%                           --> The extension of the body divided by the Open price in %
        :body2ATR%                          --> bodyATR% / ATR% * 100
                                                It gives the sensibility of the body volatility with respect to the average volatility (ATR)
                                                If this number is greater than 50%, it means it's a long volatility candle
        :longATRCandle                      --> body2ATR% > 50% - Long volatility body
        :body2ATR%2shadowImbalanceRatio     --> body2ATR% / sqrt(shadowImbalance).
                                                The higher the body volatility the higher this ratio
                                                The lowest the shadow imbalance the higher this ratio.
                                                Higher values of this value means that the candle is a long bullish or long bearish session
        :trigger                            --> %body^2/100 * body2ATR%2shadowImbalanceRatio / 100

        :longCandleLight            --> If it's a longATRCandle and a longBody
        :longCandleBullishLight     --> If it's a longCandleLight
                                        and the candle Close is greater or equal than the max between the Open and the Close
                                        of the last two session (including this)
        :longCandleBearishLight     --> If it's a longCandleLight
                                        and the candle Close is less or equal than the min between the Open and the Close
                                        of the last two session (including this)

        :longCandle                 --> If it's a longATRCandle (long volatility body)
                                        AND (
                                            %body > 90%
                                            OR (%body > 80% AND body2ATR%2shadowImbalanceRatio > 10)
                                            OR (%body > 70% AND body2ATR%2shadowImbalanceRatio > 50))
                                        )
        :longCandleBullish          --> If it's a longCandle
                                        and the candle Close is greater or equal than the max between the Open and the Close
                                        of the last two session (including this)
        :longCandleBearish          --> If it's a longCandle
                                        and the candle Close is less or equal than the min between the Open and the Close
                                        of the last two session (including this)

        """

        # _____ one Candle Properties _____
        candle_data['bullish'] = candle_data['Close'] > candle_data['Open']
        candle_data['bodyDelta'] = abs(candle_data['Close'] - candle_data['Open'])
        candle_data['shadowDelta'] = candle_data['High'] - candle_data['Low']
        candle_data['%body'] = candle_data['bodyDelta'] / candle_data['shadowDelta'] * 100
        candle_data['%upperShadow'] = 100 * (candle_data['High'] - candle_data[['Close', 'Open']].max(axis=1)) / candle_data['shadowDelta']
        candle_data['%lowerShadow'] = 100 * (candle_data[['Close', 'Open']].min(axis=1) - candle_data['Low']) / candle_data['shadowDelta']
        candle_data['longBody'] = candle_data['%body'] > 70
        candle_data['shadowImbalance'] = candle_data[['%upperShadow', '%lowerShadow']].max(axis=1) / candle_data[['%upperShadow', '%lowerShadow']].min(axis=1)
        candle_data['shavenHead'] = candle_data['%upperShadow'] == 0
        candle_data['shavenBottom'] = candle_data['%lowerShadow'] == 0
        candle_data['doji'] = candle_data['%body'] == 0
        candle_data['spinningTop'] = candle_data['%body'] < 30
        # candle_data['umbrellaLine'] = (candle_data['spinningTop']) & (candle_data['%upperShadow'] * 2 < candle_data['%body'])
        candle_data['umbrellaLine'] = (candle_data['spinningTop']) & (candle_data['%lowerShadow'] > 60)
        candle_data['umbrellaLineInverted'] = (candle_data['spinningTop']) & (candle_data['%lowerShadow'] * 2 < candle_data['%body'])
        candle_data['midBody'] = (candle_data['Close'] + candle_data['Open']) / 2
        # candle_data['hammer'] = candle_data['umbrellaLine'] & (candle_data['Trend'] == 'down') & (candle_data['reversing'] == False)

        # _____ one Candle Properties (volatility needed) _____
        candle_data['bodyATR%'] = candle_data['bodyDelta'] / candle_data['Open'] * 100
        candle_data['body2ATR%'] = candle_data['bodyATR%'] / candle_data['ATR%'] * 100
        candle_data['longATRCandle'] = candle_data['body2ATR%'] > 50
        candle_data['body2ATR%2shadowImbalanceRatio'] = candle_data['body2ATR%'] / np.sqrt(candle_data['shadowImbalance'])
        # candle_data['trigger'] = candle_data['%body'].pow(2)/100 * candle_data['body2ATR%2shadowImbalanceRatio'] / 100
        candle_data['longCandleLight'] = candle_data['longATRCandle'] & candle_data['longBody']
        candle_data['longCandleBullishLight'] = candle_data['longCandleLight'] & (candle_data['Close'] >= candle_data[['Close', 'Open']].rolling(2).max().max(axis=1))
        candle_data['longCandleBearishLight'] = candle_data['longCandleLight'] & (candle_data['Close'] <= candle_data[['Close', 'Open']].rolling(2).min().min(axis=1))

        candle_data['longCandle'] = candle_data['longATRCandle'] & ((candle_data['%body'] > 90) | ((candle_data['%body'] > 80) & (candle_data['body2ATR%2shadowImbalanceRatio'] > 10)) | ((candle_data['%body'] > 70) & (candle_data['body2ATR%2shadowImbalanceRatio'] > 50)))
        candle_data['longCandleBullish'] = candle_data['longCandle'] & (candle_data['Close'] >= candle_data[['Close', 'Open']].rolling(2).max().max(axis=1))
        candle_data['longCandleBearish'] = candle_data['longCandle'] & (candle_data['Close'] <= candle_data[['Close', 'Open']].rolling(2).min().min(axis=1))

        return candle_data

    def add_two_candles_patterns(self, candle_data: pd.DataFrame) -> pd.DataFrame:
        """
        Add properties to the candlestick data for two consecutive candles.

        :param candle_data: DataFrame containing the candlestick data.
        :return: DataFrame containing the candlestick data with additional properties.
        """
        candle_data = self.add_engulfing_pattern(candle_data)
        candle_data = self.add_semi_engulfing_patterns(candle_data)
        candle_data = self.add_stars_patterns(candle_data)
        return candle_data

    @staticmethod
    def add_engulfing_pattern(candle_data: pd.DataFrame) -> pd.DataFrame:
        """
        Add engulfing patterns to the candlestick data:

        :engulfingBullish   --> If the candle is green (bullish), it's not a spinningTop, the previous candle is red or doji (not bullish)
                                and the Open is less or equal to the previous close and the Close is greater or equal to the previous Open

        :engulfingBearish   --> If the candle is red (not bullish), it's not a spinningTop, the previous candle is green or doji (bullish)
                                and the Open is greater or equal to the previous close and the Close is less or equal to the previous Open

        :param candle_data: DataFrame containing the candlestick data.
        :return: DataFrame containing the candlestick data with engulfing patterns added.
        """

        # Shifting to get previous row values
        prev_close = candle_data['Close'].shift(1)
        prev_open = candle_data['Open'].shift(1)
        prev_bullish = candle_data['bullish'].shift(1)
        prev_doji = candle_data['doji'].shift(1)

        # Vectorized conditions for engulfing bullish pattern
        is_engulfing_bullish = (
                (candle_data['bullish']) &
                (~candle_data['spinningTop']) &
                (pd.notna(prev_bullish) & (prev_bullish == False)) &
                (candle_data['Open'] <= prev_close) &
                (candle_data['Close'] >= prev_open)
        )

        # Vectorized conditions for engulfing bearish pattern
        is_engulfing_bearish = (
                (~candle_data['bullish']) &
                (~candle_data['spinningTop']) &
                ((pd.notna(prev_bullish) & prev_bullish) | (pd.notna(prev_doji) & prev_doji)) &
                (candle_data['Open'] >= prev_close) &
                (candle_data['Close'] <= prev_open)
        )

        # Assign the patterns to new columns
        candle_data['engulfingBullish'] = is_engulfing_bullish
        candle_data['engulfingBearish'] = is_engulfing_bearish

        return candle_data

    @staticmethod
    def add_semi_engulfing_patterns(candle_data: pd.DataFrame) -> pd.DataFrame:
        """
        Add semi-engulfing patterns to the candlestick data:
        :darkCloudCover         --> If the previous candle is a long bullish light candle
                                    and the current candle opens above the previous high
                                    and closes between the previous midBody and the previous low

        :darkCloudCoverLight    --> If the previous candle is a long bullish light candle
                                    and the current candle opens above the previous close
                                    and closes between the previous midBody and the previous low

        :piercingPattern        --> If the previous candle is a long bearish light candle
                                    and the current candle opens below the previous low
                                    and closes above the previous midBody

        :piercingPatternLight   --> If the previous candle is a long bearish light candle
                                    and the current candle opens below the previous close
                                    and closes between the previous midBody and the previous open

        :onNeckPattern          --> If the previous candle is a long bearish light candle
                                    and the current candle opens below the previous low
                                    and the current candle is not a long candle or a long light candle
                                    and the current candle closes between the previous low and the mean of the previous low and close

        :inNeckPattern          --> If the previous candle is a long bearish light candle
                                    and the current candle opens below the previous low
                                    and the current candle is not a long candle or a long light candle
                                    and the current candle closes between the previous close and the mean of the previous close and midBody

        :thrustingPattern       --> If the previous candle is a long bearish light candle
                                    and the current candle opens below the previous low
                                    and the current candle is a long light candle
                                    and the current candle closes between the previous close and the previous midBody

        :param candle_data: DataFrame containing the candlestick data.
        :return: DataFrame containing the candlestick data with semi-engulfing patterns added.
        """
        # Shifted columns to access previous candle data
        prev_close = candle_data['Close'].shift(1)
        prev_open = candle_data['Open'].shift(1)
        prev_high = candle_data['High'].shift(1)
        prev_low = candle_data['Low'].shift(1)
        prev_mid_body = candle_data['midBody'].shift(1)
        prev_long_bullish = candle_data['longCandleBullishLight'].shift(1)
        prev_long_bearish = candle_data['longCandleBearishLight'].shift(1)

        # darkCloudCover
        is_dark_cloud_cover = (
                prev_long_bullish &
                (candle_data['Open'] > prev_high) &
                (candle_data['Close'] < prev_mid_body) &
                (candle_data['Close'] > prev_open)
        )

        # darkCloudCoverLight
        is_dark_cloud_cover_light = (
                prev_long_bullish &
                (candle_data['Open'] > prev_close) &
                (candle_data['Close'] < prev_mid_body) &
                (candle_data['Close'] > prev_open) &
                ~is_dark_cloud_cover  # Ensure it's not already a darkCloudCover
        )

        # piercingPattern
        is_piercing_pattern = (
                prev_long_bearish &
                (candle_data['Open'] < prev_low) &
                (candle_data['Close'] > prev_mid_body)
        )

        # onNeckPattern
        is_on_neck_pattern = (
                prev_long_bearish &
                (candle_data['Open'] < prev_low) &
                ~candle_data['longCandleLight'] &
                ~candle_data['longCandle'] &
                (candle_data['Close'] > prev_low) &
                (candle_data['Close'] < (prev_low + prev_close) / 2) &
                ~is_piercing_pattern
        )

        # inNeckPattern
        is_in_neck_pattern = (
                prev_long_bearish &
                (candle_data['Open'] < prev_low) &
                ~candle_data['longCandleLight'] &
                ~candle_data['longCandle'] &
                (candle_data['Close'] > prev_close) &
                (candle_data['Close'] < (prev_close + prev_mid_body) / 2) &
                ~is_piercing_pattern &
                ~is_on_neck_pattern
        )

        # thrustingPattern
        is_thrusting_pattern = (
                prev_long_bearish &
                (candle_data['Open'] < prev_low) &
                candle_data['longCandleLight'] &
                (candle_data['Close'] > prev_close) &
                (candle_data['Close'] < prev_mid_body) &
                ~is_piercing_pattern &
                ~is_on_neck_pattern &
                ~is_in_neck_pattern
        )

        # piercingPatternLight
        is_piercing_pattern_light = (
                prev_long_bearish &
                (candle_data['Open'] < prev_close) &
                ~(candle_data['Open'] < prev_low) &
                (candle_data['Close'] > prev_mid_body) &
                (candle_data['Close'] < prev_open)
        )

        # Assign the patterns to new columns
        candle_data['darkCloudCover'] = is_dark_cloud_cover
        candle_data['darkCloudCoverLight'] = is_dark_cloud_cover_light
        candle_data['piercingPattern'] = is_piercing_pattern
        candle_data['piercingPatternLight'] = is_piercing_pattern_light
        candle_data['onNeckPattern'] = is_on_neck_pattern
        candle_data['inNeckPattern'] = is_in_neck_pattern
        candle_data['thrustingPattern'] = is_thrusting_pattern

        return candle_data

    @staticmethod
    def add_stars_patterns(candle_data: pd.DataFrame) -> pd.DataFrame:
        """Add star patterns to the candlestick data:

        :star           --> If the previous candle is a long bullish light candle
                            and the current candle is a spinningTop
                            and the current candle opens above the previous close
                            and the current candle closes above the previous close
                            OR
                            If the previous candle is a long bearish light candle
                            and the current candle is a spinningTop
                            and the current candle opens below the previous close
                            and the current candle closes below the previous close

        :eveningStar    --> If the previous candle is a star
                            and the candle before the previous candle is a long bullish light candle
                            and the current candle closes below the midBody of the candle before the previous candle

        :morningStar    --> If the previous candle is a star
                            and the candle before the previous candle is a long bearish light candle
                            and the current candle closes above the midBody of the candle before the previous candle

        :param candle_data: DataFrame containing the candlestick data.
        :return: DataFrame containing the candlestick data with star patterns added.
        """

        # __ shifted columns to access previous candle data __
        prev_close = candle_data['Close'].shift(1)
        prev_long_bullish = candle_data['longCandleBullishLight'].shift(1)
        prev_long_bearish = candle_data['longCandleBearishLight'].shift(1)
        prev_mid_body_2 = candle_data['midBody'].shift(2)

        # Star pattern conditions
        is_star = (
                (
                        prev_long_bullish &
                        candle_data['spinningTop'] &
                        (candle_data['Open'] > prev_close) &
                        (candle_data['Close'] > prev_close)
                )
                |
                (
                        prev_long_bearish &
                        candle_data['spinningTop'] &
                        (candle_data['Open'] < prev_close) &
                        (candle_data['Close'] < prev_close)
                )
        )

        candle_data['star'] = is_star

        # __ shifted 'star' column for the second part of the pattern
        prev_star = candle_data['star'].shift(1)

        # __ evening Star pattern conditions
        is_evening_star = (
                prev_star &
                prev_long_bullish.shift(1) &
                (candle_data['Close'] <= prev_mid_body_2)
        )

        # Morning Star pattern conditions
        is_morning_star = (
                prev_star &
                prev_long_bearish.shift(1) &
                (candle_data['Close'] >= prev_mid_body_2)
        )

        # Assign the patterns to new columns
        candle_data['eveningStar'] = is_evening_star
        candle_data['morningStar'] = is_morning_star

        return candle_data

    """ Add volatility indicators"""
    def add_volatility(self, candle_data: pd.DataFrame) -> pd.DataFrame:
        """
        Add volatility indicators to the candlestick data:

        :TR                 --> True range of each candle
        :ATR                --> Average True Range (14 samples window)
        :ATR%               --> Average True Range divided by the Close price


        :param candle_data: DataFrame containing the candlestick data.
        :return: DataFrame containing the candlestick data with volatility indicators added.
        """

        candle_data = self.add_true_range(candle_data)
        candle_data['ATR'] = candle_data['TR'].rolling(window=14).mean()
        candle_data['ATR%'] = candle_data['ATR'] / candle_data['Close'] * 100
        return candle_data

    @staticmethod
    def add_true_range(candle_data: pd.DataFrame) -> pd.DataFrame:

        if candle_data.shape[0] == 0:
            return candle_data

        candle_data['prevClose'] = candle_data['Close'].shift(1)
        candle_data['hl'] = candle_data['High'] - candle_data['Low']
        candle_data['hc'] = abs(candle_data['High'] - candle_data['prevClose'])
        candle_data['lc'] = abs(candle_data['Low'] - candle_data['prevClose'])

        candle_data['TR'] = candle_data[['hl', 'hc', 'lc']].max(axis=1)
        return candle_data.drop(columns=['hl', 'hc', 'lc'])

    """ Add moving averages """
    @staticmethod
    def add_moving_averages(candle_data: pd.DataFrame) -> pd.DataFrame:
        """
        Add moving averages to the candlestick data:

        :MA50               --> The 50 samples rolling moving average of the close prices
        :MA100              --> The 100 samples rolling moving average of the close prices
        :MA200              --> The 200 samples rolling moving average of the close prices

        :param candle_data: DataFrame containing the candlestick data.
        :return: DataFrame containing the candlestick data with moving averages added.
        """
        candle_data['MA50'] = candle_data['Close'].rolling(window=50).mean()
        candle_data['MA100'] = candle_data['Close'].rolling(window=100).mean()
        candle_data['MA200'] = candle_data['Close'].rolling(window=200).mean()
        candle_data['MA200Distance%'] = (candle_data['Close'] - candle_data['MA200']) / candle_data['MA200'] * 100
        return candle_data

    """ Add Oscillators"""
    @staticmethod
    def add_relative_strength_index(candle_data: pd.DataFrame, period=14) -> pd.DataFrame:
        """
        Add the Relative Strength Index (RSI) to the candlestick data.

        :RSI        --> Relative Strength Index (default: 14 samples window)

        :param candle_data:     DataFrame containing the candlestick data.
        :param period:          The period for the RSI calculation (default: 14).
        :return:                DataFrame containing the candlestick data with the RSI added.
        """
        candle_data['RSI'] = ta.rsi(candle_data['Close'], length=period)
        return candle_data

    """ Add trend """
    @staticmethod
    def add_trend(candles, sens_up=10, sens_down=9):

        curr_min = candles['Low'].iloc[0]
        curr_max = candles['High'].iloc[0]
        all_time_high = candles['High'].iloc[0]
        all_time_high_index = 0
        trend = ''

        for index, candle in candles.iterrows():

            up_from_low = (candle['High'] - curr_min) / curr_min * 100
            down_from_high = (curr_max - candle['Low']) / curr_max * 100

            if up_from_low >= sens_up and trend != 'up':
                trend = 'up'
                curr_max = candle['High']
            elif down_from_high >= sens_down and trend != 'down':
                trend = 'down'
                curr_min = candle['Low']

            if candle['High'] > curr_max:
                curr_max = candle['High']

            if candle['High'] > all_time_high:
                all_time_high = candle['High']
                all_time_high_index = index

            if candle['Low'] < curr_min:
                curr_min = candle['Low']

            down_from_high = (curr_max - candle['Close']) / curr_max * 100
            up_from_low = (candle['Close'] - curr_min) / curr_min * 100
            down_from_all_time_high = (all_time_high - candle['Close']) / all_time_high * 100
            days_from_all_time_high = index - all_time_high_index

            candles.at[index, 'TrendAllTimeHigh'] = all_time_high
            candles.at[index, 'TrendDownFromAllTimeHigh'] = down_from_all_time_high
            candles.at[index, 'TrendDaysFromAllTimeHigh'] = days_from_all_time_high
            candles.at[index, 'currMax'] = curr_max
            candles.at[index, 'currMin'] = curr_min
            candles.at[index, 'DownFromHigh'] = down_from_high
            candles.at[index, 'UpFromLow'] = up_from_low
            candles.at[index, 'Trend'] = trend

            if trend == 'up' and down_from_high > up_from_low:
                candles.at[index, 'reversing'] = True
            elif trend == 'down' and up_from_low > down_from_high:
                candles.at[index, 'reversing'] = True
            else:
                candles.at[index, 'reversing'] = False

        return candles

    @staticmethod
    def add_trend_atr(candles):

        curr_min = candles['Low'].iloc[0]
        curr_max = candles['High'].iloc[0]
        all_time_high = candles['High'].iloc[0]
        all_time_high_index = 0
        trend = ''

        for index, candle in candles.iterrows():
            sens_up = candle["ATR%"] * 4
            sens_down = 100 * (1 - 100 / (100 + sens_up))
            trend_change = False

            up_from_low = (candle['High'] - curr_min) / curr_min * 100
            down_from_high = (curr_max - candle['Low']) / curr_max * 100

            if up_from_low >= sens_up and trend != 'up':
                trend = 'up'
                curr_max = candle['High']
                trend_change = True
            elif down_from_high >= sens_down and trend != 'down':
                trend = 'down'
                curr_min = candle['Low']
                trend_change = True

            if candle['High'] > curr_max:
                curr_max = candle['High']

            if candle['High'] > all_time_high:
                all_time_high = candle['High']
                all_time_high_index = index

            if candle['Low'] < curr_min:
                curr_min = candle['Low']

            down_from_high = (curr_max - candle['Close']) / curr_max * 100
            up_from_low = (candle['Close'] - curr_min) / curr_min * 100
            down_from_all_time_high = (all_time_high - candle['Close']) / all_time_high * 100
            days_from_all_time_high = index - all_time_high_index

            candles.at[index, 'SensUp'] = sens_up
            candles.at[index, 'SensDown'] = sens_down
            candles.at[index, 'TrendAllTimeHigh'] = all_time_high
            candles.at[index, 'TrendDownFromAllTimeHigh'] = down_from_all_time_high
            candles.at[index, 'TrendDaysFromAllTimeHigh'] = days_from_all_time_high
            candles.at[index, 'currMax'] = curr_max
            candles.at[index, 'currMin'] = curr_min
            candles.at[index, 'DownFromHigh'] = down_from_high
            candles.at[index, 'UpFromLow'] = up_from_low
            candles.at[index, 'Trend'] = trend
            candles.at[index, 'TrendChange'] = trend_change

            if trend == 'up' and down_from_high > up_from_low:
                candles.at[index, 'reversing'] = True
            elif trend == 'down' and up_from_low > down_from_high:
                candles.at[index, 'reversing'] = True
            else:
                candles.at[index, 'reversing'] = False

        return candles

    def add_extreme_points(self, candles: pd.DataFrame) -> pd.DataFrame:
        # Add a column to identify trend blocks
        candles['block'] = (candles['Trend'] != candles['Trend'].shift()).cumsum()
        candles = self.add_last_minimums(candles)
        candles = self.add_last_maximums(candles)
        return candles

    @staticmethod
    def add_last_minimums(candles) -> pd.DataFrame:
        # Filter for 'down' trend
        df_down = candles[candles['Trend'] == 'down']

        # Group by block and find the minimum curr_min for each block
        grouped = df_down.groupby('block').agg({'currMin': 'min'}).reset_index()

        # Merge to find the date where the 'Low' value matches the 'curr_min' for each block
        merged = df_down.merge(grouped, on='block', suffixes=('', '_min'))

        # Filter the rows where 'Low' matches 'curr_min_min'
        filtered = merged[merged['Low'] == merged['currMin_min']][["Session", "block", "currMin_min"]]
        # __ drop duplicates on 'block' to make sure there is only one block - keep only first entry__
        filtered = filtered.drop_duplicates(subset='block', keep='first')
        filtered['min_1'] = filtered['currMin_min'].rolling(window=3, min_periods=1).min()
        filtered['min_2'] = filtered['currMin_min'].rolling(window=3, min_periods=1).apply(lambda x: sorted(x)[1] if len(x) > 1 else np.nan)

        # Calculate the corresponding dates for min_1 and min_2
        filtered['session_min_1'] = filtered.apply(lambda row: filtered[(filtered['block'] <= row['block']) & (filtered['currMin_min'] == row['min_1'])]["Session"].max(), axis=1)
        filtered['session_min_2'] = filtered.apply(lambda row: filtered[(filtered['block'] <= row['block']) & (filtered['currMin_min'] == row['min_2'])]["Session"].max(), axis=1)

        # Duplicate rows and increment block
        duplicated_df = filtered.copy()
        duplicated_df['block'] += 1

        duplicated_df_2 = filtered.copy()
        duplicated_df_2['block'] += 2

        # Combine duplicated dataframes
        final_df = pd.concat([duplicated_df, duplicated_df_2]).sort_values(by='block').reset_index(drop=True)

        # Merge the final dataframe with the original dataframe
        candles = candles.merge(final_df[['block', 'currMin_min', 'min_1', 'min_2', 'session_min_1', 'session_min_2']], on='block', how='left')

        return candles

    @staticmethod
    def add_last_maximums(candles) -> pd.DataFrame:
        # Filter for 'down' trend
        df_up = candles[candles['Trend'] == 'up']

        # Group by block and find the minimum curr_min for each block
        grouped = df_up.groupby('block').agg({'currMax': 'max'}).reset_index()

        # Merge to find the date where the 'High' value matches the 'curr_max' for each block
        merged = df_up.merge(grouped, on='block', suffixes=('', '_max'))

        # Filter the rows where 'High' matches 'curr_max_max'
        filtered = merged[merged['High'] == merged['currMax_max']][["Session", "block", "currMax_max"]]
        # __ drop duplicates on 'block' to make sure there is only one block - keep only first entry__
        filtered = filtered.drop_duplicates(subset='block', keep='first')
        filtered['max_1'] = filtered['currMax_max'].rolling(window=3, min_periods=1).max()
        filtered['max_2'] = filtered['currMax_max'].rolling(window=3, min_periods=1).apply(lambda x: sorted(x)[-2] if len(x) > 1 else np.nan)

        # Calculate the corresponding dates for max_1 and max_2
        filtered['session_max_1'] = filtered.apply(lambda row: filtered[(filtered['block'] <= row['block']) & (filtered['currMax_max'] == row['max_1'])]["Session"].max(), axis=1)
        filtered['session_max_2'] = filtered.apply(lambda row: filtered[(filtered['block'] <= row['block']) & (filtered['currMax_max'] == row['max_2'])]["Session"].max(), axis=1)

        # Duplicate rows and increment block
        duplicated_df = filtered.copy()
        duplicated_df['block'] += 1

        duplicated_df_2 = filtered.copy()
        duplicated_df_2['block'] += 2

        # Combine duplicated dataframes
        final_df = pd.concat([duplicated_df, duplicated_df_2]).sort_values(by='block').reset_index(drop=True)

        # Merge the final dataframe with the original dataframe
        candles = candles.merge(final_df[['block', 'currMax_max', 'max_1', 'max_2', 'session_max_1', 'session_max_2']], on='block', how='left')

        return candles

    @staticmethod
    def add_local_max(candles: pd.DataFrame, window: int) -> pd.DataFrame:
        """
        Add a column 'local_max' to the DataFrame indicating local maxima.

        Parameters:
        candles (pd.DataFrame): DataFrame with a 'high' column
        window (int): Size of the rolling window (must be odd)

        Returns:
        pd.DataFrame: DataFrame with an additional 'local_max' column
        """
        # Ensure the window size is odd, if not, round up to the next odd number
        if window % 2 == 0:
            window += 1

        # Apply a rolling window to find local maxima
        candles['rolling_max'] = candles['High'].rolling(window=window, center=True).max()

        # Identify local maxima where the original high value is equal to the rolling max
        candles[f'local_max_{window}'] = candles['High'] == candles['rolling_max']

        # Shift the rolling max results backwards to avoid using future information
        candles[f'shifted_local_max_{window}'] = candles[f'local_max_{window}'].shift(window // 2)

        # Drop the temporary rolling_max column
        candles = candles.drop(columns=['rolling_max'])

        return candles

    @staticmethod
    def add_local_min(candles: pd.DataFrame, window: int) -> pd.DataFrame:
        """
        Add a column 'local_min' to the DataFrame indicating local minima.

        Parameters:
        candles (pd.DataFrame): DataFrame with a 'low' column
        window (int): Size of the rolling window (must be odd)

        Returns:
        pd.DataFrame: DataFrame with an additional 'local_min' column
        """
        # Ensure the window size is odd, if not, round up to the next odd number
        if window % 2 == 0:
            window += 1

        # Apply a rolling window to find local minima
        candles['rolling_min'] = candles['Low'].rolling(window=window, center=True).min()

        # Identify local minima where the original low value is equal to the rolling min
        candles[f'local_min_{window}'] = candles['Low'] == candles['rolling_min']

        # Shift the rolling min results backwards to avoid using future information
        candles[f'shifted_local_min_{window}'] = candles[f'local_min_{window}'].shift(window // 2)

        # Drop the temporary rolling_min column
        candles = candles.drop(columns=['rolling_min'])

        return candles



