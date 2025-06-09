import pandas as pd
from datetime import datetime, timedelta
from dataclasses import dataclass
from sqlalchemy import text
from sqlalchemy.orm import session as sess
from src.stock.src.db.database import session_local


def apply_filter(func):
    def wrapper(self, *args, **kwargs):
        result = func(self, *args, **kwargs)
        filter_func = object.__getattribute__(self, "__filter__")
        if isinstance(result, list) and all(isinstance(x, str) for x in result):
            return filter_func(result)
        return result
    return wrapper


@dataclass
class QueriesDB:
    session: sess.Session
    remove_existing_db_tickers: bool = False
    remove_not_existing_db_tickers: bool = False
    remove_failed_candle_download_tickers: bool = False
    remove_yfinance_error_tickers: bool = False

    def __filter__(self, tickers: list[str]) -> list[str]:
        if self.remove_existing_db_tickers:
            tickers = self.__remove_tickers_present_in_db(tickers)
        if self.remove_not_existing_db_tickers:
            tickers = self.__remove_tickers_not_present_in_db(tickers)
        if self.remove_failed_candle_download_tickers:
            tickers = self.__remove_failed_candle_download_tickers(tickers)
        if self.remove_yfinance_error_tickers:
            tickers = self.__remove_tickers_yfinance_error(tickers)
        return tickers

    def __remove_tickers_present_in_db(self, tickers: list[str]) -> list[str]:
        """Remove tickers that are already present in the database"""
        tickers_db = self.__get_all_tickers()
        return [x for x in tickers if x not in tickers_db]

    def __remove_tickers_not_present_in_db(self, tickers: list[str]) -> list[str]:
        """Remove tickers that are not present in the database"""
        tickers_db = self.__get_all_tickers()
        return [x for x in tickers if x in tickers_db]

    def __remove_failed_candle_download_tickers(self, tickers: list[str]) -> list[str]:
        """Remove tickers that failed to download candles"""
        tickers_failed = self.__get_failed_candle_download_tickers()
        return [x for x in tickers if x not in tickers_failed]

    def __remove_tickers_yfinance_error(self, tickers: list[str]) -> list[str]:
        """Remove tickers that have yfinance errors"""
        tickers_yfinance_error = self.__get_yfinance_error_tickers()
        return [x for x in tickers if x not in tickers_yfinance_error]

    def __execute_query_return_df(self, query: text) -> pd.DataFrame:
        result = self.session.execute(query)
        rows = result.fetchall()
        columns = list(result.keys())
        return pd.DataFrame(rows, columns=columns)
    
    def __execute_query_return_list_of_first_element(self, query: text) -> list:
        result = self.session.execute(query)
        rows = result.fetchall()
        return [x[0] for x in rows]

    def __get_all_tickers(self) -> list[str]:
        query = text("""
            SELECT symbol
            FROM ticker
            ORDER BY symbol
        """)
        return self.__execute_query_return_list_of_first_element(query)

    def __get_failed_candle_download_tickers(self) -> list[str]:
        query = text("""
            SELECT symbol
            FROM ticker
            WHERE failed_candle_download = True
            ORDER BY symbol
        """)
        return self.__execute_query_return_list_of_first_element(query)

    def __get_yfinance_error_tickers(self) -> list[str]:
        query = text("""
            SELECT DISTINCT symbol
            FROM ticker T
            JOIN ticker_status S ON T.id = S.ticker_id
            ORDER BY symbol
        """)
        return self.__execute_query_return_list_of_first_element(query)

    # __ LISTS __
    @apply_filter
    def get_all_tickers(self):
        query = text("""
            SELECT symbol
            FROM ticker
            ORDER BY symbol
        """)
        return self.__execute_query_return_list_of_first_element(query)

    @apply_filter
    def get_tickers_not_updated_from_days(self, days: int = 5) -> list[str]:
        # Calculate the date limit (5 days ago)
        date_limit = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
        query = text(f"""
            SELECT symbol
            FROM ticker
            WHERE last_update < '{date_limit}' OR last_update IS NULL
            ORDER BY symbol
        """)
        return self.__execute_query_return_list_of_first_element(query)

    @apply_filter
    def get_tickers_with_day_candles_not_updated_from_days(self, days: int = 5) -> list[str]:
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
        return self.__execute_query_return_list_of_first_element(query)

    @apply_filter
    def get_tickers_with_week_candles_not_updated_from_days(self, days: int = 5) -> list[str]:
        # Calculate the date limit (5 days ago)
        date_limit = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
        query = text(f"""
            WITH LatestUpdates AS (
                SELECT ticker_id, MAX(last_update) AS max_last_update
                FROM candle_data_week
                GROUP BY ticker_id
            )
            SELECT T.symbol, LU.max_last_update
            FROM ticker T
            JOIN LatestUpdates LU ON T.id = LU.ticker_id
            WHERE LU.max_last_update < '{date_limit}'
            ORDER BY T.symbol
        """)
        return self.__execute_query_return_list_of_first_element(query)

    @apply_filter
    def get_all_tickers_with_candlestick_analysis_not_updated(self) -> list[str]:
        """Get all symbols that have not been updated with the latest candlestick analysis"""
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
        return self.__execute_query_return_list_of_first_element(query)

    @apply_filter
    def get_failed_candle_download_tickers(self) -> list[str]:
        query = text("""
            SELECT symbol
            FROM ticker
            WHERE failed_candle_download = True
            ORDER BY symbol
        """)
        return self.__execute_query_return_list_of_first_element(query)

    @apply_filter
    def get_sp500_tickers(self) -> list[str]:
        query = text(f"""
            SELECT *
            FROM sp_500_historical
            WHERE date = (SELECT MAX(date)  FROM sp_500_historical)
            ;
        """)
    
        # Execute the query through the session
        results = self.session.execute(query).mappings().all()

        return [x['ticker_yfinance'] for x in results]

    # __ DATAFRAMES __
    def get_tickers_with_exchange(self) -> pd.DataFrame:
        query = text(f"""
            SELECT T.symbol, I.exchange, I.last_update
            FROM ticker T
            JOIN info_general_stock I
            ON T.id = I.ticker_id
            ;
        """)
        return self.__execute_query_return_df(query)

    def get_all_tickers_info(self) -> pd.DataFrame:
        query = text(f"""
            WITH ranked_general AS (
                SELECT
                    T.id,
                    T.symbol,
                    T.company_name,
                    IGS.exchange,
                    IGS.last_update,
                    ROW_NUMBER() OVER (PARTITION BY T.symbol ORDER BY IGS.last_update DESC) AS rn
                FROM ticker T
                JOIN info_general_stock IGS
                    ON T.id = IGS.ticker_id
            ),
            ranked_trading AS (
                SELECT
                    T.id,
                    ITS.market_cap,
                    ITS.trailing_pe,
                    ITS.forward_pe,
                    ITS.current_price,
                    ITS.two_hundred_day_average,
                    ITS.fifty_two_week_high,
                    ITS.fifty_two_week_low,
                    ROW_NUMBER() OVER (PARTITION BY T.symbol ORDER BY ITS.last_update DESC) AS rn
                FROM ticker T
                JOIN info_trading_session ITS
                    ON T.id = ITS.ticker_id
            )
            SELECT G.id, G.symbol, G.company_name,
                   G.exchange, G.last_update,
                   T.market_cap
                   ,T.trailing_pe, T.forward_pe
                   ,T.current_price, T.two_hundred_day_average
                   ,T.fifty_two_week_high, T.fifty_two_week_low
                   ,(T.current_price / T.two_hundred_day_average - 1) * 100 AS price_to_200_day_avg
                   ,(T.current_price / T.fifty_two_week_high - 1) * 100 AS price_to_52_week_high
                   ,(T.current_price / T.fifty_two_week_low - 1) * 100 AS price_to_52_week_low
            FROM ranked_general G
            JOIN ranked_trading T
                ON G.id = T.id
            WHERE G.rn = 1 AND T.rn = 1
                AND T.market_cap IS NOT NULL
            --     AND (G.exchange = 'NMS' OR G.exchange = 'NGM' OR G.exchange = 'NCM') -- NASDAQ
            --     AND G.exchange = ' NYQ' -- NYSE
            --     AND G.exchange = 'ASE' -- NYSE MKT
                AND G.exchange != 'NCM' -- NOT 'NCM'
            ORDER BY T.market_cap DESC;
        """)
        return self.__execute_query_return_df(query)


if __name__ == '__main__':
    # __ sqlAlchemy __ create new session
    session_ = session_local()

    queries_db = QueriesDB(session_)
    df = queries_db.get_all_tickers_info()
    print(df.head())
