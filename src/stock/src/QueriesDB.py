import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text
from sqlalchemy.orm import session as sess
from src.stock.src.database import session_local


class QueriesDB:
    def __init__(self, session: sess.Session):
        self.session = session
    
    def __execute_query_return_df(self, query: text) -> pd.DataFrame:
        result = self.session.execute(query)
        rows = result.fetchall()
        columns = list(result.keys())
        return pd.DataFrame(rows, columns=columns)
    
    def __execute_query_return_list_of_first_element(self, query: text) -> list:
        result = self.session.execute(query)
        rows = result.fetchall()
        return [x[0] for x in rows]

    # __ LISTS __
    def get_all_tickers(self) -> list[str]:
        query = text("""
            SELECT symbol
            FROM ticker
            ORDER BY symbol
        """)
        return self.__execute_query_return_list_of_first_element(query)

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

    def get_tickers_with_candles_not_updated_from_days(self, days: int = 5) -> list[str]:
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

    def get_sp500_tickers(self) -> list[str]:
        query = text(f"""
            SELECT *
            FROM sp_500_historical
            WHERE date = (
                SELECT MAX(date)
                FROM sp_500_historical
            )
            ;
        """)
    
        # Execute the query through the session
        results = self.session.execute(query).fetchall()
    
        results_as_dicts = [
            {'date': result[0],
             'symbol': result[1]
             }
            for result in results
        ]
    
        return [results_as_dicts[x]['symbol'] for x in range(len(results_as_dicts))]

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
