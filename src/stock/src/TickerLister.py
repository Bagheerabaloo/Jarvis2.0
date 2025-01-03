import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text
from sqlalchemy.orm import session as sess

from src.stock.src.database import session_local
from src.stock.src.QueriesDB import QueriesDB
from src.stock.src.Indexes import Indexes
from src.stock.src.sp500.sp500Handler import SP500Handler
from src.stock.src.nasdaq.nasdaqHandler import get_nasdaq_tickers
from src.stock.src.nyse.nyseHandler import get_nyse_tickers


class TickerLister(QueriesDB):
    def __init__(self, session: sess.Session):
        super().__init__(session)

    # TODO: use new SP500Handler class
    @ staticmethod
    def tickers_sp500_from_wikipedia(include_company_data=False):
        """Downloads list of tickers currently listed in the S&P 500"""

        # __ get list of all S&P 500 stocks __
        sp500 = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]
        # sp500["Symbol"] = sp500["Symbol"].str.replace(".", "-", regex=True)

        if include_company_data:
            return sp500

        sp_tickers = sp500.Symbol.tolist()
        sp_tickers = sorted(sp_tickers)

        return [x.replace(".", "-") for x in sp_tickers]

    @staticmethod
    def get_symbols_from_sp500_handler():
        sp500_handler = SP500Handler()
        symbols = sp500_handler.get_delisted_sp500_components()
        return symbols

    @staticmethod
    def get_indexes_tickers():
        return Indexes.get_all_indexes()

    @staticmethod
    def get_nasdaq_tickers():
        return get_nasdaq_tickers()

    @staticmethod
    def get_nyse_tickers():
        return get_nyse_tickers()

    def remove_tickers_present_in_db(self, tickers: list[str]) -> list[str]:
        """Remove tickers that are already present in the database"""
        tickers_db = self.get_all_tickers()
        return [x for x in tickers if x not in tickers_db]


if __name__ == '__main__':
    # __ sqlAlchemy __ create new session
    session_ = session_local()

    ticker_lister = TickerLister(session_)
    df = ticker_lister.get_all_tickers_info()
    print(df.head())