import pandas as pd
from sqlalchemy.orm import session as sess
from dataclasses import dataclass
from src.stock.src.db.database import session_local
from src.stock.src.db.QueriesDB import QueriesDB
from src.stock.src.indexes.Indexes import Indexes
from src.stock.src.etf.etf import Etf
from src.stock.src.indexes.sp500.sp500DBHandler import SP500DBHandler
from src.stock.src.exchanges.nasdaq.nasdaqHandler import get_nasdaq_tickers
from src.stock.src.exchanges.nyse.nyseHandler import get_nyse_tickers


@dataclass
class Queries(QueriesDB):
    def __post_init__(self):
        self.sp500_handler = SP500DBHandler(self.session)

    def get_sp500_tickers_from_wikipedia(self, include_company_data=False) -> list[str]:
        """Downloads list of tickers currently listed in the S&P 500"""
        symbols = self.sp500_handler.get_sp500_tickers_list_from_wikipedia()
        return symbols

    # @staticmethod
    # def get_symbols_from_sp500_handler() -> list[str]:
    #     sp500_handler = SP500Handler()
    #     symbols = sp500_handler.get_delisted_sp500_components()
    #     return symbols

    @staticmethod
    def get_indexes_tickers() -> list[str]:
        return Indexes.get_all_indexes()

    @staticmethod
    def get_etf_tickers() -> list[str]:
        return Etf.get_all_etf_tickers()

    @staticmethod
    def get_nasdaq_tickers() -> list[str]:
        return get_nasdaq_tickers()

    @staticmethod
    def get_nyse_tickers():
        return get_nyse_tickers()



if __name__ == '__main__':
    # __ sqlAlchemy __ create new session
    session_ = session_local()

    ticker_lister = Queries(session_)
    df = ticker_lister.get_all_tickers_info()
    print(df.head())