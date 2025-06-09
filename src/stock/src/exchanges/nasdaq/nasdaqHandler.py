import pandas as pd
import os
import requests
import io
from dataclasses import dataclass
from typing import Optional
from pathlib import Path
from datetime import datetime

import yfinance as yf
from src.stock.src.db.database import session_local
from sqlalchemy import text

session = session_local()


def get_nasdaq_tickers() -> list[str]:
    """
    https://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs

    Field Name	        Definition
    Symbol	            The one to four or five character identifier for each NASDAQ-listed security.
    Security Name	    Company issuing the security.

    Market Category	    The category assigned to the issue by NASDAQ based on Listing Requirements. Values:
                        Q = NASDAQ Global Select MarketSM
                        G = NASDAQ Global MarketSM
                        S = NASDAQ Capital Market

    Test Issue	        Indicates whether or not the security is a test security. Values: Y = yes, it is a test issue. N = no, it is not a test issue.
    Financial Status	Indicates when an issuer has failed to submit its regulatory filings on a timely basis, has failed to meet NASDAQ's continuing listing standards, and/or has filed for bankruptcy. Values include:
                        D = Deficient: Issuer Failed to Meet NASDAQ Continued Listing Requirements
                        E = Delinquent: Issuer Missed Regulatory Filing Deadline
                        Q = Bankrupt: Issuer Has Filed for Bankruptcy
                        N = Normal (Default): Issuer Is NOT Deficient, Delinquent, or Bankrupt.
                        G = Deficient and Bankrupt
                        H = Deficient and Delinquent
                        J = Delinquent and Bankrupt
                        K = Deficient, Delinquent, and Bankrupt
    Round Lot	        Indicates the number of shares that make up a round lot for the given security.
    File Creation Time:	The last row of each Symbol Directory text file contains a timestamp that reports the File Creation Time. The file creation time is based on when NASDAQ Trader generates the file and can be used to determine the timeliness of the associated data. The row contains the words File Creation Time followed by mmddyyyyhhmm as the first field, followed by all delimiters to round out the row. An example: File Creation Time: 1217200717:03|||||
    """
    url = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
    response = requests.get(url)
    nasdaq = pd.read_csv(io.StringIO(response.content.decode('utf-8')), sep='|')
    nasdaq = nasdaq.dropna(subset=['Symbol'])
    nasdaq_tickers = nasdaq.Symbol.tolist()
    nasdaq_tickers = sorted(nasdaq_tickers)
    return nasdaq_tickers


def check_nasdaq_tickers() -> list[str]:
    nasdaq_tickers = get_nasdaq_tickers()

    query = text(f"""
        SELECT T.symbol, I.exchange, I.last_update
        FROM ticker T
        JOIN info_general_stock I
        ON T.id = I.ticker_id
        ;
    """)

    # Execute the query through the session
    results = session.execute(query).fetchall()

    results_as_dicts = [
        {'ticker': result[0],
         'exchange': result[1],
         'last_update': result[2]
         }
        for result in results
    ]

    df = pd.DataFrame(results_as_dicts)

    # __ transform list of nyse tickers in df __
    nasdaq_tickers = [x.replace(".", "-") if "." in x else x for x in nasdaq_tickers]
    df_nasdaq = pd.DataFrame(nasdaq_tickers, columns=["ticker"])

    # __ left join df with df_nyse __
    df_merged = df_nasdaq.merge(df, on="ticker", how="left")
    # df_merged.dropna(subset=["exchange"], inplace=True)
    df_merged.sort_values(by=["ticker", "last_update"], ascending=[True, False], inplace=True)
    df_merged.drop_duplicates(subset=["ticker"], inplace=True, keep="first")

    # __ print unique exchanges __
    print(df_merged["exchange"].unique())

    # __ filter out tickers with exchange different from NYSE __
    df_not_nyse = df_merged[~df_merged["exchange"].isin(["NCM", "NGM", "NMS"])]

    return list(df_not_nyse["ticker"])


if __name__ == '__main__':
    # tickers = get_nasdaq_tickers()
    # print(tickers)
    # print(len(tickers))
    check_nasdaq_tickers()