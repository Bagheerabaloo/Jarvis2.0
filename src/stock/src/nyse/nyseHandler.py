import pandas as pd
import os
import requests
import io
from dataclasses import dataclass
from typing import Optional
from pathlib import Path
from datetime import datetime
from sqlalchemy import text

import yfinance as yf
from src.stock.src.database import session_local

session = session_local()

def get_nyse_tickers(include_not_ordinary: bool = False):
    # __ read file NYSE.txt __
    filename = Path(__file__).parent.joinpath('NYSE.txt')
    if os.path.isfile(filename):
        with open(filename, 'r') as f:
            nyse_tickers = f.read().splitlines()

            # __ convert to df __
            df = pd.DataFrame(nyse_tickers)
            df = df[0].str.split("\t", expand=True) # split column delimited by tab
            df.columns = df.iloc[0] # use first row as columns
            df = df[1:] # remove first row
            df = df.sort_values(by=["Symbol"])

            # __ if include_not_ordinary is True return all stocks __
            if include_not_ordinary:
                return list(df["Symbol"])

            # __ filter out non-ordinary stocks __
            df["has_dot"] = df["Symbol"].str.contains("\.")
            df["has_dash"] = df["Symbol"].str.contains("-")

            # __ create a new column with tickers with only left part __
            df["ticker"] = df["Symbol"]
            df["ticker"] = df["ticker"].str.split(".", expand=True)[0]
            df["ticker"] = df["ticker"].str.split("-", expand=True)[0]

            # __ groupby ticker and count occurrences __
            df_groupby = df.groupby("ticker").count()["Symbol"].rename("count").reset_index()

            # __ join df with df_groupby __
            df = df.merge(df_groupby, on="ticker")

            # __ create column Symbol == ticker __
            df["ticker_is_symbol"] = df["Symbol"] == df["ticker"]

            # __ filter df where count > 1 and ticker_is_symbol is True __
            df_duplicates = df[(df["count"] > 1) & df["ticker_is_symbol"]][["ticker", "ticker_is_symbol"]].rename(columns={"ticker_is_symbol": "is_duplicate"})

            # __ left join df with df_duplicates __
            df = df.merge(df_duplicates, on="ticker", how="left")
            df["is_duplicate"] = df["is_duplicate"].fillna(False)
            df["is_duplicate"] = df["is_duplicate"].astype(bool)

            # __ remove rows where is_duplicate is True and ticker_is_symbol is False __
            df = df[~((df["is_duplicate"] == True) & (df["ticker_is_symbol"] == False))]

            return list(df["Symbol"])


def check_nyse_tickers():
    nyse_tickers = get_nyse_tickers()

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
    nyse_tickers = [x.replace(".", "-") if "." in x else x for x in nyse_tickers]
    df_nyse = pd.DataFrame(nyse_tickers, columns=["ticker"])

    # __ left join df with df_nyse __
    df_merged = df_nyse.merge(df, on="ticker", how="left")
    df_merged.dropna(subset=["exchange"], inplace=True)
    df_merged.sort_values(by=["ticker", "last_update"], ascending=[True, False], inplace=True)
    df_merged.drop_duplicates(subset=["ticker"], inplace=True, keep="first")

    # __ print unique exchanges __
    print(df_merged["exchange"].unique())

    # __ filter out tickers with exchange different from NYSE __
    df_not_nyse = df_merged[df_merged["exchange"] != "NYQ"]

    print('end')


if __name__ == '__main__':
    check_nyse_tickers()