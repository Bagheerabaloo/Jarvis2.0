import pandas as pd
import os
from dataclasses import dataclass
from typing import Optional
from pathlib import Path
from datetime import datetime

import yfinance as yf
from src.stock.src.db.database import session_local


@dataclass
class SP500Handler:
    data_path: Path = Path(__file__).parent.joinpath('github')

    @staticmethod
    def read_table(filename) -> Optional[pd.DataFrame]:
        if os.path.isfile(filename):
            df = pd.read_csv(filename, index_col='date')
            return df
        return None

    @staticmethod
    def get_sp500_from_wikipedia(include_company_data=False, replace_dot=True):
        """Downloads list of tickers currently listed in the S&P 500"""

        # __ get list of all S&P 500 stocks __
        sp500 = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]

        if include_company_data:
            return sp500

        sp_tickers = sp500.Symbol.tolist()
        sp_tickers = sorted(sp_tickers)

        return [x.replace(".", "-") if replace_dot else x for x in sp_tickers]

    def get_sp500_historical_from_file(self) -> pd.DataFrame:
        """Reads the S&P 500 Historical Components & Changes csv file and returns a dataframe."""
        filename = self.data_path.joinpath('S&P 500 Historical Components & Changes.csv')
        df = self.read_table(filename)
        # Convert ticker column from csv to list, then sort.
        df['tickers'] = df['tickers'].apply(lambda x: sorted(x.split(',')))
        # Replace SYMBOL-yyyymm with SYMBOL.
        df['tickers'] = [[ticker.split('-')[0] for ticker in tickers] for tickers in df['tickers']]
        # Add LIN after 2018-10-31
        df.loc[df.index > '2018-10-31', 'tickers'].apply(lambda x: x.append('LIN'))
        # Remove duplicates in each row.
        df['tickers'] = [sorted(list(set(tickers))) for tickers in df['tickers']]
        return df

    def get_sp500_historical_changes_from_file(self) -> pd.DataFrame:
        # Changes to the list of S&P 500 components -> https://en.wikipedia.org/wiki/List_of_S%26P_500_companies#Selected_changes_to_the_list_of_S&P_500_components

        # __ read S&P 500 changes csv file __
        filename = self.data_path.joinpath('sp500_changes_since_2019.csv')
        changes = self.read_table(filename)

        # __ convert ticker column from csv to list, then sort __
        changes['add'] = changes['add'].apply(lambda x: sorted(x.split(',')))
        changes['remove'] = changes['remove'].apply(lambda x: sorted(x.split(',')))
        return changes

    @staticmethod
    def add_changes_to_historical_sp500(historical: pd.DataFrame, changes: pd.DataFrame) -> pd.DataFrame:
        """ Updates the S&P 500 historical components dataframe with the changes dataframe.
        :param historical: dataframe of historical S&P 500 components until 2019
        :param changes:  dataframe of changes from 2019 to the S&P 500 components
        :return: updated dataframe of historical S&P 500 components
        """
        for index, row in changes.iterrows():
            new_row = historical.tail(1)

            tickers = list(new_row['tickers'].iloc[0])
            tickers += row["add"]
            tickers = list(set(tickers) - set(row["remove"]))
            tickers = sorted(tickers)

            d = {'date': index, 'tickers': [tickers]}
            new_entry = pd.DataFrame(d)
            new_entry.set_index('date', inplace=True)
            historical = pd.concat([historical, new_entry])

        return historical

    def compare_sp500(self, updated_df: pd.DataFrame) -> bool:
        # __ compare last row to current S&P500 list __
        current = self.get_sp500_from_wikipedia(replace_dot=False)
        last_entry = list(updated_df['tickers'].iloc[-1])

        diff = list(set(current) - set(last_entry)) + list(set(last_entry) - set(current))
        if len(diff) > 0:
            print(f" ############## WARNING ##############\n S&P 500 from wikipedia is different:\n{diff}")
            return False
        return True

    def save_new_sp500_historical(self, df: pd.DataFrame):
        # Convert tickers column back to csv.
        df['tickers'] = df['tickers'].apply(lambda x: ",".join(x))
        filename = self.data_path.joinpath(f"S&P 500 Historical Components & Changes({datetime.now().strftime('%Y-%m-%d')}).csv")
        df.to_csv(filename)

    def update_historical_sp500(self, save_new_snapshot: bool = True):
        historical = self.get_sp500_historical_from_file()
        changes = self.get_sp500_historical_changes_from_file()

        # __ update historical S&P 500 components with changes __
        updated_historical = self.add_changes_to_historical_sp500(historical, changes)

        # __ compare last row to current S&P500 list __
        self.compare_sp500(updated_historical)

        # __ save new snapshot of S&P 500 historical components __
        self.save_new_sp500_historical(updated_historical.copy()) if save_new_snapshot else None

        return updated_historical

    def get_delisted_sp500_components(self, replace_dot: bool = True) -> list[str]:
        """Get list of components that are not anymore in the S&P 500"""
        current = self.update_historical_sp500(save_new_snapshot=True)
        wikipedia = self.get_sp500_from_wikipedia(replace_dot=False)

        unique_tickers = set([ticker for sublist in current['tickers'] for ticker in sublist])
        delisted_tickers = sorted(list(set(unique_tickers) - set(wikipedia)))
        return [x.replace(".", "-") if replace_dot else x for x in delisted_tickers]

    def get_tickers_not_present_in_yfinance(self):
        # __ sqlAlchemy __ create new session
        session = session_local()

        # __ get all tickers __
        current = self.update_historical_sp500(save_new_snapshot=False)

        # __ get all unique tickers __
        unique_tickers = sorted(list(set([ticker for sublist in current['tickers'] for ticker in sublist])))
        # unique_tickers = ["VNT"]
        delisted_tickers = []
        for ticker in unique_tickers:
            # print(f"{ticker} - checking...")

            try:
                candle_data = yf.download(
                            tickers=ticker,
                            interval="1d",
                            period="max",
                            progress=False
                        )

                if candle_data.empty:
                    delisted_tickers.append(ticker)
                    continue
            except KeyError as e:
                delisted_tickers.append(ticker)
                continue

        print(delisted_tickers)


if __name__ == '__main__':
    sp500_handler = SP500Handler()
    sp500 = sp500_handler.update_historical_sp500()
    # sp500_handler.get_delisted_sp500_components()
    # sp500_handler.get_tickers_not_present_in_yfinance()
