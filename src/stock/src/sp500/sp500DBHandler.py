import yfinance as yf
import pandas as pd
import os
from dataclasses import dataclass
from typing import Optional
from pathlib import Path
from time import time, sleep
from datetime import datetime, timedelta
from sqlalchemy.orm import session as sess
from sqlalchemy import text

from src.common.tools.library import seconds_to_time, safe_execute
from src.stock.src.TickerService import TickerService
from src.stock.src.database import session_local
from src.stock.src.models import SP500Changes, SP500Historical

session = session_local()


def move_sp500_changes_from_file_to_db():
    def bulk_insert_records(records_to_insert: list, model_class_name: str) -> None:
        """
        Perform bulk insert of records into the database.

        :param records_to_insert: List of records to insert.
        :param model_class_name: Name of the model class for logging.
        """
        if records_to_insert:
            session.bulk_save_objects(records_to_insert)
            session.commit()
            print(f"{model_class_name} - {len(records_to_insert)} records inserted.")
        else:
            print(f"{model_class_name} - no records to insert.")

    def prepare_records(df):
        records_to_insert = []
        for index, row in df.iterrows():
            # Create SP500Changes object for each row
            record = SP500Changes(
                date=index,  # The date is already the index in the DataFrame
                ticker=row['ticker'],  # Ticker symbol
                add=row['add'],  # Add flag
                remove=row['remove'],  # Remove flag
                last_update=datetime.now()  # Optional: add a timestamp of the current insertion time
            )
            records_to_insert.append(record)
        return records_to_insert

    def expand_tickers(df):
        # Convert the index (date) to datetime format for correct sorting
        df.index = pd.to_datetime(df.index, format='%d/%m/%Y')

        # Expand the tickers in the "add" column
        add_rows = df.explode('add')
        add_rows['ticker'] = add_rows['add']
        add_rows['add'] = True
        add_rows['remove'] = False

        # Expand the tickers in the "remove" column
        remove_rows = df.explode('remove')
        remove_rows['ticker'] = remove_rows['remove']
        remove_rows['add'] = False
        remove_rows['remove'] = True

        # Concatenate both DataFrames (add and remove)
        result = pd.concat([add_rows[['ticker', 'add', 'remove']],
                            remove_rows[['ticker', 'add', 'remove']]])

        # Remove rows where tickers are NaN (empty)
        result = result.dropna(subset=['ticker'])

        # Sort by the index (date)
        result = result.sort_index()

        return result

    data_path = Path(__file__).parent.joinpath('github')

    # __ read S&P 500 changes csv file __
    filename = data_path.joinpath('sp500_changes_since_2019.csv')
    changes = pd.read_csv(filename, index_col='date')

    # __ convert ticker column from csv to list, then sort __
    changes['add'] = changes['add'].apply(lambda x: sorted(x.split(',')))
    changes['remove'] = changes['remove'].apply(lambda x: sorted(x.split(',')))

    # Apply the function
    expanded_df = expand_tickers(changes)

    # Perform the bulk insert
    records = prepare_records(expanded_df)
    bulk_insert_records(records, 'SP500Changes')

    return expanded_df


def move_sp500_historical_components_from_file_to_db():
    def get_sp500_historical_from_file() -> pd.DataFrame:
        """Reads the S&P 500 Historical Components & Changes csv file and returns a dataframe."""
        filename = data_path.joinpath('S&P 500 Historical Components & Changes.csv')
        df = pd.read_csv(filename, index_col='date')
        # Convert ticker column from csv to list, then sort.
        df['tickers'] = df['tickers'].apply(lambda x: sorted(x.split(',')))
        # Replace SYMBOL-yyyymm with SYMBOL.
        df['tickers'] = [[ticker.split('-')[0] for ticker in tickers] for tickers in df['tickers']]
        # Add LIN after 2018-10-31
        df.loc[df.index > '2018-10-31', 'tickers'].apply(lambda x: x.append('LIN'))
        # Remove duplicates in each row.
        df['tickers'] = [sorted(list(set(tickers))) for tickers in df['tickers']]
        return df

    def expand_dataframe(df_):
        # Expand tickers into separate rows
        df_expanded = df_.explode('tickers')
        # Rename the 'tickers' column to 'ticker' for consistency
        df_expanded = df_expanded.rename(columns={'tickers': 'ticker'})
        return df_expanded

    def save_expanded_df_to_db(df_expanded_):
        # Convert the expanded DataFrame to a list of SP500Tickers objects
        records_to_insert = [
            SP500Historical(
                date=index,
                ticker=row['ticker'],
                last_update=datetime.now()  # Optional: add a timestamp of the current insertion time
            ) for index, row in df_expanded_.iterrows()
        ]

        # Bulk insert records
        session.bulk_save_objects(records_to_insert)
        session.commit()

    data_path = Path(__file__).parent.joinpath('github')
    df = get_sp500_historical_from_file()

    # Convert the index (date) to datetime format for correct sorting
    df.index = pd.to_datetime(df.index, format='%Y-%m-%d')

    # Expand the DataFrame before inserting into the database
    df_expanded = expand_dataframe(df)

    # Save the expanded DataFrame to the database
    save_expanded_df_to_db(df_expanded)

    # Close session
    session.close()


def update_sp500_historical_from_change():

    def get_sp500_from_wikipedia(include_company_data=False, replace_dot=True):
        """Downloads list of tickers currently listed in the S&P 500"""

        # __ get list of all S&P 500 stocks __
        sp500 = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]

        if include_company_data:
            return sp500

        sp_tickers = sp500.Symbol.tolist()
        sp_tickers = sorted(sp_tickers)

        return [x.replace(".", "-") if replace_dot else x for x in sp_tickers]

    def get_last_sp500_historical_row_from_db():
        query = text(f"""
        SELECT *
        FROM sp_500_historical
        WHERE date = (
        SELECT MAX(date)
        FROM sp_500_historical)
        ;
        """)

        # Execute the query through the session
        results = session.execute(query).fetchall()

        results_as_dicts = [
            {'date': result[0],
             'symbol': result[1]
             }
            for result in results
        ]

        results_compact = {'date': results_as_dicts[0]['date'], 'tickers': [results_as_dicts[x]['symbol'] for x in range(len(results_as_dicts))]}
        df = pd.DataFrame([results_compact])
        df.set_index('date', inplace=True)

        return df

    def get_sp500_changes_after_date(date):
        query = text(f"""
        SELECT *
        FROM sp_500_changes
        WHERE date > '{date}'
        ;
        """)

        # Execute the query through the session
        results = session.execute(query).fetchall()

        results_as_dicts = [
            {'date': result[0],
             'ticker': result[1],
             'add': result[2],
             'remove': result[3]
             }
            for result in results
        ]

        df = pd.DataFrame(results_as_dicts)
        df.set_index('date', inplace=True) if not df.empty else None

        return df

    def add_changes_to_historical_sp500(historical_: pd.DataFrame, changes_: pd.DataFrame) -> pd.DataFrame:
        """ Updates the S&P 500 historical components dataframe with the changes dataframe.
        :param historical_: dataframe of historical S&P 500 components until 2019
        :param changes_:  dataframe of changes from 2019 to the S&P 500 components
        :return: updated dataframe of historical S&P 500 components
        """
        if last_changes.empty:
            return historical_

        def get_tickers_add(group):
            return list(group.loc[group['add'] == True, 'ticker'])

        def get_tickers_remove(group):
            return list(group.loc[group['remove'] == True, 'ticker'])

        grouped_changes_df = changes_.groupby(changes_.index).apply(
            lambda x: pd.Series({
                'add': get_tickers_add(x),
                'remove': get_tickers_remove(x)
            })
        )
        # __ make sure the index is sorted by date __
        grouped_changes_df.sort_index(inplace=True)

        for index, row in grouped_changes_df.iterrows():
            new_row = historical_.tail(1)

            tickers = list(new_row['tickers'].iloc[0])
            tickers += row["add"]
            tickers = list(set(tickers) - set(row["remove"]))
            tickers = sorted(tickers)

            d = {'date': index, 'tickers': [tickers]}
            new_entry = pd.DataFrame(d)
            new_entry.set_index('date', inplace=True)
            historical_ = pd.concat([historical_, new_entry])

        return historical_

    def compare_sp500(updated_df: pd.DataFrame) -> bool:
        # __ compare last row to current S&P500 list __
        current = get_sp500_from_wikipedia(replace_dot=False)
        last_entry = list(updated_df['tickers'].iloc[-1])

        diff = list(set(current) - set(last_entry)) + list(set(last_entry) - set(current))
        if len(diff) > 0:
            print(f" ############## WARNING ##############\n S&P 500 from wikipedia is different:\n{diff}")
            return False
        print("S&P 500 from wikipedia is the same as the last entry.")
        return True

    def save_new_sp500_historical(df_: pd.DataFrame):
        df_expanded = df_.explode('tickers')
        # Rename the 'tickers' column to 'ticker' for consistency
        df_expanded = df_expanded.rename(columns={'tickers': 'ticker'})
        # Convert the expanded DataFrame to a list of SP500Historical objects
        records_to_insert = [
            SP500Historical(
                date=pd.to_datetime(index).date(),
                ticker=row['ticker'],
                last_update=datetime.now()  # Optional: add a timestamp of the current insertion time
            ) for index, row in df_expanded.iterrows()
        ]

        # Bulk insert records
        session.bulk_save_objects(records_to_insert)
        session.commit()

    last_sp500_df = get_last_sp500_historical_row_from_db()
    last_date = last_sp500_df.index[0]
    last_changes = get_sp500_changes_after_date(last_date)

    if last_changes.empty:
        print("No changes found.")
        compare_sp500(last_sp500_df)
        return None

    # __ update historical S&P 500 components with changes __
    new_historical = add_changes_to_historical_sp500(last_sp500_df, last_changes)

    # __ compare last row to current S&P500 list __
    if compare_sp500(new_historical):
        # __ filter the DataFrame for dates greater than filter_date __
        new_historical = new_historical[new_historical.index > last_date]
        # __ save new snapshot of S&P 500 historical components __
        save_new_sp500_historical(new_historical.copy())



