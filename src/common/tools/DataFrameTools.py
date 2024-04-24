import datetime
import os
import re
from typing import Optional

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from tabulate import tabulate


# _____ Load _____

def load(table: str, encoding: str = "ISO-8859-1", folder: str = 'data', sort_columns: bool = True,
         date_columns: list = None) -> Optional[pd.DataFrame]:
    """ Load csv tables into pandas Dataframe """

    # print(f'======== start {table} loading ========')

    filename = f'{folder}/{table}.csv'

    if f'{table}.csv' not in os.listdir(f'{folder}/'):
        return None

    if date_columns is None:
        date_columns = []

    print(f'=> loading "{filename}" dataset..')

    df = pd.read_csv(filename, encoding=encoding, low_memory=False, parse_dates=date_columns)

    # sort columns
    df.sort_index(axis=1, ascending=True, inplace=True) if sort_columns else None

    # keep time and/or date column as first ones
    for name in ['time', 'date']:
        if name in df.columns:
            col = df.pop(name)
            df.insert(0, name, col)

    print(f'Loaded {table}: {len(df)} records found')

    return df


# _____ Describe _____

def pretty_print(obj):
    print(to_markdown(obj)) if isinstance(obj, pd.DataFrame) else print(obj)


def print_df(df: pd.DataFrame):
    print(to_markdown(df))


def awesome_print(df):
    print(tabulate(df, headers='keys', tablefmt='psql')) if type(df) == pd.core.frame.DataFrame else print(df)


def describe(data: pd.DataFrame, table: str = 'NONE', write_to_file: bool = False) -> None:
    """ Generic input data documentation """

    print(f'======== start {table} documentation ========\n')

    # columns info
    _info = pd.concat([pd.DataFrame(data.dtypes), data.count()], axis=1) \
        .set_axis(['type', 'non-null count'], axis='columns', inplace=False)

    # numeric columns documentation
    description = data.describe()

    # examples
    samples = data.head()

    if write_to_file:
        if 'documentation' not in os.listdir():
            os.mkdir('documentation')
        filename = f'documentation/{table}.txt'
        print(f'=> writing documentation to file "{filename}"..')

        with open(filename, mode='w+') as file:
            file.write(f'======== start {table} documentation ========\n\n')

            file.write('************ DATAFRAME DESCRIPTION ************\n\n')
            file.write(f'shape={data.shape}\n')
            file.write(to_markdown(_info))

            file.write('\n\n************ NUMERIC DESCRIPTION ************\n\n')
            file.write(to_markdown(description))

            file.write('\n\n****************** SAMPLES ******************\n\n')
            file.write(to_markdown(samples))

            file.write(f'\n\n========  end {table} documentation  ========')
    else:
        print(f'shape={data.shape}')
        print_df(_info)
        print_df(description)
        print_df(samples)

    print(f'========  end {table} documentation  ========\n')


def info(df: pd.DataFrame) -> None:
    """ DataFrame data info per column"""

    for column in df.columns:
        values = [str(x) for x in df[column].unique()]
        print(f'{column.capitalize()} are:' + ', '.join(values))


# _____ Null info _____

def null_counting(df: pd.DataFrame) -> pd.DataFrame:
    """ return a new df including the null values counting
    for all columns which have at least one null"""
    return df[df.columns[df.isna().any()]].isna().sum()


def print_null_counting(df: pd.DataFrame) -> None:
    """ print all columns which have at least one null"""
    null_df = null_counting(df)
    print(f'Null columns: {null_df.to_dict()}')


def contains_null(df: pd.DataFrame) -> bool:
    """ returns true if the df contains at least one null value """
    return df.isnull().values.any()


def is_null(df: pd.DataFrame) -> bool:
    """ returns true if the df contains at least one null value """
    return True if df.isnull().values.any() else False


# _____ Conversion _____

def to_markdown(df: pd.DataFrame) -> str:
    return df.to_markdown(headers='keys', tablefmt='psql')


def to_numpy(df):
    return df.to_numpy()


# _____ Preparation _____

def process(entry_df: pd.DataFrame, sort_cols=None, mandatory_cols=None, interesting_cols=None) \
        -> Optional[pd.DataFrame]:
    """ Performs basic data manipulation to the input df """

    df = entry_df.copy(deep=True)

    # === defaults === #
    if sort_cols is None:
        sort_cols = ['time']

    if mandatory_cols is None:
        mandatory_cols = []

    if interesting_cols is None:
        interesting_cols = []

    print(f'=> filtering records where {mandatory_cols} contain null values..')

    df.dropna(how='all', inplace=True)
    df.dropna(subset=mandatory_cols, inplace=True)  # at least one missing values

    print('=> filtering out useless columns and duplicates..')

    df.drop(columns=['Unnamed: 0'], inplace=True)
    df.dropna(how='all', axis=1, inplace=True)
    df.drop_duplicates(inplace=True)

    if len(interesting_cols) > 0:
        print(f'=> selecting {interesting_cols}..')
        df = df[interesting_cols]

    print(f'=> sorting df by {sort_cols}..')

    df.sort_values(sort_cols, ascending=[True] * len(sort_cols), inplace=True)

    print('=> resetting indexes..')
    df.reset_index(drop=True, inplace=True)

    return df


def fill_null_values_categorical(df: pd.DataFrame, null_value='EMPTY', cols=None) -> pd.DataFrame:
    """ fill categorical columns that contains null values with `null_value`"""

    if cols is None:
        categorical_cols = df.loc[:, df.dtypes == 'object'].columns.values.tolist()
        cols = [x for x in categorical_cols if x in list(null_counting(df).index.values)]

    for col in cols:
        print(f'filling categorical "{col}"..')
        df[col] = df[col].fillna(null_value)
        df[col] = df[col].apply(lambda val: null_value if val == '[]' else val)

    return df


def fill_null_values_numerical(df: pd.DataFrame, cols: list, null_value=0) -> pd.DataFrame:
    """ fill numerical values columns that contains null values with `null_value`"""

    for _col in cols:
        print(f'filling numerical "{_col}"..')
        if _col in df.columns:
            df[_col] = df[_col].fillna(null_value)
        else:
            print(f'column "{_col}" not found. skipping..')

    return df


def base_cleanup(df: pd.DataFrame, _debug=True) -> pd.DataFrame:
    """ perform a base columns/rows cleanup """

    # 1) remove a-priori useless columns
    df = df.drop(columns=['Unnamed: 0'], errors='ignore')

    # 2) remove columns containing all null values
    df = drop_columns_with_all_nan(df)

    # 3) remove rows containing all null values
    df = drop_rows_with_all_nan(df)

    # 4) drop duplicated records, if any
    df = df.drop_duplicates()

    # 5) sort index
    df = df.sort_index(axis=1, ascending=True)

    # 6) keep time/id columns as first ones
    df = move_columns_to_first_position(df, ['time', 'id'])

    # 7) reset indexes
    df = df.reset_index(drop=True)

    if _debug:
        print(f'Total entries after cleanup: {len(df)}')

    return df


def standard_cleanup(df: pd.DataFrame, _debug=True) -> (pd.DataFrame, bool):
    """
    perform a base cleanup on the database and compute
    if something changed during this manipulation
    """

    initial_shape = df.shape
    df = base_cleanup(df, _debug)

    return df, not (initial_shape == df.shape)


def move_columns_to_first_position(df: pd.DataFrame, list_: list) -> pd.DataFrame:
    list_.reverse()

    for col in list_:
        if col in df.columns:
            popped = df.pop(col)
            df.insert(0, col, popped)

    return df


def remove_useless_columns(df: pd.DataFrame, useless_columns: list) -> pd.DataFrame:
    return df.drop(useless_columns, axis=1, errors='ignore')


def convert_categorical_columns_to_uppercase(df: pd.DataFrame, column: str = None) -> pd.DataFrame:
    """ converts all categorical columns (i.e. object) to uppercase string to be case insensitive"""

    if column is None:
        columns = df.loc[:, df.dtypes == 'object'].columns.values
    else:
        columns = [column]

    for col in columns:
        df[col] = df[col].apply(lambda val: str(val).upper())

    return df


def remove_columns_that_contains_only_one_value(df: pd.DataFrame) -> pd.DataFrame:
    """ removes all columns that contains only one value"""

    useless = []
    # series: index=col, val=[true if 1 distinct value, false otherwise]
    tmp = df.nunique() == 1
    useless.extend(tmp[np.where(tmp)[0]].index.to_list())

    return df.drop(useless, axis=1, errors='ignore')


def drop_rows_with_all_nan(df: pd.DataFrame) -> pd.DataFrame:
    return df.dropna(how='all')


def drop_columns_with_all_nan(df: pd.DataFrame) -> pd.DataFrame:
    print(f'dropping columns with all nulls..')
    return df.dropna(how='all', axis=1)


def drop_rows_with_nan_mandatory_cols(df: pd.DataFrame, mandatory_cols) -> pd.DataFrame:
    print(f'dropping records having nulls for following columns: {mandatory_cols}..')
    return df.dropna(subset=mandatory_cols)


def to_camelcase(string: str) -> str:
    """ convert a string to a capitalized camel case notation """
    string = string.replace('-', '_')
    string = ''.join(list(map(lambda x: x.capitalize(), string.split(' '))))
    return ''.join(list(map(lambda x: x.lower().capitalize(), string.split('_'))))


def to_snake_case(name: str):
    """ takes camel case format string as input and return its snake case conversion"""

    pattern = re.compile(r'(?<!^)(?=[A-Z])')
    return pattern.sub('_', name).lower()


def to_percentage(total, count):
    return 0 if count <= 0 else count / total


def columns_to_percentage(df: pd.DataFrame, overall: str, to_convert: list):
    """ converts `to_convert` columns from absolute count to percentage wrt the `overall` column"""

    for col in to_convert:
        df[col] = df.apply(lambda row: to_percentage(row[overall], row[col]), axis=1)

    return df


def one_hot_encode(df_: pd.DataFrame, cols: list, drop_columns: bool = False,
                   preserve_col_name: bool = False) -> pd.DataFrame:
    """ iterate through each col and enhances the dataframe by adding the one hot encoding for each col"""

    for col in cols:
        # skip if column is not present in dataFrame
        if col not in df_.columns:
            continue

        # For each unique value of col generate a new column
        for unique_value in df_[col].unique():
            col_name = to_camelcase(str(unique_value))
            new_column_name = f'is{col_name}' if not preserve_col_name else f'{col}Is{col_name}'
            df_[new_column_name] = df_[col].apply(lambda v: of_specific_value(v, unique_value))

        # Drop column at the end if drop_columns = True
        if drop_columns:
            df_ = df_.drop(columns=[col])

    return df_


# _____ Filtering _____

def filter_by_value(df: pd.DataFrame, column: str, value: any, drop_: bool = True) -> pd.DataFrame:
    if column in df.columns:
        df = df[df[column] == value]
        return df.drop([column], axis=1, errors='ignore', inplace=False) if drop_ else df
    else:
        print(f'Column {column} not found. Skipping..')
        return df


def filter_by_values(df: pd.DataFrame, filter_: dict, drop_: bool = True) -> pd.DataFrame:
    """filter_ must be a dictionary with dataFrame columns as keys and value to filter as value of the dict """

    for column in filter_:
        df = filter_by_value(df=df, column=column, value=filter_[column], drop_=drop_)

    return df


# _____ Other utils _____

def extract_datetime_info(df: pd.DataFrame, date_columns: list) -> pd.DataFrame:
    """ extract additional datetime information, like dayofweek and so on.. """

    for col in date_columns:
        day_name = f'{col}DayOfWeek'
        month_name = f'{col}Month'

        if col in df.columns:
            df[day_name] = df[col].dt.day_name()  # extract day of week
            df[month_name] = df[col].dt.month_name()  # extract month name

    return df


def filter_data_by_week(df: pd.DataFrame, week: str, matching_col) -> pd.DataFrame:
    """ filter dataset considering only records belonging to the provided time window """
    return df[(df[matching_col] == week)].reset_index(drop=True)


def to_string(timestamp: pd.Timestamp) -> str:
    return str(timestamp)[0:10].replace('-', '')


def extract_weeks(start_time: str = '2021-03-01', end_time: str = '2021-12-31'):
    """
    given a time window (in the same year) it extracts an array of
    weeks starting from start_time to end_time (included)
    """

    date1 = datetime.datetime.strptime(start_time, "%Y-%m-%d").date()
    date2 = datetime.datetime.strptime(end_time, "%Y-%m-%d").date()

    assert date1.year == date2.year

    weeks = [(date1 + relativedelta(weeks=i)).strftime("%Y-%m-%d") for i in
             range(0, date2.isocalendar()[1] - date1.isocalendar()[1], 1)]
    weeks.append(end_time)

    periods = []
    for i in range(len(weeks) - 1):
        periods.append((pd.to_datetime(weeks[i]), pd.to_datetime(weeks[i + 1])))

    return periods


def generate_data(df: pd.DataFrame, weeks, filter_fn, filter_col):
    """
    iterate over a subset of periods and compute some enhancement on the filtered temporary df - after
    that concatenate all temporary datasets together creating multiple records for the same input record
    per each period
    """

    windowed_df = None

    # iterate over each period and compute enhancement on the filtered df
    for week in weeks:
        print(f'augmenting data for week {week}..')
        tmp = filter_fn(df, week, filter_col)

        if windowed_df is None:
            windowed_df = tmp
        else:
            windowed_df = pd.concat([windowed_df, tmp], ignore_index=True)

    return windowed_df


def extract_week(timestamp: pd.Timestamp):
    return timestamp.dt.isocalendar().week


def is_number(s):
    """ Returns True is string is a number. """
    try:
        float(s)
        return True
    except ValueError:
        return False


def of_specific_value(value: str, expected: str) -> int:
    """ return 1 if the value is equal to the expected one, otherwise 0 """
    return 1 if value == expected else 0


