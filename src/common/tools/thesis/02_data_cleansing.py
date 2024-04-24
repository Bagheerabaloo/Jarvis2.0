from src.Tools.DataFrameTools import *
from src.utils import init_logger
import pandas as pd

# initial setup
pd.options.display.max_columns = None

# create logger
logger = init_logger(name='Cleansing')

OVERRIDE_CLIENT_BEST_TOP3PRICE = False
OVERRIDE_CLIENT_DISTANCE_TO_TOB = False
OVERRIDE_CLIENT_M2M = False
OVERRIDE_FLOW_QUARTILE_AND_LP_RELEVANCE = False

OVERRIDE_FLOW_DB = True
OVERRIDE_CLIENT_ORDER = False
OVERRIDE_COCKPIT_TABLES = False


# _____ 360T ______

def clean_360t_tables(save_csv=True, override=False):
    clean_client_best_top_3_price(to_csv=save_csv, override=override or OVERRIDE_CLIENT_BEST_TOP3PRICE)
    clean_client_distance_to_tob(to_csv=save_csv, override=override or OVERRIDE_CLIENT_DISTANCE_TO_TOB)
    clean_client_m2m(to_csv=save_csv, override=override or OVERRIDE_CLIENT_M2M)
    clean_flow_quartile_and_lp_relevance(to_csv=save_csv, override=override or OVERRIDE_FLOW_QUARTILE_AND_LP_RELEVANCE)


def clean_client_best_top_3_price(to_csv=False, override=False):
    table_name = 'ClientBestPriceTop3'
    mandatory_cols = ['category', 'categoryDefinition', 'date', 'client']

    # _____ 0 - Override _____
    if not override and f"{table_name}.csv" in os.listdir('data/02_cleaned'):
        logger.info(f'======== {table_name} cleansing skipped. File already present ========\n')
        return None

    # _____ 1 - Exploration _____
    plot_folder = f'plots/{table_name}'
    base_dir = 'data/01_raw'
    df = load(table=table_name, folder=base_dir)

    # ___ DataFrame description ___
    logger.debug(f'Total clients: {len(df["client"].drop_duplicates().values.tolist())}')
    # describe(df, 'ClientBestPriceTop3')
    # info(df)

    logger.info(f'======== start {table_name} cleansing ========\n')

    # ___ initial cleaning ___
    logger.debug(f'Total entries before cleansing: {len(df)}')
    print_null_counting(df)  # print columns that contain null values - null count

    df = base_cleanup(df)
    df = drop_rows_with_nan_mandatory_cols(df, mandatory_cols=mandatory_cols)  # drop rows for which mandatory_cols are nan
    df = remove_columns_that_contains_only_one_value(df)  # remove columns for which all records have the same value

    print_null_counting(df)  # print columns that contain null values - null count

    # _____ 2 - Preprocessing ______

    df = fill_null_values_categorical(df)  # fill null value with "EMPTY" string
    df = convert_categorical_columns_to_uppercase(df)  # convert all categorical columns to uppercase string
    df['categoryDefinition'] = df['categoryDefinition'].apply(lambda x: ''.join(x.split('/') if '/' in x else x))  # remove "/" from Pairs

    assert not contains_null(df)  # assert no null values are still present

    # _____ 3 - Save file _____
    logger.debug(f'Total entries before saving to csv: {len(df)}')
    output_path = 'data/02_cleaned'
    df.to_csv(f'{output_path}/{table_name}.csv', index=False) if to_csv else None

    # _____ 4 - Plotting ______
    # plotting()

    logger.info(f'======== end {table_name} cleansing ========\n')


def clean_client_distance_to_tob(to_csv=False, override=False):
    table_name = 'ClientDistancetoTOB'
    mandatory_cols = ['category', 'categoryDefinition', 'date', 'client']

    # _____ 0 - Override _____
    if not override and f"{table_name}.csv" in os.listdir('data/02_cleaned'):
        logger.info(f'======== {table_name} cleansing skipped. File already present ========\n')
        return None

    # _____ 1 - Exploration _____
    plot_folder = f'plots/{table_name}'
    base_dir = 'data/01_raw'
    df = load(table=table_name, folder=base_dir)

    # ___ DataFrame description ___
    logger.debug(f'Total clients: {len(df["client"].drop_duplicates().values.tolist())}')
    # describe(df, table_name)
    # info(df)

    logger.info(f'======== start {table_name} cleansing ========\n')

    # ___ initial cleaning ___
    logger.debug('Total entries before cleansing: {}'.format(len(df)))
    print_null_counting(df)  # print columns that contain null values - null count

    df = base_cleanup(df)
    df = drop_rows_with_nan_mandatory_cols(df, mandatory_cols=mandatory_cols)  # drop rows for which mandatory_cols are nan
    df = remove_columns_that_contains_only_one_value(df)  # remove columns for which all records have the same value

    print_null_counting(df)  # print columns that contain null values - null count

    # _____ 2 - Preprocessing ______

    df = fill_null_values_categorical(df)  # fill null value with "EMPTY" string
    df = convert_categorical_columns_to_uppercase(df)  # convert all categorical columns to uppercase string
    df['categoryDefinition'] = df['categoryDefinition'].apply(lambda x: ''.join(x.split('/') if '/' in x else x))  # remove "/" from Pairs

    assert not contains_null(df)  # assert no null values are still present

    # _____ 3 - Save file _____
    logger.debug(f'Total entries before saving to csv: {len(df)}')
    output_path = 'data/02_cleaned'
    df.to_csv(f'{output_path}/{table_name}.csv', index=False) if to_csv else None

    # _____ 4 - Plotting ______
    # plotting()

    logger.info(f'======== end {table_name} cleansing ========\n')


def clean_client_m2m(to_csv=False, override=False):
    table_name = 'ClientM2M'
    mandatory_cols = ['category', 'definition', 'date', 'client', 'T-1', 'T-2', 'T-5', 'T0', 'T1', 'T10', 'T120',
                      'T180', 'T240', 'T30', 'T300', 'T5', 'T60']

    # _____ 0 - Override _____
    if not override and f"{table_name}.csv" in os.listdir('data/02_cleaned'):
        logger.info(f'======== {table_name} cleansing skipped. File already present ========\n')
        return None

    # _____ 1 - Exploration _____
    plot_folder = f'plots/{table_name}'
    base_dir = 'data/01_raw'
    df = load(table=table_name, folder=base_dir, date_columns=['date'])

    # ___ DataFrame description ___
    logger.debug(f'Total clients: {len(df["client"].drop_duplicates().values.tolist())}')
    # describe(df, table_name)
    # info(df)

    logger.info(f'======== start {table_name} cleansing ========\n')

    # ___ initial cleaning ___
    logger.debug('Total entries before cleansing: {}'.format(len(df)))
    print_null_counting(df)  # print columns that contain null values - null count

    df = base_cleanup(df)
    df = drop_rows_with_nan_mandatory_cols(df, mandatory_cols=mandatory_cols)  # drop rows for which mandatory_cols are nan
    df = remove_columns_that_contains_only_one_value(df)  # remove columns for which all records have the same value

    print_null_counting(df)  # print columns that contain null values - null count

    # _____ 2 - Preprocessing ______

    df = fill_null_values_categorical(df)  # fill null value with "EMPTY" string
    df = convert_categorical_columns_to_uppercase(df)  # convert all categorical columns to uppercase string
    df['definition'] = df['definition'].apply(lambda x: ''.join(x.split('/') if '/' in x else x))  # remove "/" from Pairs

    assert not contains_null(df)  # assert no null values are still present

    # _____ 3 - Save file _____
    logger.debug(f'Total entries before saving to csv: {len(df)}')
    output_path = 'data/02_cleaned'
    df.to_csv(f'{output_path}/{table_name}.csv', index=False) if to_csv else None

    # _____ 4 - Plotting ______
    # plotting()

    logger.info(f'======== end {table_name} cleansing ========\n')


def clean_flow_quartile_and_lp_relevance(to_csv=False, override=False):
    table_name = 'FlowQuartileAndLPRelevance'
    mandatory_cols = ['category', 'categoryDefinition', 'date', 'client']

    # _____ 0 - Override _____
    if not override and f"{table_name}.csv" in os.listdir('data/02_cleaned'):
        logger.info(f'======== {table_name} cleansing skipped. File already present ========\n')
        return None

    # _____ 1 - Exploration _____
    plot_folder = f'plots/{table_name}'
    base_dir = 'data/01_raw'
    df = load(table=table_name, folder=base_dir)

    # ___ DataFrame description ___
    logger.debug(f'Total clients: {len(df["client"].drop_duplicates().values.tolist())}')
    # describe(df, 'ClientBestPriceTop3')
    # info(df)

    logger.info(f'======== start {table_name} cleansing ========\n')

    # ___ initial cleaning ___
    logger.debug('Total entries before cleansing: {}'.format(len(df)))
    print_null_counting(df)  # print columns that contain null values - null count

    df = base_cleanup(df)
    df = drop_rows_with_nan_mandatory_cols(df, mandatory_cols=mandatory_cols)  # drop rows for which mandatory_cols are nan
    df = remove_columns_that_contains_only_one_value(df)  # remove columns for which all records have the same value

    print_null_counting(df)  # print columns that contain null values - null count

    # _____ 2 - Preprocessing ______

    df = fill_null_values_categorical(df)  # fill null value with "EMPTY" string
    df = convert_categorical_columns_to_uppercase(df)  # convert all categorical columns to uppercase string
    df['categoryDefinition'] = df['categoryDefinition'].apply(lambda x: ''.join(x.split('/') if '/' in x else x))  # remove "/" from Pairs

    assert not contains_null(df)  # assert no null values are still present

    # _____ 3 - Save file _____
    logger.debug(f'Total entries before saving to csv: {len(df)}')
    output_path = 'data/02_cleaned'
    df.to_csv(f'{output_path}/{table_name}.csv', index=False) if to_csv else None

    # _____ 4 - Plotting ______
    # plotting()

    logger.info(f'======== end {table_name} cleansing ========\n')


# _____ EXCEED ______

def clean_exceed_tables(save_csv=True, override=False):
    clean_flow_db(to_csv=save_csv, override=override or OVERRIDE_FLOW_DB)
    clean_client_order(to_csv=save_csv, override=override or OVERRIDE_CLIENT_ORDER)
    clean_cockpit_table(to_csv=save_csv, override=override or OVERRIDE_COCKPIT_TABLES)


def clean_flow_db(to_csv=False, override=False):
    def compute_date(row, condition_column='isTimedelta', _format='%Y-%m-%d %H:%M:%S.%f'):
        """ merges datetime and timedelta columns if condition column is true, otherwise use 'time' column as target """
        date_value = row['date']
        time_value = row['time']
        condition = row[condition_column]

        if condition:
            return pd.to_datetime(date_value, format=_format).normalize() + pd.to_timedelta(time_value)
        else:
            return pd.to_datetime(time_value, format=_format)

    def check_regex(row, _col, _patterns):
        """ checks if a specific column matches at least one specific regular expression """

        import re

        val = row[_col]
        return any(list(map(lambda p: re.match(p, val) is not None, _patterns)))

    def plotting(data):

        categoricals = data.loc[:, data.dtypes == object].columns.values

        numericals = data.select_dtypes(include='number').columns.values

        datetimes = data.select_dtypes(include='datetime').columns.values

        # categorical value counts
        # do not consider categorical ids - useless for this kind of plotting
        _cols = ['ecn', 'passfail', 'platform', 'spotCentre', 'swapCentre', 'sym', 'tenor']

        fig, axs = plt.subplots(len(_cols), 1, figsize=(15, 5))
        plt.subplots_adjust(hspace=0.6, top=15)

        for n, col in enumerate(_cols):
            ax = axs[n]
            ax.title.set_text(col)
            data[col].value_counts().plot(ax=ax, kind='bar')

        plt.show()

        # top clients
        n = 20
        fig = plt.figure(figsize=(15, 5))
        ax = fig.gca()
        ax.title.set_text(f'Top {n} clients')
        data['externalClientId'].value_counts()[:n].plot(ax=ax, kind='bar')
        plt.show()

        # top traders
        n = 20
        fig = plt.figure(figsize=(15, 5))
        ax = fig.gca()
        ax.title.set_text(f'Top {n} traders')
        data['externalTraderId'].value_counts()[:n].plot(ax=ax, kind='bar')
        plt.show()

        fig = plt.figure(figsize=(15, 5))
        ax = fig.gca()
        ax.title.set_text(f'Confirmed platform volumes')
        data['platform'].value_counts().plot(ax=ax, kind='bar')
        plt.show()

        fig = plt.figure(figsize=(15, 5))
        ax = fig.gca()
        ax.title.set_text(f'Client platform volumes')
        data.groupby(['externalClientId', 'platform']).mean().reset_index()['platform'].value_counts().plot(ax=ax,
                                                                                                            kind='bar')
        plt.show()

        # numerical value histograms
        treshold = 250
        # ignore ids
        cols = np.delete(numericals, np.where((numericals == 'clientId') | (numericals == 'traderId')))
        fig, axs = plt.subplots(len(cols), 1, figsize=(15, 5))
        plt.subplots_adjust(hspace=0.7, top=15)

        for n, col in enumerate(cols):
            ax = axs[n]
            ax.title.set_text(col)
            diff = int((data[col].max() - data[col].min()) / 4)
            diff = treshold if diff > treshold else diff
            data[col].hist(ax=ax, bins=diff)

    table_name = 'flowDB'
    mandatory_cols = ['date', 'externalClientId', 'sym', 'platform', 'dealType']

    # _____ 0 - Override _____
    if not override and f"{table_name}.csv" in os.listdir('data/02_cleaned'):
        logger.info(f'======== {table_name} cleansing skipped. File already present ========\n')
        return None

    # _____ 1 - Exploration _____
    plot_folder = f'plots/{table_name}'
    base_dir = 'data/01_raw'
    df = load(table=table_name, folder=base_dir)

    logger.info(f'======== start {table_name} cleansing ========\n')

    # ___ initial cleansing ___
    logger.debug('Total entries before cleansing: {}'.format(len(df)))
    print_null_counting(df)  # print columns that contain null values - null count

    df = base_cleanup(df)

    # drop rows for which mandatory_cols are nan
    df = drop_rows_with_nan_mandatory_cols(df, mandatory_cols=mandatory_cols)
    logger.debug('Total entries after removing rows without mandatory columns: {}'.format(len(df)))

    print_null_counting(df)  # print columns that contain null values - null count

    # NB: we'll focus only on data coming from EXCEED
    df = filter_by_value(df, column='src', value='EXCEED')
    logger.debug('Total entries after filtering by src = "EXCEED" : {}'.format(len(df)))

    # keep records having empty failreason
    # df = df[df['failreason'].isnull()]
    # logger.debug('Total entries after filtering by failreason is null : {}'.format(len(df)))

    # remove columns for which all records have the same value
    df = remove_columns_that_contains_only_one_value(df)

    print_null_counting(df)  # print columns that contain null values - null count

    # _____ 2 - Preprocessing ______

    # TODO: understand if PNL columns are not relevant for the analysis (i.e. filled with 'EMPTY')
    # or if we must drop rows with null PNL
    # Convert PNL columns to type object so that they can be filled as categorical columns with
    # 'EMPTY' value - the choice can be taken in the preprocessing section
    # for _col in [x for x in df.columns if 'pnl' in x]:
    #     df[_col] = df[_col].astype('object')

    # categorical_cols = df.loc[:, df.dtypes == 'object'].columns.values.tolist()
    # cols = [x for x in categorical_cols if x in list(null_counting(df).index.values) and x not in ['date', 'time', 'dealTime', 'tradeDate', 'valueDate']]
    # df = fill_null_values_categorical(df, cols=cols)  # fill null value with "EMPTY" string

    # print_null_counting(df)  # print columns that contain null values - null count
    # numerical_cols = ['clientAllInRate', 'clientSpotRate']
    # df = fill_null_values_numerical(df, cols=numerical_cols, null_value=-1)

    df = convert_categorical_columns_to_uppercase(df)  # convert all categorical columns to uppercase string

    # convert datetime columns - checks if time columns contains time deltas or datetime formats
    patterns = [
        r'(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2}).(\d{4})',
        r'(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2}).(\d{9})',
        r'(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})'
    ]

    df['isTimedelta'] = df.apply(lambda row: not check_regex(row, _col='time', _patterns=patterns), axis=1)

    # compute new date from time column (if datetime) or date-time merge (if time is timedelta)
    df['date'] = df.apply(lambda row: compute_date(row), axis=1)
    # keeps time and date aligned
    df['time'] = df['date']
    # drop temporary column and old one
    df = df.drop(['isTimedelta'], axis=1, errors='ignore')

    # convert datetimes
    datetime_columns = ['time', 'date']
    for _col in datetime_columns:
        df[_col] = pd.to_datetime(df[_col], format='%Y-%m-%d %H:%M:%S.%f')

    # assert not contains_null(df)  # assert no null values are still present

    # _____ 3 - Save file _____
    logger.debug(f'Total entries before saving to csv: {len(df)}')
    output_path = 'data/02_cleaned'
    df.to_csv(f'{output_path}/{table_name}.csv', index=False) if to_csv else None

    # _____ 4 - Plotting ______
    # plotting()

    logger.info(f'======== end {table_name} cleansing ========\n')


def clean_client_order(to_csv=False, override=False):
    def extract_single_reason(val):
        res = (re.findall(r'^([A-Za-z]*)\(.*\)$', val), True)

        if len(res[0]) == 0:
            # match another regex
            res = (re.findall(r'([A-Za-z ]*):.*', val), True)

        if len(res[0]) == 0:
            res = ([val], False)

        return res

    def extract_reason_of_rejects(val):
        res, match = extract_single_reason(val)

        if not match:
            reasons = list(map(lambda x: x.strip(), val.split(';')))
            extracted = []
            for r in reasons:
                extracted.extend(extract_single_reason(r)[0])
            return str(extracted).replace('[', '').replace(']', '').replace("'", '')

        return res[0]

    def plotting(data):

        fig = plt.figure(figsize=(15, 5))
        ax = fig.gca()
        ax.title.set_text(f'Platform volumes')
        data['platform'].value_counts().plot(ax=ax, kind='bar')
        plt.show()

        fig = plt.figure(figsize=(15, 5))
        ax = fig.gca()
        ax.title.set_text(f'Confirmed platform volumes')
        data[data['orderState'] == 'CONFIRMED']['platform'].value_counts().plot(ax=ax, kind='bar')
        plt.show()

        fig = plt.figure(figsize=(15, 5))
        ax = fig.gca()
        ax.title.set_text(f'Client platform volumes')
        data.groupby(['externalClientId', 'platform']).mean().reset_index()['platform'].value_counts().plot(ax=ax,
                                                                                                            kind='bar')
        plt.show()

    table_name = 'clientOrder'
    mandatory_cols = ['externalClientId']

    # _____ 0 - Override _____
    if not override and f"{table_name}.csv" in os.listdir('data/02_cleaned'):
        logger.info(f'======== {table_name} cleansing skipped. File already present ========\n')
        return None

    # _____ 1 - Exploration _____
    plot_folder = f'plots/{table_name}'
    base_dir = 'data/01_raw'
    df = load(table=table_name, folder=base_dir)

    # ___ DataFrame description ___
    # describe(df, table_name)
    # info(df)

    logger.info(f'======== start {table_name} cleansing ========\n')

    # ___ initial cleaning ___
    logger.debug('Total entries before cleansing: {}'.format(len(df)))
    print_null_counting(df)  # print columns that contain null values - null count

    df = base_cleanup(df)

    # drop rows for which mandatory_cols are nan
    df = drop_rows_with_nan_mandatory_cols(df, mandatory_cols=mandatory_cols)
    df = remove_columns_that_contains_only_one_value(df)  # remove columns for which all records have the same value
    df = df.drop(columns=['exceedTime', 'time', 'tradeDate'], errors='ignore')  # keep only dealDate as reference date
    df = df.rename(columns={'dealDate': 'date'}, errors="raise")
    df = move_columns_to_first_position(df, ['date'])

    print_null_counting(df)  # print columns that contain null values - null count

    logger.debug('Total ESP entries: {}'.format(len(df[df['streamType'] == 'ESP']['streamType'])))
    logger.debug('Total RFS entries: {}'.format(len(df[df['streamType'] == 'RFS']['streamType'])))

    # _____ 2 - Preprocessing ______

    # keep information regarding the same trade aligned across multiple order states
    # provides a custom categorical ordering for 'orderState' column such that if we order dataset
    # and group by trade id we can ensure to keep the last valid status for each trade
    order_states = ['CONFIRMED', 'MANUALLY_CANCELED', 'EXPIRED', 'DENIED', 'TAKE_UP_REJECTED', 'TAKE_UP_CONFIRMED',
                    'REJECTED', 'SALES_REJECTED', 'DI_REQUIRED', 'PRECHECKED', 'PRICE_REQUEST', 'TRADE_REQUEST']
    df['orderState'] = pd.Categorical(df['orderState'], order_states)

    # for each trade keep the last valid order status
    df = df.sort_values('orderState', ascending=True)
    df = df.groupby('id', as_index=False).first()

    logger.debug('Total entries after keeping only the last valid order status {}'.format(len(df)))

    # ignore those trade not yet completed - this is something possible since it depends on when we took data
    df = df[
        (df['orderState'] != 'DI_REQUIRED') &
        (df['orderState'] != 'PRECHECKED') &
        (df['orderState'] != 'TRADE_REQUEST') &
        (df['orderState'] != 'PRICE_REQUEST')
        ]

    logger.debug('Total entries after removing trades not yet completed {}'.format(len(df)))

    # ___ data formatting ___
    # tradeDate column - tradeDate can be null only for take up deals
    # df[df['tradeDate'].isnull()]['dealType'].unique()
    # fill trade date using `dealDate` column
    # df['tradeDate'] = df['tradeDate'].fillna(pd.to_datetime(df['dealDate']).dt.date)
    df = fill_null_values_categorical(df)  # fill null value with "EMPTY" string
    df = convert_categorical_columns_to_uppercase(df)  # convert all categorical columns to uppercase string

    # extract reason of rejects
    null_value = 'EMPTY'
    df['reasonOfReject'] = df['reasonOfReject'].apply(lambda r: extract_reason_of_rejects(r))
    df['reasonOfReject'] = df['reasonOfReject'].apply(lambda r: null_value if r == '' else r)
    df['reasonOfReject'] = df['reasonOfReject'].fillna("EMPTY")

    # fill remaining column null values
    fill_null_values_categorical(df, 'EMPTY', ['clientId'])

    assert not contains_null(df)  # assert no null values are still present

    # _____ 3 - Save file _____
    logger.debug(f'Total entries before saving to csv: {len(df)}')
    output_path = 'data/02_cleaned'
    df.to_csv(f'{output_path}/{table_name}.csv', index=False) if to_csv else None

    # _____ 4 - Plotting ______
    # plotting()

    logger.info(f'======== end {table_name} cleansing ========\n')


def clean_cockpit_table(to_csv=False, override=False):
    def plotting():
        categoricals = account_df.loc[:, account_df.dtypes == object].columns.values

        # top sales desks
        n = 20
        fig = plt.figure(figsize=(15, 5))
        ax = fig.gca()
        ax.title.set_text(f'Top {n} sales desks')
        account_df['salesDeskId'].value_counts()[:n].plot(ax=ax, kind='bar')
        plt.show()

        # top clients type
        len(client_df['clientType'].unique())

        n = len(client_df['clientType'].unique())
        fig = plt.figure(figsize=(15, 5))
        ax = fig.gca()
        ax.title.set_text(f'Top {n} client types')
        client_df['clientType'].value_counts()[:n].plot(ax=ax, kind='bar')
        plt.show()

    # _____ 0 - Override _____
    if not override and f"cockpitAccountMetadata.csv" in os.listdir(
            'data/02_cleaned') and f"cockpitClientMetadata.csv" in os.listdir('data/02_cleaned'):
        logger.info(f'======== Cockpit tables cleansing skipped. File already present ========\n')
        return None

    # _____ 1 - Exploration ______
    folder = 'data/01_raw'
    table_name = 'cockpitAccountMetadata'
    account_df = load(folder=folder, table=table_name, encoding='ISO-8859-1')
    table_name = 'cockpitClientMetadata'
    client_df = load(folder=folder, table=table_name, encoding='ISO-8859-1')

    logger.info(f'\n======== start Cockpit tables cleansing ========\n')

    # ___ initial cleaning ___
    logger.debug('Total cockpitClientMetadata entries before cleansing: {}'.format(len(client_df)))
    logger.debug('Total cockpitAccountMetadata entries before cleansing: {}'.format(len(account_df)))
    print_null_counting(client_df)  # print columns that contain null values - null count
    print_null_counting(account_df)  # print columns that contain null values - null count

    account_df = base_cleanup(account_df)
    account_df = drop_rows_with_nan_mandatory_cols(account_df, mandatory_cols=[])  # drop rows for which mandatory_cols are nan
    account_df = remove_columns_that_contains_only_one_value(account_df)  # remove columns for which all records have the same value

    client_df = base_cleanup(client_df)
    client_df = drop_rows_with_nan_mandatory_cols(client_df, mandatory_cols=[])  # drop rows for which mandatory_cols are nan
    client_df = remove_columns_that_contains_only_one_value(client_df)  # remove columns for which all records have the same value

    logger.debug('Total cockpitClientMetadata entries after cleanup: {}'.format(len(client_df)))
    logger.debug('Total cockpitAccountMetadata entries after cleanup: {}'.format(len(account_df)))
    print_null_counting(client_df)  # print columns that contain null values - null count
    print_null_counting(account_df)  # print columns that contain null values - null count

    # _____ 2 - Preprocessing ______
    """
    Perform a data preprocessing based on actual dataset, e.g.,:

    remove useless columns from a functional point of view
    ignore records if a column is mandatory
    fill null values
    etc..
    """

    # ___ remove useless columns ___
    useless = ['exceedTime']
    account_df = account_df.drop(useless, axis=1, errors='ignore')
    client_df = client_df.drop(useless, axis=1, errors='ignore')

    # convert columns to obj
    account_df['IeAccountId'] = account_df['IeAccountId'].astype('object')
    account_df['ndg'] = account_df['ndg'].astype('object')
    account_df['salesDeskId'] = account_df['salesDeskId'].astype('object')

    # fill null values for remaining columns
    account_df = fill_null_values_categorical(account_df, cols=['IeAccountId', 'ndg', 'salesDeskId'])
    client_df = fill_null_values_categorical(client_df, cols=['clientType'])

    client_df = convert_categorical_columns_to_uppercase(client_df)  # convert all categorical columns to uppercase string
    account_df = convert_categorical_columns_to_uppercase(account_df)  # convert all categorical columns to uppercase string

    # assert no null values are still present
    assert not contains_null(account_df)
    assert not contains_null(client_df)

    # for each account/client keeps the last received record - considering time column
    account_df = account_df.sort_values('time', ascending=False).groupby('accountId', as_index=False).first().reset_index(drop=True)
    client_df = client_df.sort_values('time', ascending=False).groupby('clientId', as_index=False).first().reset_index(drop=True)

    logger.debug('Total cockpitClientMetadata entries after keeping only the last received record: {}'.format(len(client_df)))
    logger.debug('Total cockpitAccountMetadata entries after keeping only the last received record: {}'.format(len(account_df)))

    # merge account and client data into a single dataframe
    # TODO: need to use cockpitClientHistory or something similar

    # _____ 3 - Save file _____
    output_path = 'data/02_cleaned'

    account_df.to_csv(f'{output_path}/cockpitAccountMetadata.csv', index=False) if to_csv else None
    client_df.to_csv(f'{output_path}/cockpitClientMetadata.csv', index=False) if to_csv else None

    # _____ 4 - Plotting ______
    # plotting()


if __name__ == '__main__':
    clean_360t_tables(override=False)
    clean_exceed_tables(override=False)