import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from src.utils import pretty_print
from src.utils_dataframe import *
from src.utils import *
from src.ml_clustering import *
from src.matplotLib_plot import *
from src.plot_seaborn import Seaborn
from time import time
from src.utils import timestamp2date
from enum import Enum
import json

pd.options.display.max_columns = None  # initial setup
logger = init_logger(name='Preparation')


class Metric(Enum):
    INCREMENT = 'INCREMENT'
    NORM = 'NORM'
    BINARY = 'BINARY'
    DIGITAL = 'DIGITAL'
    PERCENTAGE = 'PERCENTAGE'


OVERRIDE_PREPARATION = True

# METRICS = [Metric.DIGITAL, Metric.NORM, Metric.INCREMENT]
METRICS = [Metric.NORM]
SCALE = False
K_MEANS = True
K_MEANS_MIN = 2
K_MEANS_MAX = 6
HAC = False
DBSCAN = False
PLOT_ALL = False
SILHOUETTE = True
PAIR_ANALYSIS = False
PLOT_FOLDER = 'plots'
ANALYSIS_NAME = 'FlowDB Only positive M all'


# _____ Extract and Group by Quarter _____
def extract_quarter(week):
    # TODO: 1) verify if there is a better way to group weeks into quarters 2) verify bug output different from same input (not idempotente)
    return 4 if week == 53 else (week - 1) // 13 + 1


# _____ ExCEED preparation _____
def load_exceed(folder):
    if OVERRIDE_PREPARATION or f"exceed.csv" not in os.listdir(folder):

        # ___ Load initial dataFrame ___
        flowdb = load_flowdb(folder)
        client_order = load_client_order(folder)
        df = pd.merge(flowdb, client_order, on=['client', 'pair', 'productType'], how='left')  # merge flowDB and clientOrder dataFrames

        # ___ Save prepared dataFrame ___
        logger.info(f"Saving to: {folder}/exceed.csv")
        df.to_csv(f'{folder}/exceed.csv', index=False)

    else:
        df = load(table='exceed', folder=folder)
        logger.info("Initial entries: {}".format(len(df)))

    return df


# _____ FlowDB Preparation _____
def load_flowdb(folder):
    table_name = 'flowDB'
    if OVERRIDE_PREPARATION or f"{table_name}.csv" not in os.listdir(folder):

        # ___ Load initial dataFrame ___
        df = load(table=table_name, folder='data/02_cleaned', date_columns=['time', 'date', 'dealTime', 'tradeDate', 'valueDate'])
        logger.info("Initial entries: {}".format(len(df)))

        # ___ Add metrics to dataFrame ___
        df = prepare_flowdb(df)
        logger.debug('Total entries after preparation: {}'.format(len(df)))

        # ___ Save prepared dataFrame ___
        logger.info(f"Saving to: {folder}/{table_name}.csv")
        df.to_csv(f'{folder}/{table_name}.csv', index=False)

    else:
        df = load(table=table_name, folder=folder)
        logger.info("Initial entries: {}".format(len(df)))

    return df


def prepare_flowdb(df_):
    time_column = 'date'

    logger.info(f'======== start flowDB preparation ========')

    # Drop copies of column date. Date is the only column we need as reference. ValueDate can be instead interesting as it contains the information about the tenor
    df_ = df_.drop(columns=['time', 'dealTime', 'tradeDate', 'clientAllInRate', 'clientSpotRate', 'externalId', 'externalTraderId', 'orderLegId',
                            'riskAllInRate', 'amount', 'dealAmount', 'riskSpotRate', 'spotDate', 'subDealType', 'ticketType', 'traderId',
                            'externalAccountId', 'ecn', 'failreason', 'proxyFullName', 'proxyGroupFullName', 'side', 'spotCentre', 'streamType',
                            'swapCentre', 'tenor', 'valueDate'], errors='ignore')

    # ___ 2 - PRE-PROCESSING ___
    df_ = df_.drop_duplicates(subset='dealID', keep="last", inplace=False)  # keep only one single record for each `dealID`
    logger.debug('Total entries after dealID filtering : {}'.format(len(df_)))

    # extract week - will be used to join with windowed volumes
    df_['weekOfYear'] = df_[time_column].dt.isocalendar().week
    df_['year'] = df_[time_column].dt.isocalendar().year
    df_['quarter'] = df_['weekOfYear'].apply(lambda x: extract_quarter(x))

    df_ = df_.drop(columns=['date'], errors='ignore')

    # Exclude trades where riskAmount = 0, because it's impossible to calculate markout% --> inf
    df_ = df_[df_['riskAmount'] > 0]

    df_ = df_[df_['markoutUSD_0'].notna()]

    df_ = df_.rename(columns={"externalClientId": "client", "sym": "pair", "dealType": "productType"}, errors='ignore')

    df_ = df_.rename(columns={"markoutUSD_0": "M0", "markoutUSD_1": "M1", "markoutUSD_5": "M5", "markoutUSD_30": "M30", "markoutUSD_60": "M60",
                              "markoutUSD_120": "M120", "markoutUSD_180": "M180", "markoutUSD_300": "M300", "markoutUSD_600": "M600", }, errors='ignore')

    df_ = add_markout_metrics(df_)

    logger.info(f'======== end flowDB preparation ========\n')

    return df_


def extract_incrementer(x, prev_col, next_col, prev_incr_col, prev_max, prev_min):
    incr = 0 if prev_incr_col is None else x[prev_incr_col]
    prev_ = x[prev_col]
    next_ = x[next_col]

    if next_ > prev_max and prev_ >= 0:
        incr += 1
    elif next_ >= 0 > prev_:
        incr = 1
    elif next_ < prev_min and prev_ < 0:
        incr -= 1
    elif next_ < 0 <= prev_:
        incr = -1

    return incr, max(prev_max, next_), min(prev_min, next_)


def add_markout_metrics(df_):
    # ___ 1 - calculate FlowDB Markout incremental metric ___
    df_['MI0'] = df_.apply(lambda x: 1 if x['M0'] >= 0 else -1, axis=1)
    df_[['MI1', 'Max', 'Min']] = df_.apply(lambda x: extract_incrementer(x, 'M0', 'M1', 'MI0', x['M0'], x['M0']), axis=1, result_type='expand')
    df_[['MI5', 'Max', 'Min']] = df_.apply(lambda x: extract_incrementer(x, 'M1', 'M5', 'MI1', x['Max'], x['Min']), axis=1, result_type='expand')
    df_[['MI30', 'Max', 'Min']] = df_.apply(lambda x: extract_incrementer(x, 'M5', 'M30', 'MI5', x['Max'], x['Min']), axis=1, result_type='expand')
    df_[['MI60', 'Max', 'Min']] = df_.apply(lambda x: extract_incrementer(x, 'M30', 'M60', 'MI30', x['Max'], x['Min']), axis=1, result_type='expand')
    df_[['MI120', 'Max', 'Min']] = df_.apply(lambda x: extract_incrementer(x, 'M60', 'M120', 'MI60', x['Max'], x['Min']), axis=1, result_type='expand')
    df_[['MI180', 'Max', 'Min']] = df_.apply(lambda x: extract_incrementer(x, 'M120', 'M180', 'MI120', x['Max'], x['Min']), axis=1, result_type='expand')
    df_[['MI300', 'Max', 'Min']] = df_.apply(lambda x: extract_incrementer(x, 'M180', 'M300', 'MI180', x['Max'], x['Min']), axis=1, result_type='expand')
    df_[['MI600', 'Max', 'Min']] = df_.apply(lambda x: extract_incrementer(x, 'M300', 'M600', 'MI300', x['Max'], x['Min']), axis=1, result_type='expand')
    df_ = df_.drop(columns=['Max', 'Min'])

    # ___ 2 - calculate FlowDB scaled markout normalized on each row (up to M60)
    copied_df = df_[['M0', 'M1', 'M5', 'M30', 'M60']]
    copied_df = df_[['M0', 'M1', 'M5', 'M30', 'M60', 'M120', 'M180', 'M300', 'M600']].div(copied_df.abs().max(axis=1), axis=0)
    copied_df = copied_df.rename(columns={"M0": "MN0", "M1": "MN1", "M5": "MN5", "M30": "MN30", "M60": "MN60", "M120": "MN120",
                                          "M180": "MN180", "M300": "MN300", "M600": "MN600"})
    df_ = df_.join(copied_df)
    # df_ = df_[(df_['MN120'].abs() < 5) & (df_['MN180'].abs() < 5) & (df_['MN300'].abs() < 5) & (df_['MN600'].abs() < 5)]

    # ___ 3 - calculate FlowDB binary markout ___
    cols = ['M0', 'M1', 'M5', 'M30', 'M60', 'M120', 'M180', 'M300', 'M600']
    output_cols = ['MB0', 'MB1', 'MB5', 'MB30', 'MB60', 'MB120', 'MB180', 'MB300', 'MB600']
    for index, _ in enumerate(cols):
        df_[output_cols[index]] = df_[cols[index]].apply(lambda x: 1 if x >= 0 else -1)

    # ___ 4 - calculate FlowDB digital markout ___
    def digital(row):
        if row >= 50:
            return 1.5
        elif row >= 0:
            return 0.5
        elif row >= -50:
            return -0.5
        else:
            return -1.5

    cols = ['M0', 'M1', 'M5', 'M30', 'M60', 'M120', 'M180', 'M300', 'M600']
    output_cols = ['MD0', 'MD1', 'MD5', 'MD30', 'MD60', 'MD120', 'MD180', 'MD300', 'MD600']
    for index, _ in enumerate(cols):
        df_[output_cols[index]] = df_[cols[index]].apply(lambda x: digital(x))

    # ___ 5 - calculate FlowDB percentage markout ___
    cols = ['M0', 'M1', 'M5', 'M30', 'M60', 'M120', 'M180', 'M300', 'M600']
    output_cols = ['MP1', 'MP5', 'MP30', 'MP60', 'MP120', 'MP180', 'MP300', 'MP600']
    for index, _ in enumerate(output_cols):
        df_[output_cols[index]] = (df_[cols[index + 1]] - df_[cols[index]]) / df_[cols[index]].abs()

    return df_


# _____ ClientOrder Preparation _____
def load_client_order(folder):
    table_name = 'clientOrder'
    if OVERRIDE_PREPARATION or f"{table_name}.csv" not in os.listdir(folder):

        # ___ Load initial dataFrame ___
        df = load(table=table_name, folder='data/02_cleaned', date_columns=['date'])
        logger.info("Initial entries: {}".format(len(df)))

        # ___ Add metrics to dataFrame ___
        df = prepare_client_order(df)
        logger.debug('Total entries after preparation: {}'.format(len(df)))

        # ___ Save prepared dataFrame ___
        logger.info(f"Saving to: {folder}/{table_name}.csv")
        df.to_csv(f'{folder}/{table_name}.csv', index=False)

    else:
        df = load(table=table_name, folder=folder)
        logger.info("Initial entries: {}".format(len(df)))

    return df


def prepare_client_order(df):
    def is_confirmed(order_state: str) -> int:
        """ return 1 if order_state is CONFIRMED, otherwise 0 """
        return 1 if order_state == 'CONFIRMED' else 0

    def aggregate(df_: pd.DataFrame, aggregation_: list) -> pd.DataFrame:
        """ aggregates by `aggregation` param and computes several volumes """

        df_ = df_.groupby(aggregation_).agg(
            CoTradesCount=('externalClientId', 'count'),
            CoConfirmedTrades=('isConfirmed', 'sum'),
            CoEURAmount=('riskAmount', 'sum')
        ).reset_index()

        return df_

    aggregation = ['externalClientId', 'sym', 'dealType']
    table_name = 'clientOrder'

    logger.info(f'======== start {table_name} preparation ========')

    # ___ 2 - PRE-PROCESSING ___
    df['isConfirmed'] = df['orderState'].apply(lambda val: is_confirmed(val))  # extract if a trade is CONFIRMED or not

    aggregated_df = aggregate(df, aggregation)
    aggregated_df['hitRatio'] = aggregated_df['CoConfirmedTrades'] / aggregated_df['CoTradesCount']
    aggregated_df = aggregated_df.rename(columns={"externalClientId": "client", "sym": "pair", "dealType": "productType"}, errors='ignore')

    logger.info(f'======== end {table_name} preparation ========')

    return aggregated_df


# _____ Profile Analysis_____
def profile_analysis_flowdb(df, metric_):
    def scale_func_1(df_):

        df_['MI0'] = df_['MI0'] / 2
        df_['MI1'] = df_['MI1'] / 2
        df_['MI5'] = df_['MI5'] / 3
        df_['MI30'] = df_['MI30'] / 4
        df_['MI60'] = df_['MI60'] / 5
        df_['MI120'] = df_['MI120'] / 6
        df_['MI180'] = df_['MI180'] / 7
        df_['MI300'] = df_['MI300'] / 8
        # df_['MI600'] = df_['MI600'] / 9

        return df_

    def rescale_func_1(k_means_df, clusters_df):

        k_means_df['MI0'] = k_means_df['MI0'] * 2
        k_means_df['MI1'] = k_means_df['MI1'] * 2
        k_means_df['MI5'] = k_means_df['MI5'] * 3
        k_means_df['MI30'] = k_means_df['MI30'] * 4
        k_means_df['MI60'] = k_means_df['MI60'] * 5

        clusters_df['MI0'] = clusters_df['MI0'] * 2
        clusters_df['MI1'] = clusters_df['MI1'] * 2
        clusters_df['MI5'] = clusters_df['MI5'] * 3
        clusters_df['MI30'] = clusters_df['MI30'] * 4
        clusters_df['MI60'] = clusters_df['MI60'] * 5

        if 'MI120' in k_means_df.columns:
            k_means_df['MI120'] = k_means_df['MI120'] * 6
            k_means_df['MI180'] = k_means_df['MI180'] * 7
            k_means_df['MI300'] = k_means_df['MI300'] * 8
            # k_means_df['MI600'] = k_means_df['MI600'] * 9

            clusters_df['MI120'] = clusters_df['MI120'] * 6
            clusters_df['MI180'] = clusters_df['MI180'] * 7
            clusters_df['MI300'] = clusters_df['MI300'] * 8
            # clusters_df['MI600'] = clusters_df['MI600'] * 9

        return k_means_df, clusters_df

    # ___ Analysis functions ___
    def analysis(profile_df_, aggregation, folder_path_, default_columns, folder_, pair_=None, manually_func=None):

        if len(profile_df_) == 0:
            return

        if folder_ not in os.listdir(folder_path_):
            os.mkdir('{}/{}'.format(folder_path_, folder_))

        if pair_ is not None:
            if pair_ not in os.listdir("{}/{}".format(folder_path_, folder_)):
                os.mkdir('{}/{}/{}'.format(folder_path_, folder_, pair_))

            aggregation = [x for x in aggregation if x != 'pair_']
            folder_path_ = '{}/{}/{}'.format(folder_path_, folder_, pair_)
            profile_df_ = filter_by_value(profile_df_, 'pair_', pair_)
        else:
            folder_path_ = '{}/{}'.format(folder_path_, folder_)

        if K_MEANS:
            k_means_analysis(df=profile_df_, folder_path=folder_path_, columns=default_columns, index_columns=aggregation,
                             plot_parallel_all=PLOT_ALL, run_silhouette=SILHOUETTE, manually_func=manually_func, min_k=K_MEANS_MIN, max_k=K_MEANS_MAX)
        if HAC:
            hac_analysis(df=profile_df_, folder_path=folder_path_, columns=default_columns, index_columns=aggregation, plot_parallel_all=PLOT_ALL)
        if DBSCAN:
            dbscan_analysis(df=profile_df_, folder_path=folder_path_, columns=default_columns, index_columns=aggregation, plot_parallel_all=PLOT_ALL)

    # ___ 1 - Metric and Scaling ___
    manual_func = None
    if metric_ == Metric.INCREMENT:
        g10_cols = ['MI0', 'MI1', 'MI5', 'MI30', 'MI60']
        ceemea_cols = ['MI0', 'MI1', 'MI5', 'MI30', 'MI60', 'MI120', 'MI180', 'MI300']
        if SCALE:
            # scaled_df = min_max_scaler(df, columns=['MI0', 'MI1', 'MI5', 'MI30', 'MI60', 'MI120', 'MI180', 'MI300'], feature_range=(-1, 1))
            df = scale_func_1(df)
            manual_func = rescale_func_1
    elif metric_ == Metric.NORM:
        g10_cols = ['MN0', 'MN1', 'MN5', 'MN30', 'MN60']
        ceemea_cols = ['MN0', 'MN1', 'MN5', 'MN30', 'MN60', 'MN120', 'MN180', 'MN300']
        df = df[(df['MN120'].abs() < 2) & (df['MN180'].abs() < 2) & (df['MN300'].abs() < 2) & (df['MN600'].abs() < 2)]
    elif metric_ == Metric.BINARY:
        g10_cols = ['MB0', 'MB1', 'MB5', 'MB30', 'MB60']
        ceemea_cols = ['MB0', 'MB1', 'MB5', 'MB30', 'MB60', 'MB120', 'MB180', 'MB300']
    elif metric_ == Metric.DIGITAL:
        g10_cols = ['MD0', 'MD1', 'MD5', 'MD30', 'MD60']
        ceemea_cols = ['MD0', 'MD1', 'MD5', 'MD30', 'MD60', 'MD120', 'MD180', 'MD300']
    elif metric_ == Metric.PERCENTAGE:
        g10_cols = ['MP0', 'MP1', 'MP5', 'MP30', 'MP60']
        ceemea_cols = ['MP0', 'MP1', 'MP5', 'MP30', 'MP60', 'MP120', 'MP180', 'MP300']
    else:
        g10_cols = ['M0', 'M1', 'M5', 'M30', 'M60']
        ceemea_cols = ['M0', 'M1', 'M5', 'M30', 'M60', 'M120', 'M180', 'M300']

    folder_name = f"{timestamp2date(time(), frmt='%m_%d_%H%M%S')} - {ANALYSIS_NAME} - {metric_.value}"

    # ___ 2 - PREPARATION ___
    df = df.drop(columns=['accountId', 'clientId', 'clientName', 'espTrades', 'isBuy', 'isSell', 'on360t', 'onApril', 'onAugust', 'onBloomberg', 'onDecember', 'onFastmatch',
                          'onFebruary', 'onFriday', 'onFxall', 'onJanuary', 'onJuly', 'onJune', 'onMarch', 'onMay', 'onMonday', 'onNovember', 'onOctober', 'onSaturday', 'onSeptember',
                          'onSunday', 'onThursday', 'onTobo', 'onTuesday', 'onUct', 'onUfx', 'onWednesday'], errors='ignore')
    logger.info("Entries after preparation: {}".format(len(df)))

    # ___ 3 - PROFILE ANALYSIS ___
    if folder_name not in os.listdir(PLOT_FOLDER):
        os.mkdir('{}/{}'.format(PLOT_FOLDER, folder_name))

    file_write(path=f'{PLOT_FOLDER}/{folder_name}/conf.txt', data=json.dumps({'Override': OVERRIDE_PREPARATION, 'Silhouette': SILHOUETTE, 'Kmeans': K_MEANS,
                                                                              'hac': HAC, 'dbscan': DBSCAN, 'scale': SCALE, 'PlotAll': PLOT_ALL, 'Metric': metric_.value}))

    folder_path = '{}/{}'.format(PLOT_FOLDER, folder_name)

    # ___ Profile dataFrame ___
    profile_df = df[df['M0'].notna()][reference_aggregation + ceemea_cols]

    profile_df = profile_df.replace([np.inf, -np.inf], np.nan).dropna()
    profile_df['isG10'] = profile_df['pair'].isin(g10)

    # ___ NO PAIR ANALYSIS ___
    analysis(profile_df, folder_path_=folder_path, aggregation=reference_aggregation, default_columns=ceemea_cols,
             folder_='01_all', manually_func=manual_func)

    # ___ ONLY G10 ANALYSIS ___
    analysis(profile_df[profile_df['isG10']], folder_path_=folder_path, aggregation=reference_aggregation,
             default_columns=g10_cols, folder_='02_onlyG10', manually_func=manual_func)

    # ___ ONLY CEEMEA ANALYSIS ___
    analysis(profile_df[~profile_df['isG10']], folder_path_=folder_path, aggregation=reference_aggregation,
             default_columns=ceemea_cols, folder_='03_onlyCEEMEA', manually_func=manual_func)

    # ___ PAIR ANALYSIS ___
    if PAIR_ANALYSIS:
        pairs = profile_df.groupby('pair').agg('count').sort_values(by='client', ascending=False)
        pairs = list(pairs[pairs['client'] > 100]['client'].index)

        g10_pairs = [x for x in pairs if x in g10]
        ceemea_pairs = [x for x in pairs if x not in g10]

        for pair in g10_pairs:
            analysis(profile_df, pair_=pair, folder_path_=folder_path, aggregation=reference_aggregation, default_columns=g10_cols,
                     folder_='04_Pair_G10', manually_func=manual_func)

        for pair in ceemea_pairs:
            analysis(profile_df, pair_=pair, folder_path_=folder_path, aggregation=reference_aggregation,
                     default_columns=ceemea_cols, folder_='05_Pair_CEEMEA', manually_func=manual_func)


if __name__ == '__main__':

    # ___ 0 - Folder set-up ___
    analysis_folder = '05_profile_analysis_flowDB'
    reference_aggregation = ['client', 'pair', 'productType', 'platform', 'pnlTotalPerM', 'pnlMatchUSD', 'M0']

    if analysis_folder not in os.listdir('data/'):
        os.mkdir('data/{}'.format(analysis_folder))

    analysis_folder = 'data/{}'.format(analysis_folder)

    # ___ 1 - LOAD dataFrame ___
    df = load_exceed(analysis_folder)

    # ___ 1 - FILTER dataFrame ___
    limit = 20000
    df = df[(df['M0'].abs() < limit) & (df['M1'].abs() < limit) & (df['M5'].abs() < limit) & (df['M30'].abs() < limit) & (df['M60'].abs() < limit)]
    df = df[df['CoConfirmedTrades'] > 10]  # filter out clients/pair that have less than 10 trades
    df = df[df['platform'] != 'UCT']  # filter out UCT trades
    df = df[df['platform'] != 'UFX']  # filter out UFX trades
    # df = df[df['pnlTotalPerM'] < 0]  # filter for negative pnl
    # df = df[df['pnlMatchUSD'] < 0]  # filter for negative match pnl
    # df = df[df['M0'] < 0]  # filter for negative Markout at time T0
    # df = df[df['M0'] > 0]  # filter for positive Markout at time T0

    # scatter_plot_dataframe(df[['M5', 'M30', 'M60']])
    # scatter_plot_dataframe(df[['MP1', 'MP5', 'MP30', 'MP60']])

    # ___ 2 - G10 EXTRACTION ___
    g10_ccys = ['USD', 'EUR', 'GBP', 'JPY', 'AUD', 'NZD', 'CAD', 'CHF', 'NOK', 'SEK', 'DKK']
    g10 = [pair for pair in df['pair'].unique() if pair[:3].upper() in g10_ccys and pair[3:].upper() in g10_ccys]
    df['isG10'] = df['pair'].apply(lambda x: x.upper() in g10)

    # ___ 3 - FILTER dataFrame ___
    n_df = df[~((df['isG10']) & (df['MB0'] == 1) & (df['MB1'] == 1) & (df['MB5'] == 1) & (df['MB30'] == 1) & (df['MB60'] == 1))]
    n_df = n_df[~((~n_df['isG10']) & (n_df['MB0'] == 1) & (n_df['MB1'] == 1) & (n_df['MB5'] == 1) & (n_df['MB30'] == 1) & (n_df['MB60'] == 1) & (n_df['MB120'] == 1) & (n_df['MB180'] == 1) & (n_df['MB300'] == 1))]

    p_df = df[((df['isG10']) & (df['MB0'] == 1) & (df['MB1'] == 1) & (df['MB5'] == 1) & (df['MB30'] == 1) & (df['MB60'] == 1)) |
              ((~df['isG10']) & (df['MB0'] == 1) & (df['MB1'] == 1) & (df['MB5'] == 1) & (df['MB30'] == 1) & (df['MB60'] == 1)
               & (df['MB120'] == 1) & (df['MB180'] == 1) & (df['MB300'] == 1))]

    # ___ 4 - Profile Analysis Plot ___
    for metric in METRICS:
        profile_analysis_flowdb(p_df, metric)

    assert len(df) == len(p_df) + len(n_df)

    g10_cols = ['MN0', 'MN1', 'MN5', 'MN30', 'MN60']
    ceemea_cols = ['MN0', 'MN1', 'MN5', 'MN30', 'MN60', 'MN120', 'MN180', 'MN300']
    reference_aggregation = ['client', 'pair', 'productType', 'platform']

    # ___ 1 - Negative Markout G10 ___
    profile_df = n_df[n_df['M0'].notna()][reference_aggregation + ceemea_cols]
    profile_df = profile_df.replace([np.inf, -np.inf], np.nan).dropna()
    profile_df['isG10'] = profile_df['pair'].isin(g10)
    profile_df = profile_df[profile_df['isG10']]

    n_clusters = 4
    k_means_df, clusters_df = k_means(profile_df, n_clusters=n_clusters, columns=g10_cols, index_columns=reference_aggregation)
    k_means_df = move_columns_to_first_position(k_means_df, reference_aggregation + g10_cols)

    fig = parallel_coordinates_plot(clusters_df, 'label')

    clusters_df['mean'] = clusters_df[[x for x in clusters_df.columns if x != 'label']].mean(axis=1)
    hard_sharp = str(clusters_df[(clusters_df['MN5'] < 0) & (clusters_df['MN30'] < 0)]['label'].iloc[0])
    sharp = str(clusters_df[(clusters_df['MN5'] > 0) & (clusters_df['MN30'] < 0)]['label'].iloc[0])
    soft_sharp = str(clusters_df[(clusters_df['MN30'] > 0) & (clusters_df['MN60'] < 0)]['label'].iloc[0])
    v_sharp = str(clusters_df[(clusters_df['MN5'] < 0) & (clusters_df['MN30'] > 0)]['label'].iloc[0])

    k_means_df['label'] = k_means_df['label'].astype(str)
    k_means_df['label'] = k_means_df['label'].str.replace(hard_sharp, 'HARD SHARP')
    k_means_df['label'] = k_means_df['label'].str.replace(sharp, 'SHARP')
    k_means_df['label'] = k_means_df['label'].str.replace(soft_sharp, 'SOFT_SHARP')
    k_means_df['label'] = k_means_df['label'].str.replace(v_sharp, 'V_SHARP')

    final_df = df.join(k_means_df['label'])  # merge initial dataFrame and profile dataFrame

    # ___ 2 - Negative Markout CEEMEA ___
    profile_df = n_df[n_df['M0'].notna()][reference_aggregation + ceemea_cols]
    profile_df = profile_df.replace([np.inf, -np.inf], np.nan).dropna()
    profile_df['isG10'] = profile_df['pair'].isin(g10)
    profile_df = profile_df[~profile_df['isG10']]
    profile_df = profile_df[(profile_df['MN120'].abs() < 2) & (profile_df['MN180'].abs() < 2) & (profile_df['MN300'].abs() < 2)]

    n_clusters = 4
    k_means_df, clusters_df = k_means(profile_df, n_clusters=n_clusters, columns=ceemea_cols, index_columns=reference_aggregation)
    k_means_df = move_columns_to_first_position(k_means_df, reference_aggregation + g10_cols)

    fig = parallel_coordinates_plot(clusters_df, 'label')

    clusters_df['mean'] = clusters_df[[x for x in clusters_df.columns if x != 'label']].mean(axis=1)

    valid_labels = sorted(list(clusters_df[clusters_df['mean'] < 0]['label'].values))

    hard_sharp = str(clusters_df[(clusters_df['MN5'] < 0)]['label'].iloc[0])
    sharp = str(clusters_df[(clusters_df['MN60'] > 0) & (clusters_df['MN120'] < 0)]['label'].iloc[0])
    soft_sharp = str(clusters_df[(clusters_df['MN180'] > 0) & (clusters_df['MN300'] < 0)]['label'].iloc[0])
    v_sharp = [str(x) for x in clusters_df['label'].unique() if str(x) not in [hard_sharp, sharp, soft_sharp]][0]

    k_means_df['label'] = k_means_df['label'].astype(str)
    k_means_df['label'] = k_means_df['label'].str.replace(hard_sharp, 'HARD SHARP')
    k_means_df['label'] = k_means_df['label'].str.replace(sharp, 'SHARP')
    k_means_df['label'] = k_means_df['label'].str.replace(soft_sharp, 'SOFT_SHARP')
    k_means_df['label'] = k_means_df['label'].str.replace(v_sharp, 'V_SHARP')

    final_df = final_df.join(k_means_df.rename(columns={'label': 'label2'})['label2'])
    final_df['label'] = final_df['label'].fillna(final_df['label2'])
    final_df = final_df.drop(columns=['label2'])

    # ___ 3 - Positive Markout G10 ___
    profile_df = p_df[p_df['M0'].notna()][reference_aggregation + ceemea_cols]
    profile_df = profile_df.replace([np.inf, -np.inf], np.nan).dropna()
    profile_df['isG10'] = profile_df['pair'].isin(g10)
    profile_df = profile_df[profile_df['isG10']]

    n_clusters = 2
    k_means_df, clusters_df = k_means(profile_df, n_clusters=n_clusters, columns=g10_cols, index_columns=reference_aggregation)
    k_means_df = move_columns_to_first_position(k_means_df, reference_aggregation + g10_cols)

    fig = parallel_coordinates_plot(clusters_df, 'label')

    unwise = str(clusters_df[(clusters_df['MN5'] < clusters_df['MN30']) & (clusters_df['MN30'] < clusters_df['MN60'])]['label'].iloc[0])
    flat = str(clusters_df[~((clusters_df['MN5'] < clusters_df['MN30']) & (clusters_df['MN30'] < clusters_df['MN60']))]['label'].iloc[0])

    k_means_df['label'] = k_means_df['label'].astype(str)
    k_means_df['label'] = k_means_df['label'].str.replace(unwise, 'UNWISE')
    k_means_df['label'] = k_means_df['label'].str.replace(flat, 'FLAT')

    final_df = final_df.join(k_means_df.rename(columns={'label': 'label2'})['label2'])
    final_df['label'] = final_df['label'].fillna(final_df['label2'])
    final_df = final_df.drop(columns=['label2'])

    # ___ 3 - Positive Markout CEEMEA ___
    profile_df = p_df[p_df['M0'].notna()][reference_aggregation + ceemea_cols]
    profile_df = profile_df.replace([np.inf, -np.inf], np.nan).dropna()
    profile_df['isG10'] = profile_df['pair'].isin(g10)
    profile_df = profile_df[~profile_df['isG10']]

    n_clusters = 2
    k_means_df, clusters_df = k_means(profile_df, n_clusters=n_clusters, columns=ceemea_cols, index_columns=reference_aggregation)
    k_means_df = move_columns_to_first_position(k_means_df, reference_aggregation + g10_cols)

    fig = parallel_coordinates_plot(clusters_df, 'label')

    unwise = str(clusters_df[(clusters_df['MN5'] < clusters_df['MN30']) & (clusters_df['MN30'] < clusters_df['MN60'])]['label'].iloc[0])
    flat = str(clusters_df[~((clusters_df['MN5'] < clusters_df['MN30']) & (clusters_df['MN30'] < clusters_df['MN60']))]['label'].iloc[0])

    k_means_df['label'] = k_means_df['label'].astype(str)
    k_means_df['label'] = k_means_df['label'].str.replace(unwise, 'UNWISE')
    k_means_df['label'] = k_means_df['label'].str.replace(flat, 'FLAT')

    final_df = final_df.join(k_means_df.rename(columns={'label': 'label2'})['label2'])
    final_df['label'] = final_df['label'].fillna(final_df['label2'])
    final_df = final_df.drop(columns=['label2'])

    # ___ Expand Label ---
    final_df = one_hot_encode(final_df, ['label'], drop_columns=True)

    # final_df = final_df.drop(columns=['M1', 'M120', 'M180', 'M30', 'M300', 'M5', 'M60', 'M600'], errors='ignore')
    final_df = final_df.drop(columns=['MB0', 'MB1', 'MB120', 'MB180', 'MB30', 'MB300', 'MB5', 'MB60', 'MB600'], errors='ignore')
    final_df = final_df.drop(columns=['MD0', 'MD1', 'MD120', 'MD180', 'MD30', 'MD300', 'MD5', 'MD60', 'MD600'], errors='ignore')
    final_df = final_df.drop(columns=['MI0', 'MI1', 'MI120', 'MI180', 'MI30', 'MI300', 'MI5', 'MI60', 'MI600'], errors='ignore')
    final_df = final_df.drop(columns=['MN0', 'MN1', 'MN120', 'MN180', 'MN30', 'MN300', 'MN5', 'MN60', 'MN600'], errors='ignore')
    final_df = final_df.drop(columns=['MP0', 'MP1', 'MP120', 'MP180', 'MP30', 'MP300', 'MP5', 'MP60', 'MP600'], errors='ignore')

    final_df = final_df.drop(columns=['clientId', 'dealID', 'dealRate', 'passfail'], errors='ignore')

    agg_df = final_df.groupby(['client', 'pair', 'productType']).agg(
        isHardsharp=('isHardsharp', 'sum'),
        isVSharp=('isVSharp', 'sum'),
        isSharp=('isSharp', 'sum'),
        isSoftSharp=('isSoftSharp', 'sum'),
        isFlat=('isFlat', 'sum'),
        isUnwise=('isUnwise', 'sum'),
        isNan=('isNan', 'sum'),
        M0=('M0', 'mean'),
        M1=('M1', 'mean'),
        M5=('M5', 'mean'),
        M30=('M30', 'mean'),
        M60=('M60', 'mean'),
        M120=('M120', 'mean'),
        M180=('M180', 'mean'),
        M300=('M300', 'mean')
    ).reset_index()

    agg_df['total'] = agg_df['isHardsharp'] + agg_df['isVSharp'] + agg_df['isSharp'] + agg_df['isSoftSharp'] + agg_df['isFlat'] + agg_df['isUnwise'] + agg_df['isNan']
    agg_df['isHardsharp%'] = agg_df['isHardsharp'] / agg_df['total'] * 100
    agg_df['isVSharp%'] = agg_df['isVSharp'] / agg_df['total'] * 100
    agg_df['isSharp%'] = agg_df['isSharp'] / agg_df['total'] * 100
    agg_df['isSoftSharp%'] = agg_df['isSoftSharp'] / agg_df['total'] * 100
    agg_df['isFlat%'] = agg_df['isFlat'] / agg_df['total'] * 100
    agg_df['isUnwise%'] = agg_df['isUnwise'] / agg_df['total'] * 100
    agg_df['isNan%'] = agg_df['isNan'] / agg_df['total'] * 100

    agg_df = agg_df.sort_values(by=['isHardsharp%', 'isSharp%', 'isSoftSharp%', 'isFlat%', 'isUnwise%'], ascending=False)

    agg_df = move_columns_to_first_position(agg_df, ['client', 'pair', 'productType', 'total', 'isHardsharp%', 'isVSharp%', 'isSharp%', 'isSoftSharp%',
                                                     'isFlat%', 'isUnwise%', 'isNan%', 'M0', 'M1', 'M5', 'M30', 'M60', 'M120', 'M180', 'M300'])

    agg_df.to_csv('data/07_classification/profiles.csv', index=False)

    print('end')

    # TODO: add other clustering methods to plot (hac, dbscan)
    # TODO: add valuation with PNL
    # TODO: ground truth -> after labeling, aggregation + counting

    # TODO: Client profiling - adjust code in this new python file
    # TODO: apply standard scaler before k-means (incremental) but keep original columns to calculate centroids
    # TODO: understand if to cluster per pair or not. In case we want to cluster per pair we must create a look-up table that associates each pair to the number of clusters to use
    # TODO: use a look-up table to associate each currency to the seconds needed to hedge
    # TODO: k-means and after aggregate by client pair and count the profiles

    # TODO: with all data available, group by clusters defined by Totten and cluster each of them
    # TODO: given that a client is unwise, can we decrease the spread for other pairs to win those trades?