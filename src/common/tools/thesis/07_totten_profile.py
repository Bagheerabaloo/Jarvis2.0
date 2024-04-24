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


OVERRIDE_PREPARATION = False

METRIC = Metric.INCREMENT
SCALE = True
K_MEANS = True
HAC = False
DBSCAN = False
PLOT_ALL = False
SILHOUETTE = False
PLOT_FOLDER = 'plots'
ANALYSIS_NAME = 'Solo clienti CEEMEA con profilo di Totten'


# _____ Profile Analysis_____
def profile_analysis_flowdb(df):
    def scale_func_1(df_):

        df_['MI0'] = df_['MI0'] / 2
        df_['MI1'] = df_['MI1'] / 2
        df_['MI5'] = df_['MI5'] / 3
        df_['MI30'] = df_['MI30'] / 4
        df_['MI60'] = df_['MI60'] / 5
        df_['MI120'] = df_['MI120'] / 6
        df_['MI180'] = df_['MI180'] / 7
        df_['MI300'] = df_['MI300'] / 8
        df_['MI600'] = df_['MI600'] / 9

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
            k_means_df['MI600'] = k_means_df['MI600'] * 9

            clusters_df['MI120'] = clusters_df['MI120'] * 6
            clusters_df['MI180'] = clusters_df['MI180'] * 7
            clusters_df['MI300'] = clusters_df['MI300'] * 8
            clusters_df['MI600'] = clusters_df['MI600'] * 9

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
                             plot_parallel_all=PLOT_ALL, run_silhouette=SILHOUETTE, manually_func=manually_func)
        if HAC:
            hac_analysis(df=profile_df_, folder_path=folder_path_, columns=default_columns, index_columns=aggregation, plot_parallel_all=PLOT_ALL)
        if DBSCAN:
            dbscan_analysis(df=profile_df_, folder_path=folder_path_, columns=default_columns, index_columns=aggregation, plot_parallel_all=PLOT_ALL)

    # ___ 1 - Metric and Scaling ___
    manual_func = None
    if METRIC == Metric.INCREMENT:
        g10_cols = ['MI0', 'MI1', 'MI5', 'MI30', 'MI60']
        ceemea_cols = ['MI0', 'MI1', 'MI5', 'MI30', 'MI60', 'MI120', 'MI180', 'MI300', 'MI600']
        if SCALE:
            # scaled_df = min_max_scaler(df, columns=['MI0', 'MI1', 'MI5', 'MI30', 'MI60', 'MI120', 'MI180', 'MI300', 'MI600'], feature_range=(-1, 1))
            df = scale_func_1(df)
            manual_func = rescale_func_1
    elif METRIC == Metric.NORM:
        g10_cols = ['MN0', 'MN1', 'MN5', 'MN30', 'MN60']
        ceemea_cols = ['MN0', 'MN1', 'MN5', 'MN30', 'MN60', 'MN120', 'MN180', 'MN300', 'MN600']
    else:
        g10_cols = ['M0', 'M1', 'M5', 'M30', 'M60']
        ceemea_cols = ['M0', 'M1', 'M5', 'M30', 'M60', 'M120', 'M180', 'M300', 'M600']

    folder_name = timestamp2date(time(), frmt='%m_%d_%H%M%S') + ' TottenCluster {}{}'.format(METRIC, ' Scaled' if SCALE else '')

    # ___ 2 - PREPARATION ___
    df = df.drop(columns=['accountId', 'clientId', 'clientName', 'espTrades', 'isBuy', 'isSell', 'on360t', 'onApril', 'onAugust', 'onBloomberg', 'onDecember', 'onFastmatch',
                          'onFebruary', 'onFriday', 'onFxall', 'onJanuary', 'onJuly', 'onJune', 'onMarch', 'onMay', 'onMonday', 'onNovember', 'onOctober', 'onSaturday', 'onSeptember',
                          'onSunday', 'onThursday', 'onTobo', 'onTuesday', 'onUct', 'onUfx', 'onWednesday'], errors='ignore')
    logger.info("Entries after preparation: {}".format(len(df)))

    # ___ 3 - PROFILE ANALYSIS ___
    if folder_name not in os.listdir(PLOT_FOLDER):
        os.mkdir('{}/{}'.format(PLOT_FOLDER, folder_name))

    file_write(path=f'{PLOT_FOLDER}/{folder_name}/conf.txt', data=json.dumps({'Override': OVERRIDE_PREPARATION, 'Silhouette': SILHOUETTE, 'Kmeans': K_MEANS,
                                                                              'hac': HAC, 'dbscan': DBSCAN, 'scale': SCALE, 'PlotAll': PLOT_ALL,
                                                                              'Metric': METRIC.value, 'Analysis': ANALYSIS_NAME}))

    folder_path = '{}/{}/{}'.format(PLOT_FOLDER, folder_name, 'Markout_incrementer')
    os.mkdir(folder_path)

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

    analysis_folder = '07_totten_profile'
    reference_aggregation = ['client', 'pair', 'productType', 'platform', 'pnlTotalPerM', 'pnlMatchUSD', 'M0']

    if analysis_folder not in os.listdir('data/'):
        os.mkdir('data/{}'.format(analysis_folder))

    analysis_folder = 'data/{}'.format(analysis_folder)

    # ___ 0 - LOAD dataFrame ___
    df = load(table='exceed', folder='data/05_profile_analysis_flowDB')
    logger.info("Initial entries: {}".format(len(df)))

    limit = 20000
    df = df[(df['M0'].abs() < limit) & (df['M1'].abs() < limit) & (df['M5'].abs() < limit) & (df['M30'].abs() < limit) & (df['M60'].abs() < limit)]
    # scatter_plot_dataframe(df[['M5', 'M30', 'M60']])
    # scatter_plot_dataframe(df[['M0', 'M1', 'M5', 'M30', 'M60']])

    # ___ 1 - FILTER dataFrame ___
    df = df[df['CoConfirmedTrades'] > 10]  # filter out clients/pair that have less than 10 trades
    df = df[df['platform'] != 'UCT']  # filter out UCT trades
    df = df[df['platform'] != 'UFX']  # filter out UFX trades
    # df = df[df['pnlTotalPerM'] < 0]  # filter for negative pnl
    # df = df[df['pnlMatchUSD'] < 0]  # filter for negative match pnl
    # df = df[df['M0'] < 0]  # filter for negative Markout at time T0
    # df = df[df['M0'] > 0]  # filter for positive Markout at time T0

    # scatter_plot_dataframe(df[['M5', 'M30', 'M60']])
    # scatter_plot_dataframe(df[['M0', 'M1', 'M5', 'M30', 'M60']])

    # ___ 3 - Totten cluster ___
    filtered_df = df[(df['M0'] > 0) & (df['M1'] < 0) & (df['M5'] < 0) & (df['M30'] > 0)]
    filtered_df['label'] = 0

    columns = ['M0', 'M1', 'M5', 'M30', 'M60', 'M120']
    centroids_k_df = pd.DataFrame()
    for label in sorted(list(filtered_df['label'].unique())):
        centroids_k_df = centroids_k_df.append(pd.DataFrame(filtered_df[filtered_df['label'] == label][columns + ['label']].mean()).transpose(), ignore_index=True)
    centroids_k_df['label'] = centroids_k_df['label'].astype(int)

    # fig = parallel_coordinates_plot(centroids_k_df, 'label')
    # fig.show()
    # fig = parallel_coordinates_plot(filtered_df[['MI0', 'MI1', 'MI5', 'MI30', 'MI60', 'MI120', 'label']], 'label')
    # fig.show()
    # fig = parallel_coordinates_plot(filtered_df[['M0', 'M1', 'M5', 'M30', 'M60', 'M120', 'label']], 'label')
    # fig.show()

    clients_df = filtered_df[['client', 'pair', 'productType']].drop_duplicates().sort_values(by=['client', 'pair']).reset_index()

    g10_ccys = ['USD', 'EUR', 'GBP', 'JPY', 'AUD', 'NZD', 'CAD', 'CHF', 'NOK', 'SEK', 'DKK']
    g10 = [pair for pair in filtered_df['pair'].unique() if pair[:3].upper() in g10_ccys and pair[3:].upper() in g10_ccys]
    clients_df = clients_df[~clients_df['pair'].isin(g10)]

    filtered_df = pd.merge(clients_df, df, 'inner', ['client', 'pair', 'productType'])
    filtered_df['label'] = 0

    columns = ['M0', 'M1', 'M5', 'M30', 'M60', 'M120']
    centroids_k_df = pd.DataFrame()
    for label in sorted(list(filtered_df['label'].unique())):
        centroids_k_df = centroids_k_df.append(pd.DataFrame(filtered_df[filtered_df['label'] == label][columns + ['label']].mean()).transpose(), ignore_index=True)
    centroids_k_df['label'] = centroids_k_df['label'].astype(int)

    # fig = parallel_coordinates_plot(centroids_k_df, 'label')
    # fig.show()

    filtered_df['isG10'] = filtered_df['pair'].apply(lambda x: x.upper() in g10)

    limit = 20000
    df = filtered_df
    df = df[(df['M0'].abs() < limit) & (df['M1'].abs() < limit) & (df['M5'].abs() < limit) & (df['M30'].abs() < limit) & (df['M60'].abs() < limit)]
    df = df[(df['MN120'].abs() < 5) & (df['MN180'].abs() < 5) & (df['MN300'].abs() < 5) & (df['MN600'].abs() < 5)]

    profile_analysis_flowdb(df)

    print('end')