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

pd.options.display.max_columns = None  # initial setup
logger = init_logger(name='Preparation')

PER_QUARTER = False
OVERRIDE_PREPARATION = False

PLOT_FOLDER = 'plots'
PLOT_ALL = False
TYPE_OF_ANALYSIS = 'QUARTER' if PER_QUARTER else 'WEEK'
FOLDER_NAME = timestamp2date(time(), frmt='%m_%d_%H_%M_%S') + ' {} Profile Analysis'.format(TYPE_OF_ANALYSIS)


# _____ Profile Analysis Preparation _____
def prepare(df_):
    df_ = add_client_relevance_metrics(df_)
    df_ = add_mark_to_market_metrics(df_)
    df_ = add_markout_metrics(df_)

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


def add_client_relevance_metrics(df_):
    df_['FQIsA'] = df_['FlowQuartileIsA'] / df_['FQLPRCount']
    df_['FQIsB'] = df_['FlowQuartileIsB'] / df_['FQLPRCount']
    df_['FQIsC'] = df_['FlowQuartileIsC'] / df_['FQLPRCount']
    df_['FQIsD'] = df_['FlowQuartileIsD'] / df_['FQLPRCount']

    df_['LPRIsA'] = df_['LPRelevanceIsA'] / df_['FQLPRCount']
    df_['LPRIsB'] = df_['LPRelevanceIsB'] / df_['FQLPRCount']
    df_['LPRIsC'] = df_['LPRelevanceIsC'] / df_['FQLPRCount']
    df_['LPRIsD'] = df_['LPRelevanceIsD'] / df_['FQLPRCount']
    # df_['LPRIsEmpty'] = df_['LPRelevanceIsEmpty'] / df_['FQLPRCount'] * 0

    df_['clientRelevance'] = df_['FQIsA'] * 4 + df_['FQIsB'] * 3 + df_['FQIsC'] * 2 + df_['FQIsD']
    df_['clientRelevance'] = df_['clientRelevance'] - (df_['LPRIsA'] * 4 + df_['LPRIsB'] * 3 + df_['LPRIsC'] * 2 + df_['LPRIsD'])
    df_['clientRelevance'] = - df_['clientRelevance'] / 4

    # df_ = df_.drop(columns=['FlowQuartileIsA', 'FlowQuartileIsB', 'FlowQuartileIsC', 'FlowQuartileIsD', 'LPRelevanceIsA', 'LPRelevanceIsB', 'LPRelevanceIsC', 'LPRelevanceIsD', 'LPRelevanceIsEmpty',
    #                         'FQIsA', 'FQIsB', 'FQIsC', 'FQIsD', 'LPRIsA', 'LPRIsB', 'LPRIsC', 'LPRIsD', 'LPRIsEmpty'], errors='ignore')

    return df_


def add_mark_to_market_metrics(df_):
    df_['TI0'] = df_.apply(lambda x: 1 if x['T0'] >= 0 else -1, axis=1)
    df_[['TI1', 'Max', 'Min']] = df_.apply(lambda x: extract_incrementer(x, 'T0', 'T1', 'TI0', x['T0'], x['T0']), axis=1, result_type='expand')
    df_[['TI5', 'Max', 'Min']] = df_.apply(lambda x: extract_incrementer(x, 'T1', 'T5', 'TI1', x['Max'], x['Min']), axis=1, result_type='expand')
    df_[['TI10', 'Max', 'Min']] = df_.apply(lambda x: extract_incrementer(x, 'T5', 'T10', 'TI5', x['Max'], x['Min']), axis=1, result_type='expand')
    df_[['TI30', 'Max', 'Min']] = df_.apply(lambda x: extract_incrementer(x, 'T10', 'T30', 'TI10', x['Max'], x['Min']), axis=1, result_type='expand')
    df_[['TI60', 'Max', 'Min']] = df_.apply(lambda x: extract_incrementer(x, 'T30', 'T60', 'TI30', x['Max'], x['Min']), axis=1, result_type='expand')
    df_[['TI120', 'Max', 'Min']] = df_.apply(lambda x: extract_incrementer(x, 'T60', 'T120', 'TI60', x['Max'], x['Min']), axis=1, result_type='expand')
    df_[['TI180', 'Max', 'Min']] = df_.apply(lambda x: extract_incrementer(x, 'T120', 'T180', 'TI120', x['Max'], x['Min']), axis=1, result_type='expand')
    df_[['TI240', 'Max', 'Min']] = df_.apply(lambda x: extract_incrementer(x, 'T180', 'T240', 'TI180', x['Max'], x['Min']), axis=1, result_type='expand')
    df_[['TI300', 'Max', 'Min']] = df_.apply(lambda x: extract_incrementer(x, 'T240', 'T300', 'TI240', x['Max'], x['Min']), axis=1, result_type='expand')

    return df_


def add_markout_metrics(df_):
    # __ calculate FlowDB Markout incremental metric
    df_['MI0'] = df_.apply(lambda x: 1 if x['M0'] >= 0 else -1, axis=1)
    df_[['MI1', 'Max', 'Min']] = df_.apply(lambda x: extract_incrementer(x, 'M0', 'M1', 'MI0', x['M0'], x['M0']), axis=1, result_type='expand')
    df_[['MI5', 'Max', 'Min']] = df_.apply(lambda x: extract_incrementer(x, 'M1', 'M5', 'MI1', x['Max'], x['Min']), axis=1, result_type='expand')
    df_[['MI30', 'Max', 'Min']] = df_.apply(lambda x: extract_incrementer(x, 'M5', 'M30', 'MI5', x['Max'], x['Min']), axis=1, result_type='expand')
    df_[['MI60', 'Max', 'Min']] = df_.apply(lambda x: extract_incrementer(x, 'M30', 'M60', 'MI30', x['Max'], x['Min']), axis=1, result_type='expand')
    df_[['MI120', 'Max', 'Min']] = df_.apply(lambda x: extract_incrementer(x, 'M60', 'M120', 'MI60', x['Max'], x['Min']), axis=1, result_type='expand')
    df_[['MI180', 'Max', 'Min']] = df_.apply(lambda x: extract_incrementer(x, 'M120', 'M180', 'MI120', x['Max'], x['Min']), axis=1, result_type='expand')
    df_[['MI300', 'Max', 'Min']] = df_.apply(lambda x: extract_incrementer(x, 'M180', 'M300', 'MI180', x['Max'], x['Min']), axis=1, result_type='expand')
    df_[['MI600', 'Max', 'Min']] = df_.apply(lambda x: extract_incrementer(x, 'M300', 'M600', 'MI300', x['Max'], x['Min']), axis=1, result_type='expand')

    return df_


# _____ Profile Analysis_____
def profile_analysis_360t(df):
    # ___ Aggregation functions ___

    def aggregate_by_incrementer(profile_df_, aggregation):

        aggregated_df = profile_df_.groupby(aggregation).agg(
            T0=('TI0', 'mean'),
            T1=('TI1', 'mean'),
            T5=('TI5', 'mean'),
            T10=('TI10', 'mean'),
            T30=('TI30', 'mean'),
            T60=('TI60', 'mean'),
            T120=('TI120', 'mean'),
            T180=('TI180', 'mean'),
            T240=('TI240', 'mean'),
            T300=('TI300', 'mean')
        ).reset_index()

        return aggregated_df

    # ___ Analysis functions ___
    def no_pair_analysis(profile_df_, aggregate_func_, aggregation, folder_path_, default_columns=None, folder_='01_NoPair'):

        # profile_eurusd_df = filter_by_value(profile_df_, 'Pair', 'EURUSD')

        if default_columns is None:
            default_columns = ['T0', 'T1', 'T5', 'T10', 'T30', 'T60', 'T120', 'T180', 'T240', 'T300']

        if folder not in os.listdir(folder_path_):
            os.mkdir('{}/{}'.format(folder_path_, folder_))

        folder_path_ = '{}/{}'.format(folder_path_, folder_)

        client_df = aggregate_func_(profile_df_, aggregation)

        k_means_analysis(df=client_df, folder_path=folder_path_, columns=default_columns, index_columns=aggregation, plot_parallel_all=PLOT_ALL)
        hac_analysis(df=client_df, folder_path=folder_path_, columns=default_columns, index_columns=aggregation, plot_parallel_all=PLOT_ALL)
        dbscan_analysis(df=client_df, folder_path=folder_path_, columns=default_columns, index_columns=aggregation, plot_parallel_all=PLOT_ALL)
        mean_shift_analysis(df=client_df, folder_path=folder_path_, columns=default_columns, index_columns=aggregation)

    def only_g10_analysis(profile_df_, folder_path_, aggregate_func_, aggregation):
        profile_df_ = profile_df_[profile_df_['isG10']]
        no_pair_analysis(profile_df_, folder_path_=folder_path_, aggregate_func_=aggregate_func_, aggregation=aggregation, default_columns=['T0', 'T1', 'T5', 'T10', 'T30', 'T60'], folder_='02_onlyG10')

    def only_ceemea_analysis(profile_df_, folder_path_, aggregate_func_, aggregation):
        profile_df_ = profile_df_[~profile_df_['isG10']]
        no_pair_analysis(profile_df_, folder_path_=folder_path_, aggregate_func_=aggregate_func_, aggregation=aggregation, folder_='03_onlyCEEMEA')

    def pair_analysis(profile_df_, pair, aggregate_func_, aggregation, folder_path_, default_columns=None, folder_='04_Pair'):

        if default_columns is None:
            default_columns = ['T0', 'T1', 'T5', 'T10', 'T30', 'T60', 'T120', 'T180', 'T240', 'T300']

        if folder_ not in os.listdir(folder_path_):
            os.mkdir('{}/{}'.format(folder_path_, folder_))

        if pair not in os.listdir("{}/{}".format(folder_path_, folder_)):
            os.mkdir('{}/{}/{}'.format(folder_path_, folder_, pair))

        aggregation = [x for x in aggregation if x != 'pair']
        folder_path_ = '{}/{}/{}'.format(folder_path_, folder_, pair)
        profile_df_ = filter_by_value(profile_df_, 'pair', pair)
        client_df = aggregate_func_(profile_df_, aggregation)

        k_means_analysis(df=client_df, folder_path=folder_path_, columns=default_columns, index_columns=aggregation, plot_parallel_all=PLOT_ALL)
        hac_analysis(df=client_df, folder_path=folder_path_, columns=default_columns, index_columns=aggregation, plot_parallel_all=PLOT_ALL)
        dbscan_analysis(df=client_df, folder_path=folder_path_, columns=default_columns, index_columns=aggregation, plot_parallel_all=PLOT_ALL)
        mean_shift_analysis(df=client_df, folder_path=folder_path_, columns=default_columns, index_columns=aggregation)

    # ___ 2 - PREPARATION ___
    df = df.drop(columns=['accountId', 'clientId', 'clientName', 'espTrades', 'isBuy', 'isSell', 'on360t', 'onApril', 'onAugust', 'onBloomberg', 'onDecember', 'onFastmatch',
                          'onFebruary', 'onFriday', 'onFxall', 'onJanuary', 'onJuly', 'onJune', 'onMarch', 'onMay', 'onMonday', 'onNovember', 'onOctober', 'onSaturday', 'onSeptember',
                          'onSunday', 'onThursday', 'onTobo', 'onTuesday', 'onUct', 'onUfx', 'onWednesday'], errors='ignore')
    logger.info("Entries after preparation: {}".format(len(df)))

    # ___ 3 - PROFILE ANALYSIS ___
    if FOLDER_NAME not in os.listdir(PLOT_FOLDER):
        os.mkdir('{}/{}'.format(PLOT_FOLDER, FOLDER_NAME))

    for folder in ['M2M_incrementer']:

        folder_path = '{}/{}/{}'.format(PLOT_FOLDER, FOLDER_NAME, folder)
        os.mkdir(folder_path)

        # ___ Profile dataFrame ___
        profile_df = df[df['T0'].notna()][reference_aggregation + ['TI0', 'TI1', 'TI5', 'TI10', 'TI30', 'TI60', 'TI120', 'TI180', 'TI240', 'TI300']]
        aggregate_func = aggregate_by_incrementer

        profile_df = profile_df.replace([np.inf, -np.inf], np.nan).dropna()
        profile_df['isG10'] = profile_df['pair'].isin(g10)

        # ___ NO PAIR ANALYSIS ___
        no_pair_analysis(profile_df, folder_path_=folder_path, aggregate_func_=aggregate_func, aggregation=reference_aggregation)

        # ___ ONLY G10 ANALYSIS ___
        only_g10_analysis(profile_df, folder_path_=folder_path, aggregate_func_=aggregate_func, aggregation=reference_aggregation)

        # ___ ONLY CEEMEA ANALYSIS ___
        only_ceemea_analysis(profile_df, folder_path_=folder_path, aggregate_func_=aggregate_func, aggregation=reference_aggregation)

        pairs = profile_df.groupby('pair').agg('count').sort_values(by='client', ascending=False)
        pairs = list(pairs[pairs['client'] > 100]['client'].index)

        g10_pairs = [x for x in pairs if x in g10]
        ceemea_pairs = [x for x in pairs if x not in g10]

        for pair in g10_pairs:
            pair_analysis(profile_df, pair, folder_path_=folder_path, aggregate_func_=aggregate_func, aggregation=reference_aggregation, default_columns=['T0', 'T1', 'T5', 'T10', 'T30', 'T60'], folder_='04_Pair_G10')

        for pair in ceemea_pairs:
            pair_analysis(profile_df, pair, folder_path_=folder_path, aggregate_func_=aggregate_func, aggregation=reference_aggregation, folder_='05_Pair_CEEMEA')


def profile_analysis_flowdb(df):
    # ___ Aggregation functions ___
    def aggregate_by_incrementer(profile_df_, aggregation):

        aggregated_df = profile_df_.groupby(aggregation).agg(
            M0=('MI0', 'mean'),
            M1=('MI1', 'mean'),
            M5=('MI5', 'mean'),
            M30=('MI30', 'mean'),
            M60=('MI60', 'mean'),
            M120=('MI120', 'mean'),
            M180=('MI180', 'mean'),
            M300=('MI300', 'mean'),
            M600=('MI600', 'mean'),
        ).reset_index()

        return aggregated_df

    # ___ Analysis functions ___
    def no_pair_analysis(profile_df_, aggregate_func_, aggregation, folder_path_, default_columns=None, folder_='01_NoPair'):

        if default_columns is None:
            default_columns = ['M0', 'M1', 'M5', 'M30', 'M60', 'M120', 'M180', 'M300', 'M600']

        if folder not in os.listdir(folder_path_):
            os.mkdir('{}/{}'.format(folder_path_, folder_))

        folder_path_ = '{}/{}'.format(folder_path_, folder_)
        client_df = aggregate_func_(profile_df_, aggregation)

        k_means_analysis(df=client_df, folder_path=folder_path_, columns=default_columns, index_columns=aggregation, plot_parallel_all=PLOT_ALL)
        hac_analysis(df=client_df, folder_path=folder_path_, columns=default_columns, index_columns=aggregation, plot_parallel_all=PLOT_ALL)
        dbscan_analysis(df=client_df, folder_path=folder_path_, columns=default_columns, index_columns=aggregation, plot_parallel_all=PLOT_ALL)
        mean_shift_analysis(df=client_df, folder_path=folder_path_, columns=default_columns, index_columns=aggregation)

    def only_g10_analysis(profile_df_, folder_path_, aggregate_func_, aggregation):
        profile_df_ = profile_df_[profile_df_['isG10']]
        no_pair_analysis(profile_df_, folder_path_=folder_path_, aggregate_func_=aggregate_func_, aggregation=aggregation, default_columns=['M0', 'M1', 'M5', 'M30', 'M60'], folder_='02_onlyG10')

    def only_ceemea_analysis(profile_df_, folder_path_, aggregate_func_, aggregation):
        profile_df_ = profile_df_[~profile_df_['isG10']]
        no_pair_analysis(profile_df_, folder_path_=folder_path_, aggregate_func_=aggregate_func_, aggregation=aggregation, folder_='03_onlyCEEMEA')

    def pair_analysis(profile_df_, pair, aggregate_func_, aggregation, folder_path_, default_columns=None, folder_='04_Pair'):

        if default_columns is None:
            default_columns = ['M0', 'M1', 'M5', 'M30', 'M60', 'M120', 'M180', 'M300', 'M600']

        if folder_ not in os.listdir(folder_path_):
            os.mkdir('{}/{}'.format(folder_path_, folder_))

        if pair not in os.listdir("{}/{}".format(folder_path_, folder_)):
            os.mkdir('{}/{}/{}'.format(folder_path_, folder_, pair))

        aggregation = [x for x in aggregation if x != 'pair']
        folder_path_ = '{}/{}/{}'.format(folder_path_, folder_, pair)
        profile_df_ = filter_by_value(profile_df_, 'pair', pair)
        client_df = aggregate_func_(profile_df_, aggregation)

        k_means_analysis(df=client_df, folder_path=folder_path_, columns=default_columns, index_columns=aggregation, plot_parallel_all=PLOT_ALL)
        hac_analysis(df=client_df, folder_path=folder_path_, columns=default_columns, index_columns=aggregation, plot_parallel_all=PLOT_ALL)
        dbscan_analysis(df=client_df, folder_path=folder_path_, columns=default_columns, index_columns=aggregation, plot_parallel_all=PLOT_ALL)
        mean_shift_analysis(df=client_df, folder_path=folder_path_, columns=default_columns, index_columns=aggregation)

    # ___ 1 - G10 EXTRACTION ___
    g10_ccys = ['USD', 'EUR', 'GBP', 'JPY', 'AUD', 'NZD', 'CAD', 'CHF', 'NOK', 'SEK', 'DKK']
    g10 = [pair for pair in df['pair'].unique() if pair[:3].upper() in g10_ccys and pair[3:].upper() in g10_ccys]
    # df['isG10'] = df['pair'].apply(lambda x: x.upper() in g10)

    # ___ 2 - PREPARATION ___
    df = df.drop(columns=['accountId', 'clientId', 'clientName', 'espTrades', 'isBuy', 'isSell', 'on360t', 'onApril', 'onAugust', 'onBloomberg', 'onDecember', 'onFastmatch',
                          'onFebruary', 'onFriday', 'onFxall', 'onJanuary', 'onJuly', 'onJune', 'onMarch', 'onMay', 'onMonday', 'onNovember', 'onOctober', 'onSaturday', 'onSeptember',
                          'onSunday', 'onThursday', 'onTobo', 'onTuesday', 'onUct', 'onUfx', 'onWednesday'], errors='ignore')
    logger.info("Entries after preparation: {}".format(len(df)))

    # ___ 3 - PROFILE ANALYSIS ___
    if FOLDER_NAME not in os.listdir(PLOT_FOLDER):
        os.mkdir('{}/{}'.format(PLOT_FOLDER, FOLDER_NAME))

    for folder in ['Markout_incrementer']:

        folder_path = '{}/{}/{}'.format(PLOT_FOLDER, FOLDER_NAME, folder)
        os.mkdir(folder_path)

        # ___ Profile dataFrame ___
        profile_df = df[df['M0'].notna()][reference_aggregation + ['MI0', 'MI1', 'MI5', 'MI30', 'MI60', 'MI120', 'MI180', 'MI300', 'MI600']]
        aggregate_func = aggregate_by_incrementer

        profile_df = profile_df.replace([np.inf, -np.inf], np.nan).dropna()
        profile_df['isG10'] = profile_df['pair'].isin(g10)

        # ___ NO PAIR ANALYSIS ___
        no_pair_analysis(profile_df, folder_path_=folder_path, aggregate_func_=aggregate_func, aggregation=reference_aggregation)

        # ___ ONLY G10 ANALYSIS ___
        only_g10_analysis(profile_df, folder_path_=folder_path, aggregate_func_=aggregate_func, aggregation=reference_aggregation)

        # ___ ONLY CEEMEA ANALYSIS ___
        only_ceemea_analysis(profile_df, folder_path_=folder_path, aggregate_func_=aggregate_func, aggregation=reference_aggregation)

        pairs = profile_df.groupby('pair').agg('count').sort_values(by='client', ascending=False)
        pairs = list(pairs[pairs['client'] > 100]['client'].index)

        g10_pairs = [x for x in pairs if x in g10]
        ceemea_pairs = [x for x in pairs if x not in g10]

        for pair in g10_pairs:
            pair_analysis(profile_df, pair, folder_path_=folder_path, aggregate_func_=aggregate_func, aggregation=reference_aggregation, default_columns=['M0', 'M1', 'M5', 'M30', 'M60'], folder_='04_Pair_G10')

        for pair in ceemea_pairs:
            pair_analysis(profile_df, pair, folder_path_=folder_path, aggregate_func_=aggregate_func, aggregation=reference_aggregation, folder_='05_Pair_CEEMEA')


def add_360t_client_profile_to_df(df_):
    def aggregate_by_incrementer_360t(profile_df_, aggregation):
        aggregated_df = profile_df_.groupby(aggregation).agg(
            T1=('TI1', 'mean'),
            T5=('TI5', 'mean'),
            T10=('TI10', 'mean'),
            T30=('TI30', 'mean'),
            T60=('TI60', 'mean'),
            T120=('TI120', 'mean'),
            T180=('TI180', 'mean'),
            T240=('TI240', 'mean'),
            T300=('TI300', 'mean')
        ).reset_index()

        return aggregated_df

    profile_df = df_[df_['T1%'].notna()][reference_aggregation + ['TI1', 'TI5', 'TI10', 'TI30', 'TI60', 'TI120', 'TI180', 'TI240', 'TI300']]
    profile_df = profile_df.replace([np.inf, -np.inf], np.nan).dropna()

    default_columns = ['T1', 'T5', 'T10', 'T30', 'T60', 'T120', 'T180', 'T240', 'T300']
    client_df = aggregate_by_incrementer_360t(profile_df, reference_aggregation)
    k_means_df, clusters_df = k_means(client_df, n_clusters=3, columns=default_columns, index_columns=reference_aggregation)
    k_means_df = move_columns_to_first_position(k_means_df, reference_aggregation + default_columns)

    clusters_df['mean'] = clusters_df[[x for x in clusters_df.columns if x != 'label']].mean(axis=1)

    valid_labels = sorted(list(clusters_df[clusters_df['mean'] < 0]['label'].values))

    # very_sharp = str(clusters_df.sort_values(by='mean')['label'].iloc[0])
    sharp = str(clusters_df.sort_values(by='mean')['label'].iloc[0])
    flat = str(clusters_df.sort_values(by='mean')['label'].iloc[1])
    unwise = str(clusters_df.sort_values(by='mean')['label'].iloc[2])
    # very_unwise = str(clusters_df.sort_values(by='mean')['label'].iloc[4])

    k_means_df['label'] = k_means_df['label'].astype(str)
    # k_means_df['label'] = k_means_df['label'].str.replace(very_unwise, 'VERY_UNWISE')
    k_means_df['label'] = k_means_df['label'].str.replace(unwise, 'T_Unwise')
    k_means_df['label'] = k_means_df['label'].str.replace(flat, 'T_Flat')
    k_means_df['label'] = k_means_df['label'].str.replace(sharp, 'T_Sharp')
    # k_means_df['label'] = k_means_df['label'].str.replace(very_sharp, 'VERY_SHARP')

    k_means_df = one_hot_encode(k_means_df, ['label'])

    # logger.info('Total VERY UNWISE clients: {}'.format(len(filter_by_value(k_means_df, 'isVeryUnwise', 1))))
    logger.info('Total UNWISE clients: {}'.format(len(filter_by_value(k_means_df, 'isUnwise', 1))))
    logger.info('Total RANDOM clients: {}'.format(len(filter_by_value(k_means_df, 'isFlat', 1))))
    logger.info('Total SHARP clients: {}'.format(len(filter_by_value(k_means_df, 'isSharp', 1))))
    # logger.info('Total VERY SHARP clients: {}'.format(len(filter_by_value(k_means_df, 'isVerySharp', 1))))

    return k_means_df


def add_flowdb_client_profile_to_df(df_):
    def aggregate_by_incrementer_flowdb(profile_df_, aggregation):
        aggregated_df = profile_df_.groupby(aggregation).agg(
            M1=('MI1', 'mean'),
            M5=('MI5', 'mean'),
            M30=('MI30', 'mean'),
            M60=('MI60', 'mean'),
            M120=('MI120', 'mean'),
            M180=('MI180', 'mean'),
            M300=('MI300', 'mean'),
            M600=('MI600', 'mean'),
        ).reset_index()

        return aggregated_df

    profile_df = df_[df_['M1%'].notna()][reference_aggregation + ['MI1', 'MI5', 'MI30', 'MI60', 'MI120', 'MI180', 'MI300', 'MI600']]
    profile_df = profile_df.replace([np.inf, -np.inf], np.nan).dropna()
    default_columns = ['M1', 'M5', 'M30', 'M60', 'M120', 'M180', 'M300', 'M600']

    client_df = aggregate_by_incrementer_flowdb(profile_df, reference_aggregation)
    k_means_df, clusters_df = k_means(client_df, n_clusters=3, columns=default_columns, index_columns=reference_aggregation)
    k_means_df = move_columns_to_first_position(k_means_df, reference_aggregation + default_columns)

    clusters_df['mean'] = clusters_df[[x for x in clusters_df.columns if x != 'label']].mean(axis=1)

    valid_labels = sorted(list(clusters_df[clusters_df['mean'] < 0]['label'].values))

    # very_sharp = str(clusters_df.sort_values(by='mean')['label'].iloc[0])
    sharp = str(clusters_df.sort_values(by='mean')['label'].iloc[0])
    flat = str(clusters_df.sort_values(by='mean')['label'].iloc[1])
    unwise = str(clusters_df.sort_values(by='mean')['label'].iloc[2])
    # very_unwise = str(clusters_df.sort_values(by='mean')['label'].iloc[4])

    k_means_df['label'] = k_means_df['label'].astype(str)
    # k_means_df['label'] = k_means_df['label'].str.replace(very_unwise, 'VERY_UNWISE')
    k_means_df['label'] = k_means_df['label'].str.replace(unwise, 'M_Unwise')
    k_means_df['label'] = k_means_df['label'].str.replace(flat, 'M_Flat')
    k_means_df['label'] = k_means_df['label'].str.replace(sharp, 'M_Sharp')
    # k_means_df['label'] = k_means_df['label'].str.replace(very_sharp, 'VERY_SHARP')

    k_means_df = one_hot_encode(k_means_df, ['label'])

    # logger.info('Total VERY UNWISE clients: {}'.format(len(filter_by_value(k_means_df, 'isVeryUnwise', 1))))
    logger.info('Total UNWISE clients: {}'.format(len(filter_by_value(k_means_df, 'isUnwise', 1))))
    logger.info('Total RANDOM clients: {}'.format(len(filter_by_value(k_means_df, 'isFlat', 1))))
    logger.info('Total SHARP clients: {}'.format(len(filter_by_value(k_means_df, 'isSharp', 1))))
    # logger.info('Total VERY SHARP clients: {}'.format(len(filter_by_value(k_means_df, 'isVerySharp', 1))))

    return k_means_df


# _____ Profile Analysis per Pair _____
def add_360t_pair_client_profile_to_df(df_):
    def aggregate_by_incrementer_360t(profile_df_, aggregation):

        aggregated_df = profile_df_.groupby(aggregation).agg(
            T1=('TI1', 'mean'),
            T5=('TI5', 'mean'),
            T10=('TI10', 'mean'),
            T30=('TI30', 'mean'),
            T60=('TI60', 'mean'),
            T120=('TI120', 'mean'),
            T180=('TI180', 'mean'),
            T240=('TI240', 'mean'),
            T300=('TI300', 'mean')
        ).reset_index()

        return aggregated_df

    def pair_profiling(profile_df_, default_columns=None):

        if default_columns is None:
            default_columns = ['T1', 'T5', 'T10', 'T30', 'T60', 'T120', 'T180', 'T240', 'T300']

        aggregation = [x for x in reference_aggregation if x != 'pair']
        client_df = aggregate_by_incrementer_360t(profile_df_, aggregation)
        k_means_df, clusters_df = k_means(client_df, n_clusters=3, columns=default_columns, index_columns=aggregation)
        k_means_df = move_columns_to_first_position(k_means_df, aggregation + default_columns)

        clusters_df['mean'] = clusters_df[[x for x in clusters_df.columns if x != 'label']].mean(axis=1)

        valid_labels = sorted(list(clusters_df[clusters_df['mean'] < 0]['label'].values))

        # very_sharp = str(clusters_df.sort_values(by='mean')['label'].iloc[0])
        sharp = str(clusters_df.sort_values(by='mean')['label'].iloc[0])
        flat = str(clusters_df.sort_values(by='mean')['label'].iloc[1])
        unwise = str(clusters_df.sort_values(by='mean')['label'].iloc[2])
        # very_unwise = str(clusters_df.sort_values(by='mean')['label'].iloc[4])

        k_means_df['label'] = k_means_df['label'].astype(str)
        # k_means_df['label'] = k_means_df['label'].str.replace(very_unwise, 'VERY_UNWISE')
        k_means_df['label'] = k_means_df['label'].str.replace(unwise, 'T_Unwise')
        k_means_df['label'] = k_means_df['label'].str.replace(flat, 'T_Flat')
        k_means_df['label'] = k_means_df['label'].str.replace(sharp, 'T_Sharp')
        # k_means_df['label'] = k_means_df['label'].str.replace(very_sharp, 'VERY_SHARP')

        k_means_df = one_hot_encode(k_means_df, ['label'])

        # logger.info('Total VERY UNWISE clients: {}'.format(len(filter_by_value(k_means_df, 'isVeryUnwise', 1))))
        logger.info('Total UNWISE clients: {}'.format(len(filter_by_value(k_means_df, 'isUnwise', 1))))
        logger.info('Total RANDOM clients: {}'.format(len(filter_by_value(k_means_df, 'isFlat', 1))))
        logger.info('Total SHARP clients: {}'.format(len(filter_by_value(k_means_df, 'isSharp', 1))))
        # logger.info('Total VERY SHARP clients: {}'.format(len(filter_by_value(k_means_df, 'isVerySharp', 1))))

        return k_means_df

    profile_df = df_[df_['T1%'].notna()][reference_aggregation + ['TI1', 'TI5', 'TI10', 'TI30', 'TI60', 'TI120', 'TI180', 'TI240', 'TI300']]
    profile_df = profile_df.replace([np.inf, -np.inf], np.nan).dropna()

    pairs = profile_df.groupby('pair').agg('count').sort_values(by='client', ascending=False)
    pairs = list(pairs[pairs['client'] > 100]['client'].index)

    g10_pairs = [x for x in pairs if x in g10]
    ceemea_pairs = [x for x in pairs if x not in g10]

    concat_df = None
    for pair in g10_pairs:
        pair_df = pair_profiling(filter_by_value(profile_df, 'pair', pair), default_columns=['T1', 'T5', 'T10', 'T30', 'T60'])
        pair_df['pair'] = pair
        concat_df = pair_df if concat_df is None else pd.concat([concat_df, pair_df], ignore_index=True)  # concat dataFrame

    for pair in ceemea_pairs:
        pair_df = pair_profiling(filter_by_value(profile_df, 'pair', pair))
        pair_df['pair'] = pair
        concat_df = pair_df if concat_df is None else pd.concat([concat_df, pair_df], ignore_index=True)  # concat dataFrame

    return concat_df


def add_flowdb_pair_client_profile_to_df(df_):
    def aggregate_by_incrementer_flowdb(profile_df_, aggregation):

        aggregated_df = profile_df_.groupby(aggregation).agg(
            M1=('MI1', 'mean'),
            M5=('MI5', 'mean'),
            M30=('MI30', 'mean'),
            M60=('MI60', 'mean'),
            M120=('MI120', 'mean'),
            M180=('MI180', 'mean'),
            M300=('MI300', 'mean'),
            M600=('MI600', 'mean'),
        ).reset_index()

        return aggregated_df

    def pair_profiling(profile_df_, default_columns=None):

        if default_columns is None:
            default_columns = ['M1', 'M5', 'M30', 'M60', 'M120', 'M180', 'M300', 'M600']

        aggregation = [x for x in reference_aggregation if x != 'pair']
        client_df = aggregate_by_incrementer_flowdb(profile_df_, aggregation)
        k_means_df, clusters_df = k_means(client_df, n_clusters=3, columns=default_columns, index_columns=aggregation)
        k_means_df = move_columns_to_first_position(k_means_df, aggregation + default_columns)

        clusters_df['mean'] = clusters_df[[x for x in clusters_df.columns if x != 'label']].mean(axis=1)

        valid_labels = sorted(list(clusters_df[clusters_df['mean'] < 0]['label'].values))

        # very_sharp = str(clusters_df.sort_values(by='mean')['label'].iloc[0])
        sharp = str(clusters_df.sort_values(by='mean')['label'].iloc[0])
        flat = str(clusters_df.sort_values(by='mean')['label'].iloc[1])
        unwise = str(clusters_df.sort_values(by='mean')['label'].iloc[2])
        # very_unwise = str(clusters_df.sort_values(by='mean')['label'].iloc[4])

        k_means_df['label'] = k_means_df['label'].astype(str)
        # k_means_df['label'] = k_means_df['label'].str.replace(very_unwise, 'VERY_UNWISE')
        k_means_df['label'] = k_means_df['label'].str.replace(unwise, 'M_Unwise')
        k_means_df['label'] = k_means_df['label'].str.replace(flat, 'M_Flat')
        k_means_df['label'] = k_means_df['label'].str.replace(sharp, 'M_Sharp')
        # k_means_df['label'] = k_means_df['label'].str.replace(very_sharp, 'VERY_SHARP')

        k_means_df = one_hot_encode(k_means_df, ['label'])

        # logger.info('Total VERY UNWISE clients: {}'.format(len(filter_by_value(k_means_df, 'isVeryUnwise', 1))))
        logger.info('Total UNWISE clients: {}'.format(len(filter_by_value(k_means_df, 'isMUnwise', 1))))
        logger.info('Total RANDOM clients: {}'.format(len(filter_by_value(k_means_df, 'isMFlat', 1))))
        logger.info('Total SHARP clients: {}'.format(len(filter_by_value(k_means_df, 'isMSharp', 1))))
        # logger.info('Total VERY SHARP clients: {}'.format(len(filter_by_value(k_means_df, 'isVerySharp', 1))))

        return k_means_df

    profile_df = df_[df_['M1%'].notna()][reference_aggregation + ['MI1', 'MI5', 'MI30', 'MI60', 'MI120', 'MI180', 'MI300', 'MI600']]
    profile_df = profile_df.replace([np.inf, -np.inf], np.nan).dropna()

    pairs = profile_df.groupby('pair').agg('count').sort_values(by='client', ascending=False)
    pairs = list(pairs[pairs['client'] > 100]['client'].index)

    g10_pairs = [x for x in pairs if x in g10]
    ceemea_pairs = [x for x in pairs if x not in g10]

    concat_df = None
    for pair in g10_pairs:
        pair_df = pair_profiling(filter_by_value(profile_df, 'pair', pair), default_columns=['M1', 'M5', 'M30', 'M60'])
        pair_df['pair'] = pair
        concat_df = pair_df if concat_df is None else pd.concat([concat_df, pair_df], ignore_index=True)  # concat dataFrame

    for pair in ceemea_pairs:
        pair_df = pair_profiling(filter_by_value(profile_df, 'pair', pair))
        pair_df['pair'] = pair
        concat_df = pair_df if concat_df is None else pd.concat([concat_df, pair_df], ignore_index=True)  # concat dataFrame

    return concat_df


if __name__ == '__main__':

    analysis_folder = '05_profile_analysis'
    table_name = 'final_df_per_week' if not PER_QUARTER else 'final_df_per_quarter'
    reference_aggregation = ['client', 'pair', 'productType', 'year', 'quarter'] if PER_QUARTER else ['client', 'pair', 'productType', 'year', 'weekOfYear']

    if analysis_folder not in os.listdir('data/'):
        os.mkdir('data/{}'.format(analysis_folder))

    analysis_folder = 'data/{}'.format(analysis_folder)

    # _____ 0 - OVERRIDE _____
    if OVERRIDE_PREPARATION or f"{table_name}.csv" not in os.listdir(analysis_folder):

        # ___ Load initial dataFrame ___
        df = load(table=table_name, folder='data')
        logger.info("Initial entries: {}".format(len(df)))

        # ___ Add metrics to dataFrame ___
        df = prepare(df)
        logger.debug('Total entries after preparation: {}'.format(len(df)))

        # ___ Save prepared dataFrame ___
        df.to_csv(f'{analysis_folder}/{table_name}.csv', index=False)

    else:
        df = load(table=table_name, folder=analysis_folder)
        logger.info("Initial entries: {}".format(len(df)))

    # ___ 1 - G10 EXTRACTION ___
    g10_ccys = ['USD', 'EUR', 'GBP', 'JPY', 'AUD', 'NZD', 'CAD', 'CHF', 'NOK', 'SEK', 'DKK']
    g10 = [pair for pair in df['pair'].unique() if pair[:3].upper() in g10_ccys and pair[3:].upper() in g10_ccys]
    # df['isG10'] = df['pair'].apply(lambda x: x.upper() in g10)

    # ___ Profile Analysis Plot ___
    profile_analysis_360t(df)
    profile_analysis_flowdb(df)

    # ___ Client Profiling ___
    # profile_360t_df = add_360t_pair_client_profile_to_df(df)
    # profile_flowdb_df = add_flowdb_pair_client_profile_to_df(df)

    # final_df = pd.merge(df, profile_360t_df.drop(columns=['T1', 'T5', 'T10', 'T30', 'T60', 'T120', 'T180', 'T240', 'T300'], errors='ignore'), on=reference_aggregation, how='left')  # merge initial dataFrame and 360T profile dataFrame
    # final_df = pd.merge(final_df, profile_flowdb_df.drop(columns=['M1', 'M5', 'M30', 'M60', 'M120', 'M180', 'M300', 'M600'], errors='ignore'), on=reference_aggregation, how='left')  # merge initial dataFrame and profile dataFrame

    # final_df = final_df.drop(columns=['T1%', 'T5%', 'T10%', 'T30%', 'T60%', 'T120%', 'T180%', 'T240%', 'T300%', 'T_1', 'T_2', 'T_5'], errors='ignore')

    # final_df = final_df.drop(columns=['M1%', 'M120%', 'M180%', 'M30%', 'M300%', 'M5%', 'M60%', 'M600%', 'MI1', 'MI120', 'MI180', 'MI30', 'MI300', 'MI5', 'MI60', 'MI600', 'MIF1', 'MIF120', 'MIF180', 'MIF30', 'MIF300', 'MIF5', 'MIF60', 'MIF600',
    #                                   'Max', 'Min', 'T1%', 'T10%', 'T120%', 'T180%', 'T240%', 'T30%', 'T300%', 'T5%', 'T60%', 'TI1', 'TI10', 'TI120', 'TI180', 'TI240', 'TI30', 'TI300', 'TI5', 'TI60', 'TIF1', 'TIF10', 'TIF120', 'TIF180', 'TIF240',
    #                                   'TIF30', 'TIF300', 'TIF5', 'TIF60', 'T_1', 'T_2', 'T_5', 'label_x', 'label_y'], errors='ignore')

    # final_df = move_columns_to_first_position(final_df, ['client', 'pair', 'productType', 'year', 'quarter', 'BestPriceMean', 'Top3PriceMean', 'DistanceToTOB',
    #                                                      'clientRelevance', 'isTSharp', 'isTFlat', 'isTUnwise', 'isMSharp', 'isMFlat', 'isMUnwise',
    #                                                      'CoEURAmount', 'FDEURAmount', 'CoConfirmedTrades', 'CoTradesCount', 'hitRatio', 'FDtradesCount'])

    # ___ Save dataFrame with profile info added ___
    # final_df.to_csv(f'{analysis_folder}/{table_name}_profiled.csv', index=False)

    # filtered_df = final_df[(final_df['isTSharp'].notna()) & (final_df['isMSharp'].notna())]
    # correct_df = filtered_df[(filtered_df['isTSharp'] == filtered_df['isMSharp']) & (filtered_df['isTFlat'] == filtered_df['isMFlat']) & (filtered_df['isTUnwise'] == filtered_df['isMUnwise'])]
    # not_correct_df = filtered_df[~((filtered_df['isTSharp'] == filtered_df['isMSharp']) & (filtered_df['isTFlat'] == filtered_df['isMFlat']) & (filtered_df['isTUnwise'] == filtered_df['isMUnwise']))]
    # not_correct_df = move_columns_to_first_position(not_correct_df, ['isTSharp', 'isTFlat', 'isTUnwise', 'isMSharp', 'isMFlat', 'isMUnwise', 'T0', 'T1', 'T5', 'T10', 'T30', 'T60', 'M0', 'M1', 'M5', 'M30', 'M60'])

    # logger.info('end')

    # TODO: add other clustering methods to plot (hac, dbscan)
    # TODO: Client profiling - adjust code in this new python file
    # TODO: apply standard scaler before k-means (incremental) but keep original columns to calculate centroids
    # TODO: understand if to cluster per pair or not. In case we want to cluster per pair we must create a look-up table that associates each pair to the number of clusters to use
    # TODO: use a look-up table to associate each currency to the seconds needed to hedge