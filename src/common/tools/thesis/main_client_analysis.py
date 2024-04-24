import matplotlib.pyplot as plt

from time import time
from typing import List
from abc import ABC, abstractmethod

from matplotlib import cm
from sklearn.cluster import KMeans, MeanShift, AffinityPropagation
from sklearn.metrics import silhouette_score, silhouette_samples
from sklearn.preprocessing import StandardScaler

from src.ml_clustering import k_means_analysis, standard_scaler
from src.utils import *
from src.utils_dataframe import *

logger = init_logger('analysis')

CURRENT_TIMESTAMP = timestamp2date(time(), frmt='%m_%d_%H_%M_%S')

PLOT_FOLDER = f"plots/{CURRENT_TIMESTAMP}/WEEK_ClientAnalysis"
AGGREGATED_PLOT_FOLDER = f"plots/{CURRENT_TIMESTAMP}/QUARTER_ClientAnalysis"

OVERRIDE = False


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
    df_['clientRelevance'] = df_['clientRelevance'] - (
            df_['LPRIsA'] * 4 + df_['LPRIsB'] * 3 + df_['LPRIsC'] * 2 + df_['LPRIsD'])
    df_['clientRelevance'] = - df_['clientRelevance'] / 4

    return df_


def add_mark_to_market_metrics(df_):
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

    df_['TI0'] = df_.apply(lambda x: 1 if x['T0'] >= 0 else -1, axis=1)
    df_[['TI1', 'Max', 'Min']] = df_.apply(lambda x: extract_incrementer(x, 'T0', 'T1', 'TI0', x['T0'], x['T0']),
                                           axis=1, result_type='expand')
    df_[['TI5', 'Max', 'Min']] = df_.apply(lambda x: extract_incrementer(x, 'T1', 'T5', 'TI1', x['Max'], x['Min']),
                                           axis=1, result_type='expand')
    df_[['TI10', 'Max', 'Min']] = df_.apply(lambda x: extract_incrementer(x, 'T5', 'T10', 'TI5', x['Max'], x['Min']),
                                            axis=1, result_type='expand')
    df_[['TI30', 'Max', 'Min']] = df_.apply(lambda x: extract_incrementer(x, 'T10', 'T30', 'TI10', x['Max'], x['Min']),
                                            axis=1, result_type='expand')
    df_[['TI60', 'Max', 'Min']] = df_.apply(lambda x: extract_incrementer(x, 'T30', 'T60', 'TI30', x['Max'], x['Min']),
                                            axis=1, result_type='expand')
    df_[['TI120', 'Max', 'Min']] = df_.apply(
        lambda x: extract_incrementer(x, 'T60', 'T120', 'TI60', x['Max'], x['Min']), axis=1, result_type='expand')
    df_[['TI180', 'Max', 'Min']] = df_.apply(
        lambda x: extract_incrementer(x, 'T120', 'T180', 'TI120', x['Max'], x['Min']), axis=1, result_type='expand')
    df_[['TI240', 'Max', 'Min']] = df_.apply(
        lambda x: extract_incrementer(x, 'T180', 'T240', 'TI180', x['Max'], x['Min']), axis=1, result_type='expand')
    df_[['TI300', 'Max', 'Min']] = df_.apply(
        lambda x: extract_incrementer(x, 'T240', 'T300', 'TI240', x['Max'], x['Min']), axis=1, result_type='expand')

    return df_


def prepare(df_):
    df_ = add_client_relevance_metrics(df_)
    # df_ = add_mark_to_market_metrics(df_)

    to_fill_with_0 = ['BestPriceMean', 'Top3PriceMean', 'CoConfirmedTrades', 'CoEURAmount', 'CoTradesCount', 'hitRatio']
    for col in to_fill_with_0:
        df_[col] = df_[col].fillna(0)

    to_fill_with_negative = ['clientRelevance']
    for col in to_fill_with_negative:
        df_[col] = df_[col].fillna(-1)

    to_fill_with_empty = ['clientType']
    for col in to_fill_with_empty:
        df_[col] = df_[col].fillna('EMPTY')

    df_ = df_[df_['DistanceToTOB'].notna()]

    interesting_columns = ['BestPriceMean', 'CoConfirmedTrades', 'CoEURAmount', 'CoTradesCount', 'DistanceToTOB',
                           'Top3PriceMean', 'client', 'clientRelevance', 'clientType', 'hitRatio', 'pair',
                           'productType', 'quarter', 'year', 'weekOfYear']

    df_ = df_[interesting_columns]

    assert is_null(df_) is False

    df_.to_csv('data/client_data_prepared.csv')

    return df_


def aggregate(df_, aggregation):
    return df_.groupby(aggregation).agg(
        'mean'
    ).reset_index()


def client_analysis():
    output_path = 'data'

    if 'client_data_prepared.csv' in os.listdir('data/') or OVERRIDE:
        table_name = 'client_data_prepared'
    else:
        table_name = 'final_df_per_week'

    df = load(table=table_name, folder=output_path)
    logger.info("Initial entries: {}".format(len(df)))

    if table_name == 'final_df_per_week':
        df = prepare(df)

    # define
    default_columns = ['BestPriceMean', 'CoConfirmedTrades', 'CoEURAmount', 'CoTradesCount', 'DistanceToTOB',
                       'Top3PriceMean', 'clientRelevance', 'hitRatio']

    week_aggregation = ['client', 'pair', 'productType', 'weekOfYear', 'quarter', 'year']
    quarter_aggregation = ['client', 'pair', 'productType', 'quarter', 'year']

    df = df[[*default_columns, *week_aggregation]]

    aggregated_df = aggregate(df, quarter_aggregation)

    df = standard_scaler(df, columns=default_columns)

    create_folder(PLOT_FOLDER)
    logger.info("Total entries before by week clustering: {}".format(len(df)))
    k_means_analysis(df=df, folder_path=PLOT_FOLDER, columns=default_columns, index_columns=week_aggregation,
                     plot_parallel_all=False, min_k=2, max_k=10)

    aggregated_df = standard_scaler(aggregated_df, columns=default_columns)

    create_folder(AGGREGATED_PLOT_FOLDER)
    logger.info("Total entries before by quarter clustering: {}".format(len(aggregated_df)))
    k_means_analysis(df=aggregated_df, folder_path=AGGREGATED_PLOT_FOLDER, columns=default_columns,
                     index_columns=quarter_aggregation, plot_parallel_all=False, min_k=2, max_k=10)


if __name__ == '__main__':
    client_analysis()