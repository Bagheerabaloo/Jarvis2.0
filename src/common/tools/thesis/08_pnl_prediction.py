import pandas as pd
import matplotlib.pyplot as plt
from time import time
import os

from src.utils import init_logger, timestamp2date, create_folder
from src.utils_dataframe import load, is_null, filter_by_value, one_hot_encode, drop_rows_with_nan_mandatory_cols, null_counting
from src.ml_classification import correlation_matrix, logistic_regression_analysis, random_forest_analysis
from src.ml_clustering import standard_scaler

# initial setup
pd.options.display.max_columns = None

# create logger
logger = init_logger(name='Preparation')

CURRENT_TIMESTAMP = timestamp2date(time(), frmt='%m_%d_%H%M%S')

PLOT_FOLDER = f'plots/{CURRENT_TIMESTAMP}_PNL_Analysis'

OVERRIDE_PREPARATION = False


# ___ Histogram ___
def hist(df_, name, title):
    fig = plt.figure(figsize=(8, 5))
    ax = fig.gca()
    ax.title.set_text(f'{title}')
    df_['target'].value_counts().plot(ax=ax, kind='bar')
    fig.savefig(f'{PLOT_FOLDER}/{name}.png')


# ___ Analysis ___
def predict(override=True):
    """ Perform PnL prediction """

    create_folder(PLOT_FOLDER)

    analysis_folder = '08_pnl_prediction'
    reference_aggregation = ['client', 'pair', 'productType', 'platform', 'pnlTotalPerM', 'pnlMatchUSD', 'M0']

    if analysis_folder not in os.listdir('data/'):
        os.mkdir('data/{}'.format(analysis_folder))

    analysis_folder = 'data/{}'.format(analysis_folder)

    # ___ 0 - LOAD dfFrame ___
    df = load(folder='data/04_prepared', table='exceed')

    # --- FLOW DB ---
    df = filter_by_value(df, column='passfail', value='PASS')

    # keep records having totalPnlPerM and markoutUSD_0 not null
    df = drop_rows_with_nan_mandatory_cols(df, mandatory_cols=['pnlTotalPerM', 'M0'])

    # extract df additional features
    # df['weekOfYear'] = df['date'].dt.isocalendar().week
    # df['dayOfWeek'] = df['date'].dt.day_name()
    # df['year'] = df['date'].dt.isocalendar().year

    df['target'] = df['pnlTotalPerM'].apply(lambda pnl: 1 if pnl < 0 else 0)

    # ___ 3 - calculate FlowDB binary markout ___
    df['MB0'] = df['M0'].apply(lambda x: 1 if x >= 0 else -1)

    # rename columns
    # df = df.rename(columns={"externalClientId": "client", "sym": "pair", "dealType": "productType", 'markoutUSD_0': 'markout'})
    df = df.drop(columns={'M0', 'M1', 'M120', 'M180', 'M30', 'M300', 'M5', 'M60', 'M600', 'accountId', 'client',
                          'clientId', 'clientName', 'dealRate', 'pnlAHUSD', 'pnlAdjUSD',
                          'pnlInceptionUSD', 'pnlMatchUSD', 'pnlTotalPerM', 'pnlTotalUSD', 'weekOfYear', 'year',
                          'FDespTrades', 'FDrfsTrades', 'FDtradesCount', 'CoRfsTrades'}
                 , errors='ignore')

    # keep only independent features
    correlation_matrix(df, folder_path=PLOT_FOLDER, suffix='flowdb_before')

    # drop pnlTotalPerM, keep only 'target' one
    # df = df.drop(columns=['pnlTotalPerM'], errors='ignore')

    # in according to the correlation matrix we kept the following independent columns
    categoricals = ['clientType', 'pair', 'platform', 'productType']
    numericals = ['CoConfirmedTrades', 'CoEURAmount', 'CoTradesCount', 'FDEURAmount', 'target']

    # df = df[[*numericals, *categoricals]]

    correlation_matrix(df[numericals], folder_path=PLOT_FOLDER, suffix='flowdb_after')

    df = one_hot_encode(df, categoricals, drop_columns=True, preserve_col_name=True)

    to_ignore = ['date', 'platform', 'client', 'pair', 'productType', 'clientId', 'markout', 'target']
    features = [x for x in df.columns.tolist() if x != 'target']
    # for col in to_ignore:
    #     features.remove(col)

    logger.debug(f'Features: {features}')

    # re-balance dfset
    hist(df, name='TargetHistBefore', title='Target Distribution Before Rebalancing')

    true_df = df[df['target'] == 1]
    false_df = df[df['target'] == 0].sample(len(true_df), random_state=23)
    df = pd.concat([true_df, false_df])

    hist(df, name='TargetHistAfter', title='Target Distribution After Rebalancing')

    # -- NON-SCALED DATASET WITH ALL FEATURES ---
    non_scaled_path = f'{PLOT_FOLDER}/NonScaledAll'
    create_folder(non_scaled_path)

    df = drop_rows_with_nan_mandatory_cols(df, ['CoConfirmedTrades'])
    assert not is_null(df)

    logistic_regression_analysis(df, target='target', columns=features, folder_path=non_scaled_path, main_features=False)
    random_forest_analysis(df, target='target', columns=features, folder_path=non_scaled_path, main_features=False)

    # -- NON-SCALED DATASET WITH ALL FEATURES ---
    non_scaled_top_path = f'{PLOT_FOLDER}/NonScaledTop10'
    create_folder(non_scaled_top_path)

    top_features = ['amount', 'dealAmount', 'weekOfYear', 'year']

    logistic_regression_analysis(df, target='target', columns=top_features, folder_path=non_scaled_top_path, main_features=False)
    random_forest_analysis(df, target='target', columns=top_features, folder_path=non_scaled_top_path, main_features=False)

    # -- SCALED DATASET ---
    scaled_path = f'{PLOT_FOLDER}/ScaledTop'
    create_folder(scaled_path)
    scaled_df = standard_scaler(df, columns=top_features)

    logistic_regression_analysis(scaled_df, target='target', columns=top_features, folder_path=scaled_path)


if __name__ == '__main__':
    predict()