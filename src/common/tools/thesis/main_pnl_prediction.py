import os
from random import randint
from time import time

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn import svm
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
# to keep for HalvingRandomSearchCV
from sklearn.experimental import enable_halving_search_cv
from sklearn.model_selection import GridSearchCV, HalvingRandomSearchCV
from sklearn.utils.fixes import loguniform

from src.ml_classification import correlation_matrix, classification_analysis, baseline_analysis
from src.utils_dataframe import load, is_null, filter_by_value, drop_rows_with_nan_mandatory_cols
from src.utils import init_logger, timestamp2date, create_folder

# initial setup
pd.options.display.max_columns = None

# create logger
logger = init_logger(name='Preparation')

CURRENT_TIMESTAMP = timestamp2date(time(), frmt='%m_%d_%H_%M_%S')

INPUT_FOLDER = 'data/02_cleaned'
OUTPUT_PREPARATION_FOLDER = 'data/07_classification'
PLOT_FOLDER = f'plots/{CURRENT_TIMESTAMP}_PNL_Analysis'

START_BASELINE = False
START_GRID_SEARCH_DOWNSAMPLE = True
START_GRID_SEARCH_OVERSAMPLE = False
START_HALVING_SEARCH_DOWNSAMPLE = False
START_HALVING_SEARCH_OVERSAMPLE = False
START_LOG_REGRESSION = False
START_RAND_FOREST = True
START_SVC = False


def enhance_client_order_information(orders: pd.DataFrame) -> pd.DataFrame:
    def is_confirmed(order_state: str) -> int:
        """ return 1 if order_state is CONFIRMED, otherwise 0 """
        return 1 if order_state == 'CONFIRMED' else 0

    def is_rfs(stream_type: str) -> int:
        """ return 1 if stream_type is RFS, otherwise 0 (i.e., ESP) """
        return 1 if stream_type == 'RFS' else 0

    # extract if a trade is CONFIRMED or not
    orders['isConfirmed'] = orders['orderState'].apply(lambda val: is_confirmed(val))

    # extract if a trade is an RFS or not
    orders['isRfs'] = orders['streamType'].apply(lambda val: is_rfs(val))

    return orders.reset_index(drop=True)


def aggregate(orders: pd.DataFrame, aggregation_: list) -> pd.DataFrame:
    """ aggregates by `aggregation` param and computes several volumes """

    return orders.groupby(aggregation_).agg(
        tradesCount=('client', 'count'),
        confirmedTrades=('isConfirmed', 'sum'),
        rfsTrades=('isRfs', 'sum'),
        eurAmount=('riskAmount', 'sum')
    ).reset_index()


extracted_records = 0


def extract_volumes(row, orders):
    global extracted_records
    extracted_records = extracted_records + 1

    if extracted_records % 10000 == 0:
        logger.info(f'extracted volumes for: {extracted_records} records')

    aggregation = ['client', 'platform', 'pair']
    c = row['client']
    pl = row['platform']
    p = row['pair']
    subset_orders = orders[(orders['client'] == c) & (orders['platform'] == pl) & (
            orders['pair'] == p) & (orders['date'] < row['date'])]

    volumes = aggregate(subset_orders, aggregation)
    if len(volumes) > 0:
        return volumes.loc[0, 'tradesCount'], volumes.loc[0, 'confirmedTrades'], volumes.loc[0, 'rfsTrades'], \
               volumes.loc[0, 'eurAmount']
    else:
        logger.warning(f'no volumes found for <{c}; {pl}; {p}>')
        return 0, 0, 0, 0


def load_data(with_profiles=True, with_volumes=False) -> pd.DataFrame:
    create_folder(OUTPUT_PREPARATION_FOLDER)

    # ---------------
    # --- FLOW DB ---
    # ---------------

    if 'data_wo_volumes_unbalanced.csv' in os.listdir(OUTPUT_PREPARATION_FOLDER):
        data = load(folder=OUTPUT_PREPARATION_FOLDER, table='data_wo_volumes_unbalanced')
    else:
        flowDB = load(table='flowDB', folder=INPUT_FOLDER,
                      date_columns=['time', 'date', 'dealTime', 'tradeDate', 'valueDate'])

        # keep records having passfail=PASS
        flowDB = filter_by_value(flowDB, column='passfail', value='PASS')

        # keep records having totalPnlPerM and markoutUSD_0 not null
        flowDB = drop_rows_with_nan_mandatory_cols(flowDB, mandatory_cols=['pnlTotalPerM', 'markoutUSD_0'])
        flowDB = flowDB.reset_index()

        # extract flowDB additional features
        flowDB['weekOfYear'] = flowDB['date'].dt.isocalendar().week
        flowDB['dayOfWeek'] = flowDB['date'].dt.day_name()
        flowDB['year'] = flowDB['date'].dt.isocalendar().year

        hours_slots = [0, 8, 12, 15, 20, 24]
        session_labels = ['EARLY_MORNING', 'MORNING', 'LAUNCH', 'AFTERNOON', 'EVENING']
        flowDB['session'] = flowDB['date'].dt.hour
        flowDB['session'] = pd.cut(flowDB['session'], bins=hours_slots, labels=session_labels, include_lowest=True)

        flowDB['target'] = flowDB['pnlTotalPerM'].apply(lambda pnl: 1 if pnl < 0 else 0)

        # rename columns
        flowDB = flowDB.rename(
            columns={"externalClientId": "client", "sym": "pair", "dealType": "productType", 'markoutUSD_0': 'markout'})

        # keep only independent features
        correlation_matrix(flowDB, folder_path=PLOT_FOLDER, suffix='flowdb_before')

        # drop pnlTotalPerM, keep only 'target' one
        flowDB = flowDB.drop(columns=['pnlTotalPerM'], errors='ignore')

        # in according to the correlation matrix we kept the following independent columns
        keys = ['date', 'platform', 'client', 'pair', 'productType', 'clientId', 'weekOfYear', 'year']
        categoricals = ['ecn', 'side', 'spotCentre', 'streamType', 'swapCentre', 'tenor', 'dayOfWeek', 'session']
        numericals = [
            # independent features - keep markout even if correlated with target?
            'dealRate', 'markout', 'amount',
            # our target label
            'target'
        ]

        flowDB = flowDB[[*keys, *numericals, *categoricals]]

        correlation_matrix(flowDB[numericals], folder_path=PLOT_FOLDER, suffix='flowdb_after')

        flowDB.to_csv(f'{OUTPUT_PREPARATION_FOLDER}/flowDB.csv', index=False)

        data = flowDB

        # ---------------
        # --- COCKPIT ---
        # ---------------

        cockpit = load(folder=INPUT_FOLDER, table='cockpitClientMetadata')
        cockpit = cockpit[['clientId', 'clientType']]

        data = pd.merge(data, cockpit, on=['clientId'], how='left')
        data['clientType'] = data['clientType'].fillna('EMPTY')

        data.to_csv(f'{OUTPUT_PREPARATION_FOLDER}/data_wo_volumes_unbalanced.csv', index=False)

    # ---------------------
    # --- UNDERSAMPLING ---
    # ---------------------
    if 'data_wo_volumes_balanced_undersampling.csv' in os.listdir(OUTPUT_PREPARATION_FOLDER):
        balanced_data = load(folder=OUTPUT_PREPARATION_FOLDER, table='data_wo_volumes_balanced_undersampling')
    else:
        hist(data, name='TargetHistBefore', title='Target Distribution Before Rebalancing')

        true_data = data[data['target'] == 1]
        false_data = data[data['target'] == 0].sample(len(true_data), random_state=23)
        balanced_data = pd.concat([true_data, false_data])

        hist(balanced_data, name='TargetHistAfter', title='Target Distribution After Rebalancing')

        balanced_data.to_csv(f'{OUTPUT_PREPARATION_FOLDER}/data_wo_volumes_balanced_undersampling.csv', index=False)

    # ---------------------
    # --- OVERSAMPLING ---
    # ---------------------
    if 'data_wo_volumes_balanced_oversampling.csv' in os.listdir(OUTPUT_PREPARATION_FOLDER):
        balanced_over_data = load(folder=OUTPUT_PREPARATION_FOLDER, table='data_wo_volumes_balanced_oversampling')
    else:
        hist(data, name='TargetHistBeforeOversampling', title='Target Distribution Before Rebalancing')

        false_data = data[data['target'] == 0]
        true_data = data[data['target'] == 1]
        oversamples = true_data.sample(len(false_data) - len(true_data), replace=True, random_state=23)
        balanced_over_data = pd.concat(
            [true_data, false_data, oversamples]).reset_index()

        hist(balanced_over_data, name='TargetHistAfterOversampling', title='Target Distribution After Rebalancing')

        balanced_over_data.to_csv(f'{OUTPUT_PREPARATION_FOLDER}/data_wo_volumes_balanced_oversampling.csv', index=False)

    # ---------------
    # --- VOLUMES ---
    # ---------------

    if with_volumes:
        clientOrder = load(table='clientOrder', folder=INPUT_FOLDER, date_columns=['date'])
        clientOrder = clientOrder.rename(
            columns={"externalClientId": "client", "sym": "pair", "dealType": "productType"})
        clientOrder = enhance_client_order_information(clientOrder)

        balanced_data[['tradesCount', 'confirmedTrades', 'rfsTrades', 'eurAmount']] = balanced_data.apply(
            lambda p: extract_volumes(p, clientOrder), axis=1, result_type='expand')

        balanced_data.to_csv(f'{OUTPUT_PREPARATION_FOLDER}/data_w_volumes_balanced.csv', index=False)

    # ----------------
    # --- PROFILES ---
    # ----------------

    if with_profiles:
        profiles = load(folder=OUTPUT_PREPARATION_FOLDER, table='profiles')

        profiles = profiles[
            ['client', 'pair', 'productType', 'isUnwise%', 'isFlat%', 'isSharp%', 'isSoftSharp%', 'isVSharp%',
             'isHardsharp%']]

        profiles = profiles.rename(
            columns={"isUnwise%": "unwise", "isFlat%": "flat", "isSharp%": "sharp", 'isSoftSharp%': 'softSharp',
                     'isVSharp%': 'vSharp', 'isHardsharp%': 'hardSharp'})

        # unbalanced
        data = pd.merge(data, profiles, on=['client', 'pair', 'productType'], how='inner')
        data['clientType'] = data['clientType'].fillna('EMPTY')
        data.to_csv(f'{OUTPUT_PREPARATION_FOLDER}/data_w_profiles_unbalanced.csv', index=False)

        # balanced
        true_data = data[data['target'] == 1]
        false_data = data[data['target'] == 0].sample(len(true_data), random_state=23)
        balanced_w_prof_data = pd.concat([true_data, false_data])
        balanced_w_prof_data.to_csv(f'{OUTPUT_PREPARATION_FOLDER}/data_w_profiles_balanced.csv', index=False)

    logger.info("Prepared dataset entries: {}".format(len(data)))

    return data


def hist(df_, name, title):
    fig = plt.figure(figsize=(8, 5))
    ax = fig.gca()
    ax.title.set_text(f'{title}')
    df_['target'].value_counts().plot(ax=ax, kind='bar')
    fig.savefig(f'{PLOT_FOLDER}/{name}.png')


def create_logistic_regressor(random_state):
    logistic = LogisticRegression(random_state=random_state, class_weight='balanced')
    param_grid = {
        'classifier__solver': ['lbfgs', 'newton-cg'],
        'classifier__penalty': ['l2'],
        'classifier__max_iter': [5000, 10000, 15000]
    }

    return logistic, param_grid, 'LOGISTIC_REGRESSION'


def create_logistic_regressor_for_random_cv(random_state):
    logistic = LogisticRegression(random_state=random_state, class_weight='balanced')
    param_grid = {
        'classifier__solver': ['lbfgs', 'newton-cg', 'sag'],
        'classifier__penalty': ['l2', 'none'],
        'classifier__max_iter': [5000, 10000, 15000, 20000, 50000, 100000],
        'classifier__tol': [0.001, 0.0001, 0.00005, 0.00001],
        'classifier__C': loguniform(1e0, 1e3),
        'classifier__multi_class': ['auto', 'ovr', 'multinomial'],
    }

    return logistic, param_grid, 'LOGISTIC_REGRESSION'


def create_random_forest_classifier(random_state):
    rf = RandomForestClassifier(random_state=random_state, class_weight='balanced')
    param_grid = {
        'classifier__max_depth': [2, 3, 4, 5, 20],
        'classifier__n_estimators': [1000],
        "classifier__max_features": [5, 6, 7],
        'classifier__min_samples_split': [10],
        'classifier__min_samples_leaf': [1],
        'classifier__criterion': ['entropy'],
        'classifier__bootstrap': [False, True],
        'classifier__class_weight': ['balanced', 'balanced_subsample']
    }

    return rf, param_grid, 'RANDOM_FOREST'


def create_random_forest_classifier_for_random_cv(random_state):
    rf = RandomForestClassifier(random_state=random_state, class_weight='balanced')
    param_grid = {
        'classifier__n_estimators': [int(x) for x in np.linspace(start=200, stop=2000, num=10)],
        'classifier__max_depth': [2, 5, 10, 20, 30, None],
        'classifier__max_features': list(range(4, 20)),
        'classifier__min_samples_split': [2, 5, 10],
        'classifier__min_samples_leaf': [1, 2, 4],
        'classifier__bootstrap': [True, False],
        'classifier__criterion': ["gini", "entropy"]
    }

    return rf, param_grid, 'RANDOM_FOREST'


def create_svc(random_state):
    svc = svm.SVC(random_state=random_state, class_weight='balanced')
    param_grid = {
        'classifier__kernel': ['poly', 'rbf'],
        'classifier__gamma': ['scale'],  # , 'auto'],
        'classifier__decision_function_shape': ['ovo', 'ovr']
    }

    return svc, param_grid, 'SVM'


def create_svc_for_random_cv(random_state):
    svc = svm.SVC(random_state=random_state, class_weight='balanced')
    param_grid = {
        'classifier__C': np.logspace(-4, 4, 4),
        'classifier__kernel': ['poly', 'rbf', 'linear', 'sigmoid'],
        'classifier__degree': [3, 5, 10],
        'classifier__gamma': loguniform(1e-4, 1e-3),
        'classifier__decision_function_shape': ['ovo', 'ovr'],
        'classifier__shrinking': [True, False],
        'classifier__tol': [0.001, 0.0001, 0.00005, 0.00001]
    }

    return svc, param_grid, 'SVM'


def create_grid_search(estimator, param_grid, cv, scoring, n_jobs):
    return GridSearchCV(estimator=estimator, param_grid=param_grid, cv=cv, scoring=scoring, n_jobs=n_jobs)


def create_halving_random_search(estimator, param_grid, cv, scoring, n_jobs):
    return HalvingRandomSearchCV(estimator=estimator, param_distributions=param_grid, cv=cv, n_jobs=n_jobs, factor=2,
                                 scoring=scoring, random_state=23)


def start(override=False):
    """ Perform PnL prediction """

    create_folder(PLOT_FOLDER)

    # tables = ['data_w_profiles_unbalanced', 'data_wo_volumes_unbalanced']
    tables = ['data']

    if override:
        load_data()

    for table in tables:
        data = load(folder=OUTPUT_PREPARATION_FOLDER, table=table)

        to_ignore = ['date', 'client', 'clientId', 'target']  # , 'markout']
        features = data.columns.tolist()
        for col in to_ignore:
            features.remove(col)

        if True:
            false_data = data[data['target'] == 0]
            oversamples = false_data.sample(len(false_data) * 1, replace=True, random_state=343)
            data = pd.concat([data, oversamples]).reset_index()

        # -- SCALED DATASET ---
        if START_BASELINE:
            baseline_path = f'{PLOT_FOLDER}/{table}/ClientTypeBaseline'
            create_folder(baseline_path)

            def client_type_baseline(row):
                client_type = row['clientType']

                if client_type in ['HEDGE FUND', 'BANK', 'INTERNAL']:
                    return 1
                return 0

            baseline_analysis(data, base_func=client_type_baseline, folder_path=baseline_path,
                              estimator_name='ClientTypeBaseline', description='PnL prediction based on client type')

        if START_GRID_SEARCH_OVERSAMPLE:
            scaled_path = f'{PLOT_FOLDER}/{table}/GridSearchCVOversampled'
            create_folder(scaled_path)

            if START_LOG_REGRESSION:
                classification_analysis(data, create_model=create_logistic_regressor,
                                        create_cross_validator=create_grid_search,
                                        folder_path=f'{scaled_path}/LogisticRegression', columns=features,
                                        cv_generator=3, rebalance='oversample',
                                        scale=True, use_pca=False)

            if START_RAND_FOREST:
                classification_analysis(data, create_model=create_random_forest_classifier,
                                        create_cross_validator=create_grid_search,
                                        folder_path=f'{scaled_path}/RandomForest', columns=features, cv_generator=3,
                                        scale=True, use_pca=False, rebalance='oversample')

            if START_SVC:
                classification_analysis(data, create_model=create_svc, create_cross_validator=create_grid_search,
                                        folder_path=f'{scaled_path}/SVC', columns=features, cv_generator=3, scale=True,
                                        use_pca=False, rebalance='oversample')

        if START_GRID_SEARCH_DOWNSAMPLE:
            scaled_path = f'{PLOT_FOLDER}/{table}/GridSearchCVDownsampled'
            create_folder(scaled_path)

            if START_LOG_REGRESSION:
                classification_analysis(data, create_model=create_logistic_regressor,
                                        create_cross_validator=create_grid_search,
                                        folder_path=f'{scaled_path}/LogisticRegression', columns=features,
                                        cv_generator=3, rebalance='downsample',
                                        scale=True, use_pca=False)

            if START_RAND_FOREST:
                classification_analysis(data, create_model=create_random_forest_classifier,
                                        create_cross_validator=create_grid_search,
                                        folder_path=f'{scaled_path}/RandomForest', columns=features, cv_generator=3,
                                        scale=True, use_pca=False, rebalance=None)  # rebalance='downsample'

            if START_SVC:
                classification_analysis(data, create_model=create_svc, create_cross_validator=create_grid_search,
                                        folder_path=f'{scaled_path}/SVC', columns=features, cv_generator=3, scale=True,
                                        use_pca=False, rebalance='downsample')

        if START_HALVING_SEARCH_DOWNSAMPLE:
            scaled_path = f'{PLOT_FOLDER}/{table}/HalvingRandomSearchCVDownsampled'
            create_folder(scaled_path)

            if START_LOG_REGRESSION:
                classification_analysis(data, create_model=create_logistic_regressor_for_random_cv,
                                        create_cross_validator=create_halving_random_search,
                                        folder_path=f'{scaled_path}/LogisticRegression', columns=features,
                                        cv_generator=3, rebalance='downsample',
                                        scale=True, use_pca=False)

            if START_RAND_FOREST:
                classification_analysis(data, create_model=create_random_forest_classifier_for_random_cv,
                                        create_cross_validator=create_halving_random_search,
                                        folder_path=f'{scaled_path}/RandomForest', columns=features, cv_generator=3,
                                        rebalance='downsample', scale=True, use_pca=False)

            if START_SVC:
                classification_analysis(data, create_model=create_svc_for_random_cv,
                                        create_cross_validator=create_halving_random_search,
                                        folder_path=f'{scaled_path}/SVC', columns=features, cv_generator=3, scale=True,
                                        rebalance='downsample', use_pca=False)

        if START_HALVING_SEARCH_OVERSAMPLE:
            scaled_path = f'{PLOT_FOLDER}/{table}/HalvingRandomSearchCVOversampled'
            create_folder(scaled_path)

            if START_LOG_REGRESSION:
                classification_analysis(data, create_model=create_logistic_regressor_for_random_cv,
                                        create_cross_validator=create_halving_random_search,
                                        folder_path=f'{scaled_path}/LogisticRegression', columns=features,
                                        cv_generator=3, rebalance='oversample',
                                        scale=True, use_pca=False)

            if START_RAND_FOREST:
                classification_analysis(data, create_model=create_random_forest_classifier_for_random_cv,
                                        create_cross_validator=create_halving_random_search,
                                        folder_path=f'{scaled_path}/RandomForest', columns=features, cv_generator=3,
                                        rebalance='oversample', scale=True, use_pca=False)

            if START_SVC:
                classification_analysis(data, create_model=create_svc_for_random_cv,
                                        create_cross_validator=create_halving_random_search,
                                        folder_path=f'{scaled_path}/SVC', columns=features, cv_generator=3, scale=True,
                                        rebalance='oversample', use_pca=False)


if __name__ == '__main__':
    start(override=False)