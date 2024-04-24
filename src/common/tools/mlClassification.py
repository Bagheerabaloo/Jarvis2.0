import time

import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.compose import ColumnTransformer
from sklearn.decomposition import PCA
from sklearn.feature_selection import RFE
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import precision_recall_curve, PrecisionRecallDisplay, f1_score
from sklearn.metrics import mean_squared_error
# to keep for HalvingRandomSearchCV
from sklearn.experimental import enable_halving_search_cv
from sklearn.model_selection import train_test_split, HalvingRandomSearchCV
from sklearn import metrics, svm
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder

from .library import *

# create logger
logger = init_logger(name='classification')


# TODO add classification methods


def correlation_matrix(df_: pd.DataFrame, method='pearson', save=True, folder_path=None, suffix=None):
    fig, ax = plt.subplots(figsize=(19, 15))

    corr = df_.corr(method=method)
    sns.heatmap(corr, annot=True, fmt='.4f', cmap=plt.get_cmap('coolwarm'), ax=ax)
    ax.set_yticklabels(ax.get_yticklabels(), rotation="horizontal")

    if save:
        assert folder_path is not None
        fig.savefig('{}/{}.png'.format(folder_path, f'CorrelationMatrix_{suffix if suffix else ""}'),
                    bbox_inches='tight', pad_inches=0.0)
    return corr


# --- METRICS ---

def accuracy(y, y_pred, path, estimator_id):
    # compute accuracy
    score = metrics.accuracy_score(y, y_pred)
    logger.info(f'{estimator_id} accuracy: {score}')

    cm = metrics.confusion_matrix(y, y_pred)

    fig, ax = plt.subplots()
    sns.heatmap(cm, annot=True, fmt=".3f", linewidths=.5, square=True, cmap='Blues_r', ax=ax)
    plt.ylabel('Actual label')
    plt.xlabel('Predicted label')
    plt.title(f'Accuracy Score: {score}', size=15)
    plt.tight_layout()
    fig.savefig('{}/{}.png'.format(path, f'Accuracy_{estimator_id}'))

    return score


def precision_recall(y, y_pred, path, estimator_id):
    fig, ax = plt.subplots()
    precision, recall, _ = precision_recall_curve(y, y_pred)
    logger.info(f'{estimator_id} precision: {precision}; recall: {recall}')

    PrecisionRecallDisplay.from_predictions(y, y_pred, ax=ax)

    plt.title(f'Precision={precision} - Recall={recall}', size=15)
    plt.tight_layout()

    fig.savefig('{}/{}.png'.format(path, f'PrecisionRecall_{estimator_id}'))

    return precision, recall


def f1(y, y_pred, path, estimator_id, pos_label=1):
    score = f1_score(y, y_pred, average='binary', pos_label=pos_label)
    logger.info(f'{estimator_id} F1 for label={pos_label}: {score}')

    return score


def fit_extracting_main_features(estimator, x_tr, y_tr, n_features=5):
    selector = RFE(estimator, n_features_to_select=n_features)
    selector.fit(x_tr, y_tr)

    feature_ranks = []
    for i in selector.ranking_[:n_features]:
        feature_ranks.append(x_tr.columns[i])

    return feature_ranks


# --- LOGISTIC REGRESSION ---

def logistic_regression_analysis(df_: pd.DataFrame, target='target', columns=None, test_size=0.25, main_features=True,
                                 folder_path=None):
    tries = [
        ('lbfgs', 'l2', 1000, None),
        ('newton-cg', 'l2', 1500, None),
        # ('newton-cg', 'l2', 3000, None),
    ]

    folder_path = f'{folder_path}/LogisticRegression'
    create_folder(folder_path)

    for solver, penalty, max_iter, l1_ratio in tries:
        logistic_regression(df_, target, columns, test_size=test_size, main_features=main_features, solver=solver,
                            penalty=penalty,
                            max_iter=max_iter, l1_ratio=l1_ratio, folder_path=folder_path)


def random_forest_analysis(df_: pd.DataFrame, target='target', columns=None, test_size=0.25, main_features=True,
                           folder_path=None):
    folder_path = f'{folder_path}/RandomForestGrid'
    create_folder(folder_path)

    random_forest(df_, target, columns, test_size=test_size, main_features=main_features, folder_path=folder_path)


def logistic_regression(df_: pd.DataFrame, target='target', columns=None, test_size=0.25, main_features=True,
                        solver='lbfgs', penalty='l2', max_iter=100, l1_ratio=None, random_state=23,
                        folder_path=None) -> (LogisticRegression, list):
    estimator_name = f'{solver}_{penalty}_{max_iter}_{l1_ratio}'

    if columns is None:
        columns = [x for x in df_.columns if x != target]

    x_train, x_test, y_train, y_test = train_test_split(df_[columns], df_[target], test_size=test_size,
                                                        random_state=random_state)

    # create logistic regression
    logistic = LogisticRegression(penalty=penalty, solver=solver, random_state=random_state, max_iter=max_iter,
                                  l1_ratio=l1_ratio)

    # fit model - also extract top 10 features
    logger.info('computing important features..')
    important_features = fit_extracting_main_features(logistic, x_train, y_train, n_features=10)
    logger.info(f'main features: {important_features}')

    if main_features:
        x_train = x_train[important_features]
        x_test = x_test[important_features]

    logistic.fit(x_train, y_train)

    # compute predictions
    predictions = logistic.predict(x_test)

    # accuracy
    accuracy(y_test, predictions, folder_path, estimator_name)

    # precision and recall
    precision_recall(y_test, predictions, folder_path, estimator_name)

    # F1
    f1(y_test, predictions, folder_path, estimator_name, pos_label=0)
    f1(y_test, predictions, folder_path, estimator_name, pos_label=1)

    return logistic, important_features


def random_forest(df_: pd.DataFrame, target='target', columns=None, test_size=0.25, main_features=True,
                  solver='lbfgs', penalty='l2', max_iter=100, l1_ratio=None, random_state=23,
                  folder_path=None) -> (LogisticRegression, list):
    estimator_name = f'{solver}_{penalty}_{max_iter}_{l1_ratio}'

    if columns is None:
        columns = [x for x in df_.columns if x != target]

    x_train, x_test, y_train, y_test = train_test_split(df_[columns], df_[target], test_size=test_size,
                                                        random_state=random_state)

    # create Random Forest Regressor
    rf = RandomForestClassifier(random_state=27)
    rf.get_params()

    params = {'n_estimators': 2, 'max_depth': 10}
    rf.set_params(**params)
    rf.get_params()

    grid = {'max_depth': [2, 5, 10],
            'n_estimators': [150]}

    cv = GridSearchCV(estimator=rf, param_grid=grid, cv=3)

    cv.fit(x_train, y_train)
    print(cv.best_params_, cv.best_score_)

    # compute predictions
    predictions = cv.best_estimator_.predict(x_test)

    important_features = fit_extracting_main_features(cv.best_estimator_, x_train, y_train, n_features=10)
    print(important_features)

    results = pd.DataFrame(y_test)
    results['prediction'] = predictions

    rmse = mean_squared_error(results['target'], results['prediction'], squared=False)
    print("Test RMSE = %f" % rmse)

    # accuracy
    accuracy(y_test, predictions, folder_path, estimator_name)

    # precision and recall
    precision_recall(y_test, predictions, folder_path, estimator_name)

    # F1
    f1(y_test, predictions, folder_path, estimator_name, pos_label=0)
    f1(y_test, predictions, folder_path, estimator_name, pos_label=1)

    return cv.best_estimator_, important_features


# --- GENERIC CLASSIFICATION ANALYSIS ---

def current_millis():
    return round(time.time() * 1000)


def current_seconds():
    return time.time()


def plot_pca(pca_, x, search, path):
    pca_.fit(x)

    fig, (ax0, ax1) = plt.subplots(nrows=2, sharex=True, figsize=(6, 6))
    ax0.plot(
        np.arange(1, pca_.n_components_ + 1), pca_.explained_variance_ratio_, "+", linewidth=2
    )
    ax0.set_ylabel("PCA explained variance ratio")

    ax0.axvline(
        search.best_estimator_.named_steps["pca"].n_components,
        linestyle=":",
        label="n_components chosen",
    )

    fig.savefig('{}/{}.png'.format(path, f'PCA'))


def extract_categoricals(df_: pd.DataFrame) -> list:
    return df_.select_dtypes(include=["object"]).columns.tolist()


def extract_numericals(df_: pd.DataFrame) -> list:
    return df_.select_dtypes(include=["number"]).columns.tolist()


def plot_cv_results(cv_: HalvingRandomSearchCV, path: str, estimator_id: str):
    fig, ax = plt.subplots()  # figsize=(19, 15)

    results = pd.DataFrame(cv_.cv_results_)
    results["params_str"] = results.params.apply(str)
    results.drop_duplicates(subset=("params_str", "iter"), inplace=True)
    mean_scores = results.pivot(
        index="iter", columns="params_str", values="mean_test_score"
    )
    mean_scores.plot(legend=False, alpha=0.6, ax=ax)

    labels = [
        f"iter={i}\nn_samples={cv_.n_resources_[i]}\nn_candidates={cv_.n_candidates_[i]}"
        for i in range(cv_.n_iterations_)
    ]

    ax.set_xticks(range(cv_.n_iterations_))
    ax.set_xticklabels(labels, rotation=90, multialignment="left")
    ax.set_title("Scores of candidates over iterations")
    ax.set_ylabel("mean test score", fontsize=15)
    ax.set_xlabel("iterations", fontsize=15)
    plt.tight_layout()

    fig.savefig('{}/{}.png'.format(path, f'RandomCV{estimator_id}'))

    return mean_scores


def classification_analysis(df_: pd.DataFrame, create_model, create_cross_validator, folder_path, target='target',
                            columns=None, test_size=0.20, random_state=534, cv_generator=3, rebalance='oversample',
                            rebalance_factor=1.7, scale=True, use_pca=False):
    """
    Attributes:
        df_: Input dataframe
        create_model: function that returns a tuple (classifier, param_grid)
        folder_path: path to the folder where plots will be stored
    """

    # create plot folder
    create_folder(folder_path)

    if columns is None:
        features = [x for x in df_.columns if x != target]
    else:
        features = columns

    dataset = df_[features]
    x_train, x_test, y_train, y_test = train_test_split(dataset, df_[target], test_size=test_size,
                                                        random_state=random_state, stratify=df_[target])
    train = x_train
    train[target] = y_train

    if rebalance is not None:
        if rebalance == 'downsample':
            logger.debug('applying downsampling..')
            true_data = train[train['target'] == 1]
            fact = round(len(true_data) * rebalance_factor)
            false_data = train[train['target'] == 0].sample(fact, replace=False, random_state=random_state)
            train = pd.concat([true_data, false_data])
        elif rebalance == 'oversample':
            logger.debug('applying oversampling..')
            false_data = train[train['target'] == 0]
            true_data = train[train['target'] == 1]
            fact = (len(false_data) - len(true_data)) * (1 - (rebalance_factor - 1))
            oversamples = true_data.sample(round(fact), replace=True,
                                           random_state=random_state)
            train = pd.concat(
                [true_data, false_data, oversamples]).reset_index()
        else:
            raise RuntimeError(f'Invalid rebalancing method: {rebalance}')

    x_train, y_train = (train.drop([target], axis=1), train[target])

    categorical_features = extract_categoricals(dataset)
    numerical_features = extract_numericals(dataset)

    categorical_transformer = OneHotEncoder(handle_unknown="ignore")
    numeric_transformer = Pipeline(
        steps=[('inputer', SimpleImputer(strategy="median"))]
    )

    if scale:
        # add numeric standard scaler
        numeric_transformer.steps.append(["scaler", StandardScaler()])

    # data preprocessor
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, numerical_features),
            ("cat", categorical_transformer, categorical_features),
        ]
    )

    # model estimator
    estimator, grid, estimator_name = create_model(random_state=random_state)

    pipeline = Pipeline(
        steps=[("preprocessor", preprocessor), ("classifier", estimator)]
    )

    if use_pca:
        # add pca as second step
        pca, pca_grid = create_pca()
        grid.update(pca_grid)
        pipeline.steps.insert(1, ("pca", pca))

    # , scoring=['accuracy', 'f1'], refit='accuracy'
    cv = create_cross_validator(estimator=pipeline, param_grid=grid, cv=cv_generator, n_jobs=-1, scoring='recall')

    start_time = current_seconds()

    # fit pipeline
    logger.info(f'starting model {estimator_name}')
    logger.info(f'using train dataset size = {len(x_train)}')
    logger.info(f'using features = {features}')
    cv.fit(x_train, y_train)

    if isinstance(cv, HalvingRandomSearchCV):
        plot_cv_results(cv, folder_path, estimator_name)

    print(cv.best_params_, cv.best_score_)

    # compute predictions
    predictions = cv.best_estimator_.predict(x_test)

    if use_pca:
        plot_pca(pipeline.steps[1], x_train, cv, folder_path)

    # accuracy
    acc = accuracy(y_test, predictions, folder_path, estimator_name)

    # precision and recall
    prec, rec = precision_recall(y_test, predictions, folder_path, estimator_name)

    # F1
    f1_0 = f1(y_test, predictions, folder_path, estimator_name, pos_label=0)
    f1_1 = f1(y_test, predictions, folder_path, estimator_name, pos_label=1)

    with open(f'{folder_path}/config.txt', 'w+') as file:
        file.write(f'Features: {features}\n')
        file.write(f'Rebalancing method: {rebalance}\n')
        file.write(f'Rebalancing factor: {rebalance_factor}\n')
        file.write(f'Train size: {len(x_train)}\n')
        file.write(f'Duration: {current_seconds() - start_time}\n')
        file.write(f'Best params: {cv.best_params_}\n')
        file.write(f'Best score: {cv.best_score_}\n')
        file.write(f'Accuracy: {acc}\n')
        file.write(f'Precision: {prec}\n')
        file.write(f'Recall: {rec}\n')
        file.write(f'F1 - label 0: {f1_0}\n')
        file.write(f'F1 - label 1: {f1_1}\n')

    return cv.best_estimator_


def create_pca():
    pca = PCA()
    param_grid = {
        'pca__n_components': [2, 5, 10, 15]
    }

    return pca, param_grid


def baseline_analysis(df_: pd.DataFrame, base_func, folder_path, estimator_name, description, target='target',
                      test_size=0.2, random_state=23435):
    x_train, x_test, y_train, y_test = train_test_split(df_, df_[target], test_size=test_size,
                                                        random_state=random_state, stratify=df_[target])

    predictions = x_test.apply(lambda row: base_func(row), axis=1)

    # accuracy
    acc = accuracy(y_test, predictions, folder_path, estimator_name)

    # precision and recall
    prec, rec = precision_recall(y_test, predictions, folder_path, estimator_name)

    # F1
    f1_0 = f1(y_test, predictions, folder_path, estimator_name, pos_label=0)
    f1_1 = f1(y_test, predictions, folder_path, estimator_name, pos_label=1)

    with open(f'{folder_path}/config.txt', 'w+') as file:
        file.write(f'Description: {description}\n')
        file.write(f'Accuracy: {acc}\n')
        file.write(f'Precision: {prec}\n')
        file.write(f'Recall: {rec}\n')
        file.write(f'F1 - label 0: {f1_0}\n')
        file.write(f'F1 - label 1: {f1_1}\n')


