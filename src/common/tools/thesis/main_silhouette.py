from time import time

from src.ml_clustering import *
from src.matplotLib_plot import *
from src.utils_dataframe import *
from src.utils import *
from src.utils import timestamp2date

# initial setup
pd.options.display.max_columns = None

# create logger
logger = init_logger(name='Silhouette')

# PLOT_FOLDER = 'plots/silhouette'
# PLOT_FOLDER = 'plots/silhouette_with_volumes'
PLOT_FOLDER = 'plots/silhouette_near'
FOLDER_NAME = timestamp2date(time(), frmt='%m_%d_%H_%M_%S') + ' Main Analysis'


def aggregate(df_, cols, f, not_null_coll='T1%'):
    profile_df = df_[df_[not_null_coll].notna()][cols]
    profile_df = profile_df.replace([np.inf, -np.inf], np.nan).dropna()
    aggregation = ['client', 'pair', 'productType', 'year', 'quarter']
    return f(profile_df, aggregation)


def save_figs(figs_, analysis='analysis'):
    if analysis not in os.listdir(PLOT_FOLDER):
        os.mkdir('{}/{}'.format(PLOT_FOLDER, analysis))

    for idx, fig in enumerate(figs_):
        fig.savefig('{}/{}.png'.format(PLOT_FOLDER, f'{analysis}/{idx + 2}silhouette_score'))


def main_silhouette_profile_analysis():
    def aggregate_by_absolute(profile_df_, aggregation):
        aggregated_df = profile_df_.groupby(aggregation).agg(
            T1=('M1', 'mean'),
            T5=('M5', 'mean'),
            T30=('M30', 'mean'),
            T60=('M60', 'mean'),
            T120=('M120', 'mean'),
            T180=('M180', 'mean'),
            T300=('M300', 'mean'),
            T600=('M600', 'mean'),
            trades=('CoConfirmedTrades', 'mean')
        ).reset_index()

        return aggregated_df

    # markoutUSD_0,markoutUSD_1,markoutUSD_120,markoutUSD_180,markoutUSD_30,markoutUSD_300,markoutUSD_5,markoutUSD_60,markoutUSD_600
    def aggregate_by_markout(profile_df_, aggregation):
        aggregated_df = profile_df_.groupby(aggregation).agg(
            T1=('markoutUSD_1', 'mean'),
            T5=('markoutUSD_5', 'mean'),
            T30=('markoutUSD_30', 'mean'),
            T60=('markoutUSD_60', 'mean'),
            T120=('markoutUSD_120', 'mean'),
            T180=('markoutUSD_180', 'mean'),
            T300=('markoutUSD_300', 'mean'),
            T600=('markoutUSD_600', 'mean'),
            trades=('CoConfirmedTrades', 'mean')
        ).reset_index()

        return aggregated_df

    def aggregate_by_derivative(profile_df_, aggregation):
        aggregated_df = profile_df_.groupby(aggregation).agg(
            T1=('T1%', 'mean'),
            T5=('T5%', 'mean'),
            T10=('T10%', 'mean'),
            T30=('T30%', 'mean'),
            T60=('T60%', 'mean'),
            T120=('T120%', 'mean'),
            T180=('T180%', 'mean'),
            T240=('T240%', 'mean'),
            T300=('T300%', 'mean'),
            trades=('CoConfirmedTrades', 'mean')
        ).reset_index()

        return aggregated_df

    def aggregate_by_incrementer(profile_df_, aggregation):
        aggregated_df = profile_df_.groupby(aggregation).agg(
            T1=('TI1', 'mean'),
            T5=('TI5', 'mean'),
            T10=('TI10', 'mean'),
            T30=('TI30', 'mean'),
            T60=('TI60', 'mean'),
            T120=('TI120', 'mean'),
            T180=('TI180', 'mean'),
            T240=('TI240', 'mean'),
            T300=('TI300', 'mean'),
            trades=('CoConfirmedTrades', 'mean')
        ).reset_index()

        return aggregated_df

    def aggregate_by_delta_incrementer(profile_df_, aggregation):
        aggregated_df = profile_df_.groupby(aggregation).agg(
            T1=('TIF1', 'mean'),
            T5=('TIF5', 'mean'),
            T10=('TIF10', 'mean'),
            T30=('TIF30', 'mean'),
            T60=('TIF60', 'mean'),
            T120=('TIF120', 'mean'),
            T180=('TIF180', 'mean'),
            T240=('TIF240', 'mean'),
            T300=('TIF300', 'mean'),
            trades=('CoConfirmedTrades', 'mean')
        ).reset_index()

        return aggregated_df

    table_name = 'final_df_prepared'
    output_path = 'data'
    df = load(table=table_name, folder=output_path)
    logger.info("Initial entries: {}".format(len(df)))

    default_columns = ['T1', 'T5', 'T30', 'T60', 'T120']  # , 'T180', 'T300', 'T600']#, 'trades']

    # --- Absolute ---
    logger.info("starting silhouette for absolute..")
    client_df = aggregate(df, ['client', 'pair', 'productType', 'year', 'quarter', 'M1', 'M5', 'M30', 'M60',
                               'M120', 'M180', 'M300', 'M600', 'CoConfirmedTrades'], aggregate_by_absolute,
                          not_null_coll='M1')
    client_df = standard_scaler(client_df, ['trades'])
    figs = silhouette_analysis(client_df[default_columns], max_k=7)
    save_figs(figs, 'absolute')

    # --- Markout ---
    logger.info("starting silhouette for markouts..")
    client_df = aggregate(df, ['client', 'pair', 'productType', 'year', 'quarter', 'markoutUSD_1', 'markoutUSD_5',
                               'markoutUSD_30', 'markoutUSD_60', 'markoutUSD_120', 'markoutUSD_180', 'markoutUSD_300',
                               'markoutUSD_600', 'CoConfirmedTrades'],
                          aggregate_by_markout, not_null_coll='markoutUSD_1')
    client_df = standard_scaler(client_df, ['trades'])
    figs = silhouette_analysis(client_df[default_columns], max_k=7)
    save_figs(figs, 'markout')

    # --- Baseline ---
    def baseline(row, n_cluster):
        threshold = 2
        diff = abs(row['T30'] - row['T1'])
        sign = row['T30'] < row['T1']
        if diff > threshold and sign:
            return 0
        else:
            if n_cluster == 2:
                return 1
            elif n_cluster == 3:
                if diff > threshold and not sign:
                    return 2
                else:
                    return 1
            else:
                raise Exception("Invalid number of clusters")

    client_df = aggregate(df, ['client', 'pair', 'productType', 'year', 'quarter', 'markoutUSD_1',
                               'markoutUSD_5', 'markoutUSD_30', 'markoutUSD_60', 'markoutUSD_120', 'markoutUSD_180',
                               'markoutUSD_300', 'markoutUSD_600', 'CoConfirmedTrades'], aggregate_by_markout,
                          not_null_coll='markoutUSD_1')
    client_df = standard_scaler(client_df, ['trades'])
    figs = silhouette_analysis(client_df[default_columns], max_k=4, use_ml=False,
                               deterministic_algorithm=baseline)
    save_figs(figs, 'baseline')

    default_columns = ['T1', 'T5', 'T10', 'T30', 'T60', 'T120']  # , 'T180', 'T240', 'T300']#, 'trades']

    # --- Percentage ---
    logger.info("starting silhouette for percentage..")
    client_df = aggregate(df, ['client', 'pair', 'productType', 'year', 'quarter', 'T1%', 'T5%', 'T10%', 'T30%', 'T60%',
                               'T120%', 'T180%', 'T240%', 'T300%', 'CoConfirmedTrades'], aggregate_by_derivative)
    client_df = client_df[
        (client_df['T1'].abs() < 10) & (client_df['T5'].abs() < 10) & (client_df['T10'].abs() < 10) & (
                client_df['T30'].abs() < 10) & (client_df['T60'].abs() < 10) &
        (client_df['T120'].abs() < 10) & (client_df['T180'].abs() < 10) & (client_df['T240'].abs() < 10) & (
                client_df['T300'].abs() < 10)]
    client_df = standard_scaler(client_df, ['trades'])

    figs = silhouette_analysis(client_df[default_columns], max_k=7)
    save_figs(figs, 'percentage')

    # --- Incremental ---
    logger.info("starting silhouette for incremental..")
    client_df = aggregate(df, ['client', 'pair', 'productType', 'year', 'quarter', 'TI1', 'TI5', 'TI10', 'TI30', 'TI60',
                               'TI120', 'TI180', 'TI240', 'TI300', 'CoConfirmedTrades'], aggregate_by_incrementer)
    client_df = standard_scaler(client_df, ['trades'])
    figs = silhouette_analysis(client_df[default_columns], max_k=7)
    save_figs(figs, 'incremental')

    # --- Delta Incremental ---
    logger.info("starting silhouette for delta incremental..")
    client_df = aggregate(df, ['client', 'pair', 'productType', 'year', 'quarter', 'TIF1', 'TIF5', 'TIF10', 'TIF30',
                               'TIF60', 'TIF120', 'TIF180', 'TIF240', 'TIF300', 'CoConfirmedTrades'],
                          aggregate_by_delta_incrementer)
    client_df = standard_scaler(client_df, ['trades'])
    figs = silhouette_analysis(client_df[default_columns], max_k=7)
    save_figs(figs, 'delta_incremental')

    # --- Random Baseline ---
    def random_baseline(row, n_cluster):
        import random
        return random.randint(0, n_cluster)

    client_df = aggregate(df, ['client', 'pair', 'productType', 'year', 'quarter', 'TIF1', 'TIF5', 'TIF10', 'TIF30',
                               'TIF60', 'TIF120', 'TIF180', 'TIF240', 'TIF300', 'CoConfirmedTrades'],
                          aggregate_by_delta_incrementer)
    client_df = standard_scaler(client_df, ['trades'])
    figs = silhouette_analysis(client_df[default_columns], max_k=7, use_ml=False,
                               deterministic_algorithm=random_baseline)
    save_figs(figs, 'random baseline')


def main_silhouette_client_analysis():
    def aggregate_by_incrementer(profile_df_, aggregation):
        aggregated_df = profile_df_.groupby(aggregation).agg(
            T1=('TI1', 'mean'),
            T5=('TI5', 'mean'),
            T10=('TI10', 'mean'),
            T30=('TI30', 'mean'),
            T60=('TI60', 'mean'),
            T120=('TI120', 'mean'),
            T180=('TI180', 'mean'),
            T240=('TI240', 'mean'),
            T300=('TI300', 'mean'),
            trades=('CoConfirmedTrades', 'mean'),
            relevance=('clientRelevance', 'mean'),
            eurAmount=('CoEURAmount', 'mean'),
            bestPrice=('BestPriceMean', 'mean')
        ).reset_index()

        return aggregated_df

    table_name = 'final_df_prepared'
    output_path = 'data'
    df = load(table=table_name, folder=output_path)
    logger.info("Initial entries: {}".format(len(df)))

    default_columns = ['T1', 'T5', 'T10', 'T30', 'T60', 'T120', 'T180', 'T240', 'T300', 'eurAmount', 'relevance',
                       'trades', 'bestPrice']

    # --- Incremental ---
    logger.info("starting silhouette for incremental..")
    client_df = aggregate(df, ['client', 'pair', 'productType', 'year', 'quarter', 'TI1', 'TI5', 'TI10', 'TI30', 'TI60',
                               'TI120', 'TI180', 'TI240', 'TI300', 'CoConfirmedTrades', 'clientRelevance',
                               'CoEURAmount', 'BestPriceMean'], aggregate_by_incrementer)
    client_df = standard_scaler(client_df, ['trades', 'relevance', 'eurAmount', 'bestPrice'])
    figs = silhouette_analysis(client_df[default_columns], max_k=12)
    save_figs(figs, 'client_incremental')


if __name__ == '__main__':
    main_silhouette_profile_analysis()
    # main_silhouette_client_analysis()