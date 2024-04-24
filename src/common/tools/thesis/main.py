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

# initial setup
pd.options.display.max_columns = None

# create logger
logger = init_logger(name='Preparation')

PLOT_FOLDER = 'plots'
FOLDER_NAME = timestamp2date(time(), frmt='%m_%d_%H_%M_%S') + ' Main Analysis'

OVERRIDE_PREPARATION = False


def save_fig(fig, file_name, folder=FOLDER_NAME):
    files = os.listdir(PLOT_FOLDER)
    if folder not in files:
        os.mkdir('{}/{}'.format(PLOT_FOLDER, folder))
    fig.savefig('{}/{}/{}.png'.format(PLOT_FOLDER, folder, file_name))


# _____ Main _____
def main_analysis(override=False):
    table_name = 'final_df_per_quarter_profiled'
    output_path = 'data/05_profile_analysis_without_T0'

    # ___ 0 - Load profiled dataFrame ___
    df = load(table=table_name, folder=output_path)
    logger.info("Initial entries: {}".format(len(df)))

    # ___ 4 - PROFILE ANALYSIS MERGE BACK ___
    df = df.drop(columns=['CBT3PCount', 'CDTTOBCount', 'CM2MCount', 'FQIsA', 'FQIsB', 'FQIsC', 'FQIsD', 'FQLPRCount', 'FlowQuartileIsA', 'FlowQuartileIsB',
                          'FlowQuartileIsC', 'FlowQuartileIsD', 'LPRIsA', 'LPRIsB', 'LPRIsC', 'LPRIsD', 'LPRelevanceIsA', 'LPRelevanceIsB',
                          'LPRelevanceIsC', 'LPRelevanceIsD', 'LPRelevanceIsEmpty', 'M1', 'M120', 'M180', 'M30', 'M300', 'M5', 'M60', 'M600',
                          'T1', 'T10', 'T120', 'T180', 'T240', 'T30', 'T300', 'T5', 'T60', 'onApril', 'onAugust', 'onDecember', 'onFebruary', 'onFriday',
                          'onJanuary', 'onJuly', 'onJune', 'onMarch', 'onMay', 'onMonday', 'onNovember', 'onOctober', 'onSaturday', 'onSeptember',
                          'onSunday', 'onThursday', 'onTuesday', 'onWednesday'], errors='ignore')
    df = move_columns_to_first_position(df, ['client', 'pair', 'productType', 'year', 'quarter', 'BestPriceMean', 'Top3PriceMean', 'DistanceToTOB',
                                             'clientRelevance', 'hitRatio', 'CoEURAmount', 'FDEURAmount', 'CoTradesCount', 'CoConfirmedTrades',
                                             'FDtradesCount', 'T0', 'M0', 'clientType',
                                             'isTSharp', 'isTFlat', 'isTUnwise', 'isMSharp', 'isMFlat', 'isMUnwise',
                                             'pnlAHUSD', 'pnlAdjUSD', 'pnlInceptionUSD', 'pnlMatchUSD', 'pnlTotalPerM', 'pnlTotalUSD', 'dealRate'
                                             ])

    # TODO: 1 - Compare profile 360T results with profile FlowDB results
    # TODO: 2 - Compare profile analysis with PnL

    # ____ 5 - FILTERING ___
    filtered_df = df[(df['hitRatio'].notna()) & (df['productType'] != 'SWAP')]  # ___ Filter only valid entries ___
    # filtered_df = df[(df['hitRatio'].notna()) & (df['productType'] != 'SWAP') & (df['isSharp'].notna())]  # ___ Filter only valid entries ___
    # filtered_df = final_df[(final_df['BestPriceMean'] > 0) & (final_df['productType'] != 'SWAP')]  # ___ Filter only valid entries ___
    # filtered_df = filter_by_values(df, {'BestPriceNA': False, 'Top3PriceNA': False, 'LPRelevanceNA': False})
    logger.info("Total entries after filtering only valid entries: {}".format(len(filtered_df)))

    # TODO: Does BestPrice mean that the trade was executed?

    # df_1 = filtered_df[['clientRelevance', 'hitRatio', 'CoEURAmount']]
    # sb = Seaborn(df_1)
    # sb.pair_plot()
    # sb.show()

    logger.info(f'======== start analysis ========\n')

    # TODO: use markout from FlowDB apart UCTrader, having more than 10 trades for all the year
    # TODO: for those that have less than 10 trades, use OTHER LPs


if __name__ == '__main__':
    main_analysis(override=False)