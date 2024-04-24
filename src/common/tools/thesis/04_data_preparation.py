import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from src.utils import pretty_print
from src.utils_dataframe import *
from src.utils import *
from src.plot_seaborn import Seaborn

# initial setup
pd.options.display.max_columns = None

# create logger
logger = init_logger(name='Preparation')

OVERRIDE_360T = True
OVERRIDE_EXCEED = True
OVERRIDE_MERGE = True


# _____ Extract and Group by Quarter _____
def extract_quarter(week):
    # TODO: 1) verify if there is a better way to group weeks into quarters 2) verify bug output different from same input (not idempotente)
    return 4 if week == 53 else (week - 1) // 13 + 1


# _____ Prepare single 360T and ExCEED tables _____
def extract_and_group_by_quarter(table, aggregate, aggregation) -> Optional[pd.DataFrame]:
    folder = 'data/03_preprocessed'

    # _____ 1 - Loading _____
    df = load(table=table, folder=folder, sort_columns=True, date_columns=[])
    logger.debug(f'Total {table} entries loaded from csv: {len(df)}')

    # assert not is_null(df)      # some precondition checks
    df = df.rename(columns={"externalClientId": "client", "sym": "pair", "dealType": "productType"}, errors='ignore')

    # _____ 2 - Merge _____
    df['quarter'] = df['weekOfYear'].apply(lambda x: extract_quarter(x))
    df = df.drop(columns=['weekOfYear'], errors='ignore')

    df_grouped = aggregate(df, aggregation)

    return df_grouped


def prepare_client_best_top_3_price(aggregation) -> Optional[pd.DataFrame]:
    def aggregate(df_: pd.DataFrame, aggregation_: list) -> pd.DataFrame:
        """ aggregates by `aggregation` param and computes several volumes """

        df_ = df_.groupby(aggregation_).agg(
            BestPriceMean=('BestPriceMean', 'mean'),
            BestPriceZeroCount=('BestPriceZeroCount', 'sum'),
            Top3PriceMean=('Top3PriceMean', 'mean'),
            Top3PriceZeroCount=('Top3PriceZeroCount', 'sum'),
            CBT3PCount=('CBT3PCount', 'sum'),
        ).reset_index()

        return df_

    table = 'ClientBestPriceTop3'
    return extract_and_group_by_quarter(table, aggregate, aggregation)


def prepare_client_distance_to_tob(aggregation) -> Optional[pd.DataFrame]:
    def aggregate(df_: pd.DataFrame, aggregation_: list) -> pd.DataFrame:
        """ aggregates by `aggregation` param and computes several volumes """

        df_ = df_.groupby(aggregation_).agg(
            DistanceToTOB=('DistanceToTOB', 'mean'),
            CDTTOBCount=('CDTTOBCount', 'sum'),
            # CDTTOBisForward=('CDTTOBisForward', 'sum'),
            # CDTTOBisSpot=('CDTTOBisSpot', 'sum'),
            # CDTTOBisSwap=('CDTTOBisSwap', 'sum'),
            # CDTTOBisNdf=('CDTTOBisNdf', 'sum'),
            # CDTTOBisBlockSpot=('CDTTOBisBlockSpot', 'sum'),
            # CDTTOBisBlock=('CDTTOBisBlock', 'sum'),
            # CDTTOBisSliceOrder=('CDTTOBisSliceOrder', 'sum'),
            # CDTTOBisOption=('CDTTOBisOption', 'sum')
        ).reset_index()

        return df_

    table = 'ClientDistancetoTOB'
    return extract_and_group_by_quarter(table, aggregate, aggregation)


def prepare_client_m2m(aggregation) -> Optional[pd.DataFrame]:
    def aggregate(df_: pd.DataFrame, aggregation_: list) -> pd.DataFrame:
        """ aggregates by `aggregation` param and computes several volumes """

        df_ = df_.groupby(aggregation_).agg(
            T_5=('T_5', 'mean'),
            T_2=('T_2', 'mean'),
            T_1=('T_1', 'mean'),
            T0=('T0', 'mean'),
            T1=('T1', 'mean'),
            T5=('T5', 'mean'),
            T10=('T10', 'mean'),
            T30=('T30', 'mean'),
            T60=('T60', 'mean'),
            T120=('T120', 'mean'),
            T180=('T180', 'mean'),
            T240=('T240', 'mean'),
            T300=('T300', 'mean'),
            CM2MCount=('client', 'count'),
        ).reset_index()

        return df_

    table = 'ClientM2M'
    return extract_and_group_by_quarter(table, aggregate, aggregation)


def prepare_flow_quartile_and_lp_relevance(aggregation) -> Optional[pd.DataFrame]:
    def aggregate(df_: pd.DataFrame, aggregation_: list) -> pd.DataFrame:
        """ aggregates by `aggregation` param and computes several volumes """

        df_ = df_.groupby(aggregation_).agg(
            FlowQuartileIsA=('FlowQuartileIsA', 'sum'),
            FlowQuartileIsB=('FlowQuartileIsB', 'sum'),
            FlowQuartileIsC=('FlowQuartileIsC', 'sum'),
            FlowQuartileIsD=('FlowQuartileIsD', 'sum'),
            LPRelevanceIsEmpty=('LPRelevanceIsEmpty', 'sum'),
            LPRelevanceIsA=('LPRelevanceIsA', 'sum'),
            LPRelevanceIsB=('LPRelevanceIsB', 'sum'),
            LPRelevanceIsC=('LPRelevanceIsC', 'sum'),
            LPRelevanceIsD=('LPRelevanceIsD', 'sum'),
            FQLPRCount=('client', 'count')
        ).reset_index()

        return df_

    table = 'FlowQuartileAndLPRelevance'
    return extract_and_group_by_quarter(table, aggregate, aggregation)


def prepare_flow_db(aggregation) -> Optional[pd.DataFrame]:
    def aggregate(df_: pd.DataFrame, aggregation_: list) -> pd.DataFrame:
        """ aggregates by `aggregation` param and computes several volumes """

        df_ = df_.groupby(aggregation_).agg(
            FDtradesCount=('FDtradesCount', 'sum'),
            FDrfsTrades=('FDrfsTrades', 'sum'),
            FDespTrades=('FDespTrades', 'sum'),
            # __ deal types __
            # spotTrades=('spotTrades', 'sum'),
            # fwdTrades=('fwdTrades', 'sum'),
            # __ side __
            FDisSell=('FDisSell', 'sum'),
            FDisBuy=('FDisBuy', 'sum'),
            # __ platforms __
            FDEURAmount=('FDEURAmount', 'sum'),
            # USDAmount=('USDAmount', 'sum'),
            # dealRate=('dealRate', 'mean'),
            clientId=('clientId', 'first'),
            accountId=('accountId', 'first'),
            markoutUSD_0=('markoutUSD_0', 'mean'),
            markoutUSD_1=('markoutUSD_1', 'mean'),
            markoutUSD_5=('markoutUSD_5', 'mean'),
            markoutUSD_30=('markoutUSD_30', 'mean'),
            markoutUSD_60=('markoutUSD_60', 'mean'),
            markoutUSD_120=('markoutUSD_120', 'mean'),
            markoutUSD_180=('markoutUSD_180', 'mean'),
            markoutUSD_300=('markoutUSD_300', 'mean'),
            markoutUSD_600=('markoutUSD_600', 'mean'),
            pnlAHUSD=('pnlAHUSD', 'mean'),
            pnlAdjUSD=('pnlAdjUSD', 'mean'),
            pnlInceptionUSD=('pnlInceptionUSD', 'mean'),
            pnlMatchUSD=('pnlMatchUSD', 'mean'),
            pnlTotalPerM=('pnlTotalPerM', 'mean'),
            pnlTotalUSD=('pnlTotalUSD', 'mean')
        ).reset_index()

        return df_

    table = 'flowDB'
    return extract_and_group_by_quarter(table, aggregate, aggregation)


def prepare_client_order(aggregation) -> Optional[pd.DataFrame]:
    def aggregate(df_: pd.DataFrame, aggregation_: list) -> pd.DataFrame:
        """ aggregates by `aggregation` param and computes several volumes """

        df_ = df_.groupby(aggregation_).agg(
            CoTradesCount=('CoTradesCount', 'sum'),
            CoConfirmedTrades=('CoConfirmedTrades', 'sum'),
            CoRfsTrades=('CoRfsTrades', 'sum'),
            CoEURAmount=('C0EURAmount', 'sum'),
            # deal types
            # swapTrades=('swapTrades', 'sum'),
            # spotTrades=('spotTrades', 'sum'),
            # fwdTrades=('fwdTrades', 'sum'),
            # blockTrades=('blockTrades', 'sum'),
            # takeUpTrades=('takeUpTrades', 'sum'),
            # timeOptionTrades=('timeOptionTrades', 'sum'),
            # ndfTrades=('ndfTrades', 'sum'),
            # week's days
            onMonday=('onMonday', 'sum'),
            onTuesday=('onTuesday', 'sum'),
            onWednesday=('onWednesday', 'sum'),
            onThursday=('onThursday', 'sum'),
            onFriday=('onFriday', 'sum'),
            onSaturday=('onSaturday', 'sum'),
            onSunday=('onSunday', 'sum'),
            # months
            onJanuary=('onJanuary', 'sum'),
            onFebruary=('onFebruary', 'sum'),
            onMarch=('onMarch', 'sum'),
            onApril=('onApril', 'sum'),
            onMay=('onMay', 'sum'),
            onJune=('onJune', 'sum'),
            onJuly=('onJuly', 'sum'),
            onAugust=('onAugust', 'sum'),
            onSeptember=('onSeptember', 'sum'),
            onOctober=('onOctober', 'sum'),
            onNovember=('onNovember', 'sum'),
            onDecember=('onDecember', 'sum'),
            # platforms
            # onUct=('onUct', 'sum'),
            # on360t=('on360t', 'sum'),
            # onUfx=('onUfx', 'sum'),
            # onFxall=('onFxall', 'sum'),
            # onBloomberg=('onBloomberg', 'sum'),
            # onTobo=('onTobo', 'sum'),
            # onFastmatch=('onFastmatch', 'sum'),
        ).reset_index()

        return df_

    table = 'clientOrder'
    return extract_and_group_by_quarter(table, aggregate, aggregation)


# _____ Merge and save 360T and Exceed tables _____
def prepare_360t_tables(to_csv=True, override=False):
    override = override or OVERRIDE_360T
    table_name = '360T'
    input_path = 'data/03_preprocessed'
    output_path = 'data/04_prepared'
    # aggregation = ['client', 'pair', 'productType', 'year', 'quarter']
    aggregation = ['client', 'pair', 'productType', 'year', 'weekOfYear']

    # _____ 0 - Override _____
    if not override and f"{table_name}.csv" in os.listdir(output_path):
        logger.info(f'======== 360T merging skipped. File already present ========\n')
        return move_columns_to_first_position(load(table=table_name, folder=output_path, sort_columns=True, date_columns=[]), aggregation)

    logger.info(f'======== start 360T merging ========')
    # # _____ 1 - Loading _____
    # client_best_price_top_3_df = prepare_client_best_top_3_price(aggregation=aggregation)
    # client_distance_to_tob_df = prepare_client_distance_to_tob(aggregation=aggregation)
    # client_m2m_df = prepare_client_m2m(aggregation=aggregation)
    # flow_quartile_and_lp_relevance_df = prepare_flow_quartile_and_lp_relevance(aggregation=aggregation)

    client_best_price_top_3_df = load(table='ClientBestPriceTop3', folder=input_path, sort_columns=True, date_columns=[])
    client_distance_to_tob_df = load(table='ClientDistancetoTOB', folder=input_path, sort_columns=True, date_columns=[])
    client_m2m_df = load(table='ClientM2M', folder=input_path, sort_columns=True, date_columns=[])
    flow_quartile_and_lp_relevance_df = load(table='FlowQuartileAndLPRelevance', folder=input_path, sort_columns=True, date_columns=[])

    # _____ 2 - Merging _____
    df = pd.merge(client_best_price_top_3_df, client_distance_to_tob_df, on=aggregation, how='outer')
    df = pd.merge(df, client_m2m_df, on=aggregation, how='outer')
    df = pd.merge(df, flow_quartile_and_lp_relevance_df, on=aggregation, how='outer')
    df = move_columns_to_first_position(df, aggregation)
    logger.debug('Total entries after merging: {}'.format(len(df)))

    # _____ 3 - Save _____
    logger.debug('Total entries before saving to csv: {}'.format(len(df)))
    df.to_csv(f'{output_path}/{table_name}.csv', index=False) if to_csv else None

    # _____ 4 - Plotting _____
    # plot_client_trend(exceed, ext_client_id='TSR METALS', platform='360T', sym='EURGBP', features=[])

    logger.info(f'======== end 360T merging ========\n')

    return df


def prepare_exceed_tables(to_csv=True, override=False):
    def exceed_aggregation(df: pd.DataFrame, aggr: list) -> pd.DataFrame:
        """ aggregates by `aggregation` param and computes several volumes """

        aggregated = df.groupby(aggr).agg(
            # deals data
            amount=('amount', 'mean'),
            clientAllInRate=('clientAllInRate', 'mean'),
            clientSpotRate=('clientSpotRate', 'mean'),
            dealAmount=('dealAmount', 'mean'),
            dealRate=('dealRate', 'mean'),
            markoutUSD_0=('markoutUSD_0', 'mean'),
            markoutUSD_1=('markoutUSD_1', 'mean'),
            markoutUSD_5=('markoutUSD_5', 'mean'),
            markoutUSD_30=('markoutUSD_30', 'mean'),
            markoutUSD_60=('markoutUSD_60', 'mean'),
            markoutUSD_120=('markoutUSD_120', 'mean'),
            markoutUSD_180=('markoutUSD_180', 'mean'),
            markoutUSD_300=('markoutUSD_300', 'mean'),
            markoutUSD_600=('markoutUSD_600', 'mean'),
            riskAllInRate=('riskAllInRate', 'mean'),
            riskAmount=('riskAmount', 'mean'),
            riskSpotRate=('riskSpotRate', 'mean'),
            # trades count
            aggregatedTrades=('aggregatedTrades', 'sum'),
            tradesCount=('tradesCount', 'sum'),
            confirmedTrades=('confirmedTrades', 'sum'),
            rfsTrades=('rfsTrades', 'sum'),
            # deal types
            swapTrades=('swapTrades', 'sum'),
            spotTrades=('spotTrades', 'sum'),
            fwdTrades=('fwdTrades', 'sum'),
            blockTrades=('blockTrades', 'sum'),
            takeUpTrades=('takeUpTrades', 'sum'),
            timeOptionTrades=('timeOptionTrades', 'sum'),
            ndfTrades=('ndfTrades', 'sum'),
            # week's days
            onMonday=('onMonday', 'sum'),
            onTuesday=('onTuesday', 'sum'),
            onWednesday=('onWednesday', 'sum'),
            onThursday=('onThursday', 'sum'),
            onFriday=('onFriday', 'sum'),
            onSaturday=('onSaturday', 'sum'),
            onSunday=('onSunday', 'sum'),
            # months
            onJanuary=('onJanuary', 'sum'),
            onFebruary=('onFebruary', 'sum'),
            onMarch=('onMarch', 'sum'),
            onApril=('onApril', 'sum'),
            onMay=('onMay', 'sum'),
            onJune=('onJune', 'sum'),
            onJuly=('onJuly', 'sum'),
            onAugust=('onAugust', 'sum'),
            onSeptember=('onSeptember', 'sum'),
            onOctober=('onOctober', 'sum'),
            onNovember=('onNovember', 'sum'),
            onDecember=('onDecember', 'sum'),
            # platforms
            onUct=('onUct', 'sum'),
            on360t=('on360t', 'sum'),
            onUfx=('onUfx', 'sum'),
            onFxall=('onFxall', 'sum'),
            onBloomberg=('onBloomberg', 'sum'),
            onTobo=('onTobo', 'sum'),
            onFastmatch=('onFastmatch', 'sum'),
        ).reset_index()

        return aggregated

    def plot_client_trend(df: pd.DataFrame, ext_client_id: str, platform: str, sym: str, features: list,
                          color_map=None):
        """
        Plot the client trend, over all weeks, for a specific client and for a
        specific feature if not None, otherwise for all features
        """

        if color_map is None:
            color_map = {}

        if features is None or len(features) == 0:
            # show trend for all numerical features
            features = df.select_dtypes(include=[np.number]).columns.values

        # consider only data for the given client and platform

        if sym is None:
            platform_mean = df[(df['platform'] == platform)].groupby(['week']).mean().reset_index()
            df = df[(df['externalClientId'] == ext_client_id) & (df['platform'] == platform)]
        else:
            platform_mean = df[(df['platform'] == platform) & (df['sym'] == sym)].groupby(['week']).mean().reset_index()
            df = df[(df['externalClientId'] == ext_client_id) & (df['platform'] == platform) & (df['sym'] == sym)]

        subplot_size = len(features) * 4
        fig, axs = plt.subplots(nrows=len(features), ncols=1, figsize=(12, subplot_size))

        def get_ax(index):
            if len(features) > 1:
                return axs[index]
            return axs

        for idx, feature in enumerate(features):
            ax = get_ax(idx)
            ax.set_title(to_snake_case(feature).upper())
            ax.set(xlabel='weeks', ylabel=feature)

            color = color_map.get(feature, 'green')

            # current client
            ax.plot(df['week'],
                    df[feature],
                    color=color)

            # platform mean
            ax.plot(platform_mean['week'],
                    platform_mean[feature],
                    color='red')

            ax.legend([ext_client_id, 'mean'])

        fig.tight_layout()

        plt.show()

    override = override or OVERRIDE_EXCEED
    input_path = 'data/03_preprocessed'
    output_path = 'data/04_prepared'
    table_name = 'exceed_360t'
    table_name_2 = 'exceed'
    # aggregation = ['client', 'pair', 'productType', 'year', 'quarter', 'platform']
    aggregation = ['client', 'pair', 'productType', 'year', 'weekOfYear', 'platform']

    # _____ 0 - OVERRIDE _____
    if not override and f"{table_name}.csv" in os.listdir(output_path):
        logger.info(f'======== ExCEED merging skipped. File already present ========\n')
        return move_columns_to_first_position(load(table=table_name, folder=output_path, sort_columns=True, date_columns=[]), aggregation)

    # _____ 1 - LOADING _____
    logger.info(f'======== start ExCEED merging ========\n')

    flow_db_df = load(table='flowDB', folder=input_path, sort_columns=True, date_columns=[])
    client_order_df = load(table='clientOrder', folder=input_path, sort_columns=True, date_columns=[])
    cockpit_client_df = load(table='cockpitClientMetadata', folder=input_path, sort_columns=True, date_columns=[])

    # _____ 2 - MERGE _____
    exceed = pd.merge(flow_db_df, cockpit_client_df, on=['clientId'], how='left')  # merge flowDB and cockpit client data
    exceed = pd.merge(exceed, client_order_df, on=aggregation, how='outer')
    exceed = move_columns_to_first_position(exceed, aggregation + ['CoEURAmount', 'FDEURAmount', 'CoConfirmedTrades', 'CoTradesCount', 'hitRatio', 'FDtradesCount'])

    exceed.to_csv(f'{output_path}/{table_name_2}.csv', index=False) if to_csv else None

    exceed = filter_by_value(exceed, 'platform', '360T')  # filter only 360T clients
    logger.debug('Total entries after 360T filtering: {}'.format(len(exceed)))

    # _____ 3 - Save _____
    logger.debug('Total entries before saving to csv: {}'.format(len(exceed)))
    exceed.to_csv(f'{output_path}/{table_name}.csv', index=False) if to_csv else None

    # _____ 4 - Plotting _____
    # plot_client_trend(exceed, ext_client_id='TSR METALS', platform='360T', sym='EURGBP', features=[])

    logger.info(f'======== end ExCEED merging ========\n')

    return exceed


# _____ Main _____
def main_preparation(to_csv=True, override=False):
    def aggregate(df_: pd.DataFrame, aggregation_: list) -> pd.DataFrame:
        """ aggregates by `aggregation` param and computes several volumes """

        df_ = df_.groupby(aggregation_).agg(
            BestPriceMean=('BestPriceMean', 'mean'),
            BestPriceZeroCount=('BestPriceZeroCount', 'sum'),
            Top3PriceMean=('Top3PriceMean', 'mean'),
            Top3PriceZeroCount=('Top3PriceZeroCount', 'sum'),
            CBT3PCount=('CBT3PCount', 'sum'),
            DistanceToTOB=('DistanceToTOB', 'mean'),
            CDTTOBCount=('CDTTOBCount', 'sum'),
            T_5=('T_5', 'mean'),
            T_2=('T_2', 'mean'),
            T_1=('T_1', 'mean'),
            T0=('T0', 'mean'),
            T1=('T1', 'mean'),
            T5=('T5', 'mean'),
            T10=('T10', 'mean'),
            T30=('T30', 'mean'),
            T60=('T60', 'mean'),
            T120=('T120', 'mean'),
            T180=('T180', 'mean'),
            T240=('T240', 'mean'),
            T300=('T300', 'mean'),
            CM2MCount=('client', 'count'),
            FlowQuartileIsA=('FlowQuartileIsA', 'sum'),
            FlowQuartileIsB=('FlowQuartileIsB', 'sum'),
            FlowQuartileIsC=('FlowQuartileIsC', 'sum'),
            FlowQuartileIsD=('FlowQuartileIsD', 'sum'),
            LPRelevanceIsEmpty=('LPRelevanceIsEmpty', 'sum'),
            LPRelevanceIsA=('LPRelevanceIsA', 'sum'),
            LPRelevanceIsB=('LPRelevanceIsB', 'sum'),
            LPRelevanceIsC=('LPRelevanceIsC', 'sum'),
            LPRelevanceIsD=('LPRelevanceIsD', 'sum'),
            FQLPRCount=('client', 'count'),
            FDtradesCount=('FDtradesCount', 'sum'),
            FDrfsTrades=('FDrfsTrades', 'sum'),
            FDespTrades=('FDespTrades', 'sum'),
            FDisSell=('FDisSell', 'sum'),
            FDisBuy=('FDisBuy', 'sum'),
            FDEURAmount=('FDEURAmount', 'sum'),
            clientId=('clientId', 'first'),
            accountId=('accountId', 'first'),
            clientName=('clientName', 'first'),
            clientType=('clientType', 'first'),
            dealRate=('dealRate', 'mean'),
            M0=('M0', 'mean'),
            M1=('M1', 'mean'),
            M5=('M5', 'mean'),
            M30=('M30', 'mean'),
            M60=('M60', 'mean'),
            M120=('M120', 'mean'),
            M180=('M180', 'mean'),
            M300=('M300', 'mean'),
            M600=('M600', 'mean'),
            pnlAHUSD=('pnlAHUSD', 'mean'),
            pnlAdjUSD=('pnlAdjUSD', 'mean'),
            pnlInceptionUSD=('pnlInceptionUSD', 'mean'),
            pnlMatchUSD=('pnlMatchUSD', 'mean'),
            pnlTotalPerM=('pnlTotalPerM', 'mean'),
            pnlTotalUSD=('pnlTotalUSD', 'mean'),
            CoTradesCount=('CoTradesCount', 'sum'),
            CoConfirmedTrades=('CoConfirmedTrades', 'sum'),
            CoRfsTrades=('CoRfsTrades', 'sum'),
            CoEURAmount=('CoEURAmount', 'sum'),
            onMonday=('onMonday', 'sum'),
            onTuesday=('onTuesday', 'sum'),
            onWednesday=('onWednesday', 'sum'),
            onThursday=('onThursday', 'sum'),
            onFriday=('onFriday', 'sum'),
            onSaturday=('onSaturday', 'sum'),
            onSunday=('onSunday', 'sum'),
            onJanuary=('onJanuary', 'sum'),
            onFebruary=('onFebruary', 'sum'),
            onMarch=('onMarch', 'sum'),
            onApril=('onApril', 'sum'),
            onMay=('onMay', 'sum'),
            onJune=('onJune', 'sum'),
            onJuly=('onJuly', 'sum'),
            onAugust=('onAugust', 'sum'),
            onSeptember=('onSeptember', 'sum'),
            onOctober=('onOctober', 'sum'),
            onNovember=('onNovember', 'sum'),
            onDecember=('onDecember', 'sum'),
        ).reset_index()

        return df_

    override_merge = override or OVERRIDE_MERGE
    output_path = 'data'
    table_name = 'final_df'

    # _____ 0 - Override _____
    if not override_merge and f"{table_name}.csv" in os.listdir(output_path):
        logger.info(f'======== final merging skipped. File already present ========\n')
        return None

    # _____ 1 - Merge 360T and ExCEED in two separate df _____
    aggregation = ['client', 'pair', 'productType', 'year', 'weekOfYear']
    # aggregation = ['client', 'pair', 'productType', 'year', 'quarter']
    three_sixty_t = prepare_360t_tables(override=override)
    exceed = prepare_exceed_tables(override=override)

    # _____ 2 - Merge 360T and ExCEED into the final dataFrame _____
    logger.info(f'======== start final merging ========\n')
    df_per_week = pd.merge(three_sixty_t, exceed, on=aggregation, how='outer')  # merge 360T and ExCEED dataFrames
    df_per_week['quarter'] = df_per_week['weekOfYear'].apply(lambda x: extract_quarter(x))

    # _____ 3 - Create dataFrame grouped by quarter _____
    aggregation = ['client', 'pair', 'productType', 'year', 'quarter']
    df_per_quarter = aggregate(df_per_week, aggregation)
    df_per_quarter['hitRatio'] = df_per_quarter['CoConfirmedTrades'] / df_per_quarter['CoTradesCount']

    # _____ 4 - SAVE _____
    logger.debug('Total per Week dataFrame entries before saving to csv: {}'.format(len(df_per_week)))
    df_per_week.to_csv(f'{output_path}/{table_name}_per_week.csv', index=False) if to_csv else None

    logger.debug('Total per Quarter dataFrame entries before saving to csv: {}'.format(len(df_per_week)))
    df_per_quarter.to_csv(f'{output_path}/{table_name}_per_quarter.csv', index=False) if to_csv else None

    # _____ 5 - PLOT _____
    # plot_client_trend(exceed, ext_client_id='TSR METALS', platform='360T', sym='EURGBP', features=[])

    logger.info(f'======== end final merging ========\n')


if __name__ == '__main__':
    main_preparation(override=False)