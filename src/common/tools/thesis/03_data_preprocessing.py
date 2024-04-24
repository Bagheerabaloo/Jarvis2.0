import calendar

import matplotlib.pyplot as plt

from src.utils_dataframe import *

# initial setup
pd.options.display.max_columns = None

# create logger
logger = init_logger(name='Preprocessing')

OVERRIDE_ALL = False

OVERRIDE_CLIENT_BEST_TOP3PRICE = False
OVERRIDE_CLIENT_DISTANCE_TO_TOB = False
OVERRIDE_CLIENT_M2M = True
OVERRIDE_FLOW_QUARTILE_AND_LP_RELEVANCE = False

OVERRIDE_FLOW_DB = True
OVERRIDE_CLIENT_ORDER = False
OVERRIDE_COCKPIT_TABLES = False


# _____ 360T ______

def preprocess_360t_tables(save_csv=True, override=False):
    preprocess_client_best_top_3_price(to_csv=save_csv, override=override or OVERRIDE_CLIENT_BEST_TOP3PRICE or OVERRIDE_ALL)
    preprocess_client_distance_to_tob(to_csv=save_csv, override=override or OVERRIDE_CLIENT_DISTANCE_TO_TOB or OVERRIDE_ALL)
    preprocess_client_m2m(to_csv=save_csv, override=override or OVERRIDE_CLIENT_M2M or OVERRIDE_ALL)
    preprocess_flow_quartile_and_lp_relevance(to_csv=save_csv, override=override or OVERRIDE_FLOW_QUARTILE_AND_LP_RELEVANCE or OVERRIDE_ALL)


def preprocess_client_best_top_3_price(to_csv=False, override=False) -> Optional[pd.DataFrame]:
    def aggregate(df_: pd.DataFrame, aggregation_: list) -> pd.DataFrame:
        """ aggregates by `aggregation` param and computes several volumes """

        df_ = df_.groupby(aggregation_).agg(
            BestPriceMean=('prevWeekBestPrice', 'mean'),
            BestPriceZeroCount=('prevWeekBestPriceIsZero', 'sum'),
            Top3PriceMean=('prevWeekTop3Price', 'mean'),
            Top3PriceZeroCount=('prevWeekTop3PriceIsZero', 'sum'),
            CBT3PCount=('client', 'count'),
        ).reset_index()

        return df_

    table_name = 'ClientBestPriceTop3'
    aggregation = ['client', 'pair', 'productType', 'year', 'weekOfYear']

    # _____ 0 - Override _____
    if not override and f"{table_name}.csv" in os.listdir('data/03_preprocessed'):
        logger.info(f'======== {table_name} preprocessing skipped. File already present ========\n')
        return None

    # _____ 1 - Loading _____
    logger.info(f'======== start {table_name} pre-processing ========')

    plot_folder = f'plots/{table_name}'
    base_dir = 'data/02_cleaned'
    df = load(table=table_name, folder=base_dir)
    logger.debug(f'Total entries loaded from csv: {len(df)}')

    # ___ 2 - Pre-processing ___
    df = df[(df['category'] == 'PRODUCTTYPE VS PAIR') & (df['filter'] == 'NONE')]  # filter by category and filter
    # df[(df['category'] == 'PRODUCTTYPE VS PAIR') & (df['client'] == 'ABB ZURICH') & (df['categoryDefinition'].str.contains('EURUSD')) & (df['filter'] == 'NONE')]
    # df[(df['category'] == 'PAIR') & (df['client'] == 'ABB ZURICH') & (df['categoryDefinition'].str.contains('EURUSD')) & (df['filter'] == 'NONE')]
    # df = df[(df['category'] == 'PAIR') & (df['filter'] != 'NONE')]  # filter by category and filter

    df[['productType', 'pair']] = df['categoryDefinition'].str.split(';', 1, expand=True)

    # Rename RFS SPOT and SST SPOT to SPOT - TODO: what's the difference between RFS SPOT and SST SPOT?
    df['productType'] = df['productType'].str.replace('RFS SPOT', 'SPOT')
    df['productType'] = df['productType'].str.replace('SST SPOT', 'SPOT')
    df['productType'] = df['productType'].str.replace('RFQ BOOST SPOT', 'SPOT')

    """Ci sono casi in cui il cliente non ha mai tradato con noi, quindi prevWeekBestPrice=0, 
    ma possiamo aver avuto un prezzo nella TOP 3 list. 
    Quindi, abbiamo anche il caso in cui il client e non trada con noi, quindi prevWeekBestPrice=0, 
    e inoltre i prezzi mostrati al cliente non erano parte della top 3 list, PrevWeekTop3Price=0. 
    Se il cliente non ha chiesto an oi quel prodotto/ccy/tenor ecc non abbiamo questi dettagli.
    """
    # trace zero values: 1 if value is zero, 0 if value is different from 0
    df['prevWeekBestPriceIsZero'] = df['prevWeekBestPrice'].apply(lambda x: 1 if x == 0 else 0)
    df['prevWeekTop3PriceIsZero'] = df['prevWeekTop3Price'].apply(lambda x: 1 if x == 0 else 0)

    # Subtract 1 week to date - 360T reports data 1 week later
    df['date'] = df['date'].astype('datetime64[ns]')
    df['date'] = df['date'] - pd.to_timedelta(7, unit='d')

    # extract year, weekOfYear and quarter
    df['weekOfYear'] = df['date'].dt.isocalendar().week
    df['year'] = df['date'].dt.year
    df = df.drop(columns=['date'], errors='ignore')

    # Drop columns not more needed
    # df = df.rename(columns={"categoryDefinition": "Pair"})
    df = df.drop(columns=['category', 'categoryDefinition', 'filterDefinition', 'filter'], errors='ignore')

    # Aggregate dataFrame by aggregation
    df_grouped = aggregate(df, aggregation)
    df_grouped = move_columns_to_first_position(df_grouped, ['client', 'pair', 'productType', 'year', 'weekOfYear'])

    assert not contains_null(df)  # assert no null values are still present

    # _____ 3 - Save file _____
    logger.debug('Total entries before saving to csv: {}'.format(len(df_grouped)))
    output_path = 'data/03_preprocessed'
    df_grouped.to_csv(f'{output_path}/{table_name}.csv', index=False) if to_csv else None

    # _____ 4 - Plotting ______
    # plotting()

    logger.info(f'======== end {table_name} pre-processing ========\n')

    return df_grouped


def preprocess_client_distance_to_tob(to_csv=False, override=False) -> Optional[pd.DataFrame]:
    def aggregate(df_: pd.DataFrame, aggregation_: list) -> pd.DataFrame:
        """ aggregates by `aggregation` param and computes several volumes """

        df_ = df_.groupby(aggregation_).agg(
            DistanceToTOB=('prevWeekDistanceToTOB', 'mean'),
            CDTTOBCount=('client', 'count'),
            # CDTTOBisForward=('CDTTOBProductTypeIsForward', 'sum'),
            # CDTTOBisSpot=('CDTTOBProductTypeIsRfsspot', 'sum'),
            # CDTTOBisSwap=('CDTTOBProductTypeIsSwap', 'sum'),
            # CDTTOBisNdf=('CDTTOBProductTypeIsNdf', 'sum'),
            # CDTTOBisBlockSpot=('CDTTOBProductTypeIsBlockrfsspot', 'sum'),
            # CDTTOBisBlock=('CDTTOBProductTypeIsBlock', 'sum'),
            # CDTTOBisSliceOrder=('CDTTOBProductTypeIsSliceorder', 'sum'),
            # CDTTOBisOption=('CDTTOBProductTypeIsOption', 'sum')
        ).reset_index()

        return df_

    table_name = 'ClientDistancetoTOB'
    aggregation = ['client', 'pair', 'productType', 'year', 'weekOfYear']

    # _____ 0 - Override _____
    if not override and f"{table_name}.csv" in os.listdir('data/03_preprocessed'):
        logger.info(f'======== {table_name} preprocessing skipped. File already present ========\n')
        return None

    # _____ 1 - Loading _____
    logger.info(f'======== start {table_name} pre-processing ========')

    plot_folder = f'plots/{table_name}'
    base_dir = 'data/02_cleaned'
    df = load(table=table_name, folder=base_dir)
    logger.debug(f'Total entries loaded from csv: {len(df)}')

    # ___ 2 - Pre-processing ___
    df = df[df['filter'] == 'NONE']  # no filter applied

    # Extract ProductType and Pair
    df[['productType', 'pair']] = df['categoryDefinition'].str.split(';', 1, expand=True)

    # Rename RFS SPOT and SST SPOT to SPOT - TODO: what's the difference between RFS SPOT and SST SPOT?
    df['productType'] = df['productType'].str.replace('RFS SPOT', 'SPOT')
    df['productType'] = df['productType'].str.replace('SST SPOT', 'SPOT')
    df['productType'] = df['productType'].str.replace('RFQ BOOST SPOT', 'SPOT')

    df = df.drop(columns=['categoryDefinition', 'filter', 'filterDefinition'], errors='ignore')

    # remove not valid entries
    df = df[df['prevWeekDistanceToTOB'] >= 0]

    # df['CDTTOBProductType'] = df['CDTTOBProductType'].apply(lambda x: to_camelcase(x))
    # df = one_hot_encode(df, ['CDTTOBProductType'], drop_columns=True, preserve_col_name=True)

    # Subtract 1 week to date - 360T reports data 1 week later
    df['date'] = df['date'].astype('datetime64[ns]')
    df['date'] = df['date'] - pd.to_timedelta(7, unit='d')

    # extract year, weekOfYear and quarter
    df['weekOfYear'] = df['date'].dt.isocalendar().week
    df['year'] = df['date'].dt.year
    df = df.drop(columns=['date'], errors='ignore')

    # Aggregate by aggregation
    df_grouped = aggregate(df, aggregation)
    df_grouped = move_columns_to_first_position(df_grouped, ['client', 'pair', 'productType', 'year', 'weekOfYear'])

    assert not contains_null(df)  # assert no null values are still present

    # _____ 3 - Save file _____
    logger.debug('Total entries before saving to csv: {}'.format(len(df_grouped)))
    output_path = 'data/03_preprocessed'
    df_grouped.to_csv(f'{output_path}/{table_name}.csv', index=False) if to_csv else None

    # _____ 4 - Plotting ______
    # plotting()

    logger.info(f'======== end {table_name} pre-processing ========\n')

    return df_grouped


def preprocess_client_m2m(to_csv=False, override=False) -> Optional[pd.DataFrame]:
    def aggregate(df_: pd.DataFrame, aggregation_: list) -> pd.DataFrame:
        """ aggregates by `aggregation` param and computes several volumes """

        df_ = df_.groupby(aggregation_).agg(
            T_5=('T-5', 'mean'),
            T_2=('T-2', 'mean'),
            T_1=('T-1', 'mean'),
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

        return df_.groupby(aggregation_).mean().reset_index()

    table_name = 'ClientM2M'
    aggregation = ['client', 'pair', 'productType', 'year', 'weekOfYear']

    # _____ 0 - Override _____
    if not override and f"{table_name}.csv" in os.listdir('data/03_preprocessed'):
        logger.info(f'======== {table_name} preprocessing skipped. File already present ========\n')
        return None

    # _____ 1 - Loading _____
    logger.info(f'======== start {table_name} pre-processing ========')

    plot_folder = f'plots/{table_name}'
    base_dir = 'data/02_cleaned'
    df = load(table=table_name, folder=base_dir)
    logger.debug(f'Total entries loaded from csv: {len(df)}')

    # ___ 2 - Pre-processing ___
    df = df[(df['category'] == 'PRODUCTTYPE VS PAIR')]  # filter by category
    df[['productType', 'pair']] = df['definition'].str.split(';', 1, expand=True)

    # Rename RFS SPOT and SST SPOT to SPOT - TODO: what's the difference between RFS SPOT and SST SPOT?
    df['productType'] = df['productType'].str.replace('RFS SPOT', 'SPOT')
    df['productType'] = df['productType'].str.replace('SST SPOT', 'SPOT')
    df['productType'] = df['productType'].str.replace('RFQ BOOST SPOT', 'SPOT')

    df = df.drop(columns=['category', 'definition'], errors='ignore')

    # Subtract 1 week to date - 360T reports data 1 week later
    df['date'] = df['date'].astype('datetime64[ns]')
    df['date'] = df['date'] - pd.to_timedelta(7, unit='d')

    # extract year, weekOfYear and quarter
    df['weekOfYear'] = df['date'].dt.isocalendar().week
    df['year'] = df['date'].dt.year
    df = df.drop(columns=['date'], errors='ignore')

    # Aggregate by aggregation
    df_grouped = aggregate(df, aggregation)
    df_grouped = move_columns_to_first_position(df_grouped, ['client', 'Pair', 'year', 'weekOfYear'])

    assert not contains_null(df)  # assert no null values are still present

    # _____ 3 - Save file _____
    logger.debug('Total entries before saving to csv: {}'.format(len(df_grouped)))
    output_path = 'data/03_preprocessed'
    df_grouped.to_csv(f'{output_path}/{table_name}.csv', index=False) if to_csv else None

    # _____ 4 - Plotting ______
    # plotting()

    logger.info(f'======== end {table_name} pre-processing ========\n')

    return df_grouped


def preprocess_flow_quartile_and_lp_relevance(to_csv=False, override=False) -> Optional[pd.DataFrame]:
    def aggregate(df_: pd.DataFrame, aggregation_: list) -> pd.DataFrame:
        """ aggregates by `aggregation` param and computes several volumes """

        df_ = df_.groupby(aggregation_).agg(
            FlowQuartileIsA=('prevWeekFlowQuartileIsA', 'sum'),
            FlowQuartileIsB=('prevWeekFlowQuartileIsB', 'sum'),
            FlowQuartileIsC=('prevWeekFlowQuartileIsC', 'sum'),
            FlowQuartileIsD=('prevWeekFlowQuartileIsD', 'sum'),
            LPRelevanceIsEmpty=('prevWeekLPRelevanceIsEmpty', 'sum'),
            LPRelevanceIsA=('prevWeekLPRelevanceIsA', 'sum'),
            LPRelevanceIsB=('prevWeekLPRelevanceIsB', 'sum'),
            LPRelevanceIsC=('prevWeekLPRelevanceIsC', 'sum'),
            LPRelevanceIsD=('prevWeekLPRelevanceIsD', 'sum'),
            FQLPRCount=('client', 'count')
        ).reset_index()

        return df_

    table_name = 'FlowQuartileAndLPRelevance'
    aggregation = ['client', 'pair', 'productType', 'year', 'weekOfYear']

    # _____ 0 - Override _____
    if not override and f"{table_name}.csv" in os.listdir('data/03_preprocessed'):
        logger.info(f'======== {table_name} preprocessing skipped. File already present ========\n')
        return None

    # _____ 1 - Loading _____
    logger.info(f'======== start {table_name} pre-processing ========')

    plot_folder = f'plots/{table_name}'
    base_dir = 'data/02_cleaned'
    df = load(table=table_name, folder=base_dir)
    logger.debug(f'Total entries loaded from csv: {len(df)}')

    # ___ 2 - Pre-processing ___
    df = df[(df['category'] == 'PRODUCTTYPE VS PAIR')]  # filter by category
    df[['productType', 'pair']] = df['categoryDefinition'].str.split(';', 1, expand=True)

    # Rename RFS SPOT and SST SPOT to SPOT - TODO: what's the difference between RFS SPOT and SST SPOT?
    df['productType'] = df['productType'].str.replace('RFS SPOT', 'SPOT')
    df['productType'] = df['productType'].str.replace('SST SPOT', 'SPOT')
    df['productType'] = df['productType'].str.replace('RFQ BOOST SPOT', 'SPOT')

    df = df.drop(columns=['category', 'categoryDefinition'], errors='ignore')

    # Subtract 1 week to date - 360T reports data 1 week later
    df['date'] = df['date'].astype('datetime64[ns]')
    df['date'] = df['date'] - pd.to_timedelta(7, unit='d')

    # extract year, weekOfYear and quarter
    df['weekOfYear'] = df['date'].dt.isocalendar().week
    df['year'] = df['date'].dt.year
    df = df.drop(columns=['date'], errors='ignore')

    df = one_hot_encode(df, ['prevWeekFlowQuartile', 'prevWeekLPRelevance'], drop_columns=True, preserve_col_name=True)

    # Aggregate by aggregation
    df_grouped = aggregate(df, aggregation)
    df_grouped = move_columns_to_first_position(df_grouped, ['client', 'pair', 'productType', 'year', 'weekOfYear'])

    assert not contains_null(df)  # assert no null values are still present

    # _____ 3 - Save file _____
    logger.debug('Total entries before saving to csv: {}'.format(len(df_grouped)))
    output_path = 'data/03_preprocessed'
    df_grouped.to_csv(f'{output_path}/{table_name}.csv', index=False) if to_csv else None

    # _____ 4 - Plotting ______
    # plotting()

    logger.info(f'======== end {table_name} pre-processing ========\n')

    return df_grouped


# _____ EXCEED ______

def preprocess_exceed_tables(save_csv=True, override=False):
    preprocess_flow_db(to_csv=save_csv, override=override or OVERRIDE_FLOW_DB or OVERRIDE_ALL)
    preprocess_client_order(to_csv=save_csv, override=override or OVERRIDE_CLIENT_ORDER or OVERRIDE_ALL)
    preprocess_cockpit_table(to_csv=save_csv, override=override or OVERRIDE_COCKPIT_TABLES or OVERRIDE_ALL)


def preprocess_flow_db(to_csv=False, override=False) -> Optional[pd.DataFrame]:
    def extract_deal_rate(df_: pd.DataFrame, pair: str = 'EURUSD') -> pd.DataFrame:
        rates = df_[df_['sym'] == pair][['date', 'dealRate']]
        rates['weekOfYear'] = rates[time_column].dt.isocalendar().week
        rates['year'] = rates[time_column].dt.isocalendar().year
        rates = rates.drop(columns=['date'], errors='ignore')
        rates = rates.groupby(['weekOfYear', 'year']).mean().reset_index()

        return rates

    def aggregate(df_: pd.DataFrame, aggregation_: list) -> pd.DataFrame:
        """ aggregates by `aggregation` param and computes several volumes """

        df_ = df_.groupby(aggregation_).agg(
            FDtradesCount=('externalClientId', 'count'),
            FDrfsTrades=('streamTypeIsRfs', 'sum'),
            FDespTrades=('streamTypeIsEsp', 'sum'),
            # __ Deal types __
            # spotTrades=('dealTypeIsSpot', 'sum'),
            # fwdTrades=('dealTypeIsForward', 'sum'),
            # side
            FDisSell=('sideIsSell', 'sum'),
            FDisBuy=('sideIsBuy', 'sum'),
            # platforms
            FDEURAmount=('riskAmount', 'sum'),
            clientId=('clientId', 'first'),
            accountId=('accountId', 'first'),
            dealRate=('dealRate', 'mean'),
            M0=('markoutUSD_0', 'mean'),
            M1=('markoutUSD_1', 'mean'),
            M5=('markoutUSD_5', 'mean'),
            M30=('markoutUSD_30', 'mean'),
            M60=('markoutUSD_60', 'mean'),
            M120=('markoutUSD_120', 'mean'),
            M180=('markoutUSD_180', 'mean'),
            M300=('markoutUSD_300', 'mean'),
            M600=('markoutUSD_600', 'mean'),
            pnlAHUSD=('pnlAHUSD', 'mean'),
            pnlAdjUSD=('pnlAdjUSD', 'mean'),
            pnlInceptionUSD=('pnlInceptionUSD', 'mean'),  # TODO: failing to convert some entries to float
            pnlMatchUSD=('pnlMatchUSD', 'mean'),
            pnlTotalPerM=('pnlTotalPerM', 'mean'),
            pnlTotalUSD=('pnlTotalUSD', 'mean')
        ).reset_index()

        # TODO: pnlTotalPerM is the only per million column? --> Looks like it is

        return df_

    table_name = 'flowDB'
    time_column = 'date'
    aggregation = ['platform', 'externalClientId', 'sym', 'dealType', 'weekOfYear', 'year']

    # _____ 0 - OVERRIDE _____
    if not override and f"{table_name}.csv" in os.listdir('data/03_preprocessed'):
        logger.info(f'======== {table_name} preprocessing skipped. File already present ========\n')
        return None

    # _____ 1 - LOADING _____
    logger.info(f'======== start {table_name} pre-processing ========')

    plot_folder = f'plots/{table_name}'
    base_dir = 'data/02_cleaned'
    df = load(table=table_name, folder=base_dir, date_columns=['time', 'date', 'dealTime', 'tradeDate', 'valueDate'])

    # Drop copies of column date. Date is the only column we need as reference. ValueDate can be instead interesting as it contains the information about the tenor
    df = df.drop(columns=['time', 'dealTime', 'tradeDate'], errors='ignore')

    # Extract EURUSD rates for each weekOfYear and Year tuple
    eur_usd_rates = extract_deal_rate(df)

    # Drop columns which are not relevant once the dataFrame is aggregated (or because they are duplicated info)
    df = df.drop(columns=['clientAllInRate', 'clientSpotRate', 'externalId', 'externalTraderId', 'orderLegId', 'riskAllInRate',
                          'amount', 'dealAmount', 'riskSpotRate', 'spotDate', 'subDealType', 'ticketType', 'traderId'], errors='ignore')

    # assert not is_null(df)  # assert there's no value is null
    logger.debug(f'Total entries loaded from csv: {len(df)}')

    # ___ 2 - PRE-PROCESSING ___
    df = df.drop_duplicates(subset='dealID', keep="last", inplace=False)  # keep only one single record for each `dealID`
    logger.debug('Total entries after dealID filtering : {}'.format(len(df)))

    # extract week - will be used to join with windowed volumes
    df['weekOfYear'] = df[time_column].dt.isocalendar().week
    df['year'] = df[time_column].dt.isocalendar().year

    # OneHotEncode dataFrame
    df = one_hot_encode(df, cols=['streamType', 'side', 'tenor'], drop_columns=True, preserve_col_name=True)
    df = one_hot_encode(df, cols=['spotCentre', 'swapCentre'], drop_columns=True, preserve_col_name=True)

    # TODO: investigate if some of the removed columns are the same for aggregation - in that case we can use them as feature
    # Remove those columns not important during aggregation
    to_remove = ['date', 'externalAccountId', 'ecn', 'failreason', 'proxyFullName', 'proxyGroupFullName',
                 'side', 'spotCentre', 'streamType', 'swapCentre', 'tenor', 'valueDate']
    # `externalClientId`, `platform` and `week` must be kept since used in aggregation
    # `clientId` kept since will be used to join with `cockpit` data
    # 'accountId' kept since will be used to join with `cockpit` data
    df = df.drop(columns=to_remove, errors='ignore')

    # df['number'] = df['pnlTotalUSD'].apply(lambda x: is_number(x))

    # Exclude trades where riskAmount = 0, because it's impossible to calculate markout% --> inf
    df = df[df['riskAmount'] > 0]

    # TODO: aggregate in order to count the one hot encoded columns
    # aggregated_count = df.groupby(aggregation).agg(count=('externalClientId', 'count')).reset_index()
    # aggregated_account = df.groupby(aggregation).agg(accountId=('accountId', 'first')).reset_index()

    # Aggregate dataFrame by aggregation
    aggregated_df = aggregate(df, aggregation)
    aggregated_df = aggregated_df.rename(columns={"externalClientId": "client", "sym": "pair", "dealType": "productType"}, errors='ignore')

    # Add EURUSD rate for each entry:
    # aggregated_df['dealRate'] = aggregated_df[['weekOfYear', 'year']].apply(lambda x: filter_by_values(eur_usd_rates, {'weekOfYear': x[0], 'year': x[1]})['dealRate'].iloc[0], axis=1)
    # aggregated_df['USDAmount'] = aggregated_df['EURAmount'] * aggregated_df['dealRate']

    # _____ 3 - Save file _____
    logger.debug('Total entries before saving to csv: {}'.format(len(aggregated_df)))
    output_path = 'data/03_preprocessed'
    aggregated_df.to_csv(f'{output_path}/{table_name}.csv', index=False) if to_csv else None

    # _____ 4 - Plotting ______
    # plotting()

    logger.info(f'======== end {table_name} pre-processing ========\n')

    return aggregated_df


def preprocess_client_order(to_csv=False, override=False) -> Optional[pd.DataFrame]:
    def is_confirmed(order_state: str) -> int:
        """ return 1 if order_state is CONFIRMED, otherwise 0 """
        return 1 if order_state == 'CONFIRMED' else 0

    def is_rfs(stream_type: str) -> int:
        """ return 1 if stream_type is RFS, otherwise 0 (i.e., ESP) """
        return 1 if stream_type == 'RFS' else 0

    def of_specific_deal(deal_type: str, expected: str) -> int:
        """ return 1 if the deal_type is equal to the expected one, otherwise 0 """
        return 1 if deal_type == expected else 0

    def of_specific_platform(platform: str, expected: str) -> int:
        """ return 1 if the platform is equal to the expected one, otherwise 0 """
        return 1 if platform == expected else 0

    def enhance_client_order_information(df_: pd.DataFrame) -> pd.DataFrame:
        """ enhance clientOrder data information """

        # extract additional timing info
        df_ = extract_datetime_info(df_, [time_column])

        df_['weekOfYear'] = df_[time_column].dt.isocalendar().week
        df_['year'] = df_[time_column].dt.isocalendar().year

        # extract if a trade is CONFIRMED or not
        df_['isConfirmed'] = df_['orderState'].apply(lambda val: is_confirmed(val))

        # extract if a trade is an RFS or not
        df_['isRfs'] = df_['streamType'].apply(lambda val: is_rfs(val))

        # extract if a trade is of a specific deal_type - iterate over all of them
        # for deal in df_['dealType'].unique():
        #     col_name = to_camelcase(deal)
        #     df_[f'is{col_name}'] = df_['dealType'].apply(lambda v: of_specific_deal(v, deal))

        # extract if a trade comes from a specific platform - iterate over all of them
        # for platform in df_['platform'].unique():
        #     col_name = to_camelcase(platform)
        #     df_[f'is{col_name}'] = df_['platform'].apply(lambda val: of_specific_platform(val, platform))

        # extract if a trade has been execute on a specific week's day - iterate over all
        for day in all_days:
            col_name = to_camelcase(day)
            df_[f'is{col_name}'] = df_['dateDayOfWeek'].apply(lambda val: of_specific_day(val, day))

        # extract if a trade has been execute on a specific week's day - iterate over all
        for month in all_months:
            col_name = to_camelcase(month)
            df_[f'is{col_name}'] = df_['dateMonth'].apply(lambda val: of_specific_month(val, month))

        df_ = df_.reset_index(drop=True)

        return df_

    def aggregate(df_: pd.DataFrame, aggregation_: list) -> pd.DataFrame:
        """ aggregates by `aggregation` param and computes several volumes """

        df_ = df_.groupby(aggregation_).agg(
            CoTradesCount=('externalClientId', 'count'),
            CoConfirmedTrades=('isConfirmed', 'sum'),
            CoRfsTrades=('isRfs', 'sum'),
            CoEURAmount=('riskAmount', 'sum'),
            # __ deal types __
            # swapTrades=('isSwap', 'sum'),
            # spotTrades=('isSpot', 'sum'),
            # fwdTrades=('isForward', 'sum'),
            # blockTrades=('isBlock', 'sum'),
            # takeUpTrades=('isTakeUp', 'sum'),
            # timeOptionTrades=('isTimeOption', 'sum'),
            # ndfTrades=('isNdf', 'sum'),
            # __ week's days __
            onMonday=('isMonday', 'sum'),
            onTuesday=('isTuesday', 'sum'),
            onWednesday=('isWednesday', 'sum'),
            onThursday=('isThursday', 'sum'),
            onFriday=('isFriday', 'sum'),
            onSaturday=('isSaturday', 'sum'),
            onSunday=('isSunday', 'sum'),
            # __ months __
            onJanuary=('isJanuary', 'sum'),
            onFebruary=('isFebruary', 'sum'),
            onMarch=('isMarch', 'sum'),
            onApril=('isApril', 'sum'),
            onMay=('isMay', 'sum'),
            onJune=('isJune', 'sum'),
            onJuly=('isJuly', 'sum'),
            onAugust=('isAugust', 'sum'),
            onSeptember=('isSeptember', 'sum'),
            onOctober=('isOctober', 'sum'),
            onNovember=('isNovember', 'sum'),
            onDecember=('isDecember', 'sum'),
            # __ platforms __
            # onUct=('isUct', 'sum'),
            # on360t=('is360t', 'sum'),
            # onUfx=('isUfx', 'sum'),
            # onFxall=('isFxall', 'sum'),
            # onBloomberg=('isBloomberg', 'sum'),
            # onTobo=('isTobo', 'sum'),
            # onFastmatch=('isFastmatch', 'sum'),
        ).reset_index()

        return df_

    empty = 'EMPTY'
    all_days = list(map(lambda month: month.upper(), calendar.day_name))
    all_months = list(map(lambda month: month.upper(), calendar.month_name[1:]))
    aggregation = ['platform', 'externalClientId', 'sym', 'dealType', 'weekOfYear', 'year']
    time_column = 'date'

    table_name = 'clientOrder'

    # _____ 0 - OVERRIDE _____
    if not override and f"{table_name}.csv" in os.listdir('data/03_preprocessed'):
        logger.info(f'======== {table_name} preprocessing skipped. File already present ========\n')
        return None

    # _____ 1 - LOADING _____
    logger.info(f'======== start {table_name} pre-processing ========')

    plot_folder = f'plots/{table_name}'
    base_dir = 'data/02_cleaned'
    df = load(table=table_name, folder=base_dir, date_columns=['date'])
    df = df.drop(columns=['tradeDate', 'time'], errors='ignore')
    logger.debug(f'Total entries loaded from csv: {len(df)}')

    # ___ 2 - PRE-PROCESSING ___

    #   __ Some preconditions checks __
    order_cnt = df.groupby(['id']).size()
    assert len(order_cnt[order_cnt > 1]) == 0  # ensure we have one record per order - last valid state
    assert not is_null(df)  # assert dataset does not contain any null value - among all columns
    assert len(df[df['externalClientId'] == empty]['id']) == 0  # ensure `externalClientId` does not contain EMPTY records
    assert len(df[df['sym'] == empty]['id']) == 0  # ensure `sym` does not contain EMPTY records

    enhanced_df = enhance_client_order_information(df)  # Compute order volumes - wrt specific time window
    # weeks = list(enhanced_client_order['week'].unique())

    assert not is_null(enhanced_df)  # assert dataset does not contain any null value - among all columns

    # we'll aggregate per client, platform and week - this because a specific client can behave differently according to the platform in which it operates and the week
    volumes_df = aggregate(enhanced_df, aggregation)
    volumes_df['hitRatio'] = volumes_df['CoConfirmedTrades'] / volumes_df['CoTradesCount']
    volumes_df = volumes_df.rename(columns={"externalClientId": "client", "sym": "pair", "dealType": "productType"}, errors='ignore')

    to_convert = ['confirmedTrades', 'rfsTrades', 'swapTrades', 'spotTrades', 'fwdTrades',
                  'blockTrades', 'takeUpTrades', 'timeOptionTrades', 'ndfTrades',
                  'onMonday', 'onTuesday', 'onWednesday', 'onThursday', 'onFriday',
                  'onSaturday', 'onSunday', 'onJanuary', 'onFebruary', 'onMarch',
                  'onApril', 'onMay', 'onJune', 'onJuly', 'onAugust', 'onSeptember',
                  'onOctober', 'onNovember', 'onDecember', 'onUct', 'on360t', 'onUfx',
                  'onFxall', 'onBloomberg', 'onTobo', 'onFastmatch']

    # percentage_volumes = columns_to_percentage(volumes_df, 'tradesCount', to_convert=to_convert)

    # _____ 3 - SAVE FILE _____
    logger.debug(f'Total entries before saving to csv: {len(volumes_df)}')
    output_path = 'data/03_preprocessed'
    volumes_df.to_csv(f'{output_path}/{table_name}.csv', index=False) if to_csv else None

    # _____ 4 - Plotting ______
    # plotting()

    logger.info(f'======== end {table_name} pre-processing ========\n')

    return volumes_df


def preprocess_cockpit_table(to_csv=False, override=False):
    # _____ 1 - Exploration ______
    folder = 'data/01_raw'
    cockpit_outputh_path = 'data/03_preprocessed'

    cockpit_client_table = 'cockpitClientMetadata'
    cockpit_account_table = 'cockpitAccountMetadata'

    # _____ 0 - Override _____
    if not override and f"{cockpit_client_table}.csv" in os.listdir(
            'data/03_preprocessed') and f"{cockpit_account_table}.csv" in os.listdir('data/03_preprocessed'):
        logger.info(f'======== cockpit preprocessing skipped. File already present ========\n')
        return None

    # _____ 1 - Exploration ______
    folder = 'data/02_cleaned'

    fd_date_columns = ['time']

    logger.info(f'======== start cockpit client pre-processing ========')

    cockpit_client = load(
        folder=folder,
        table=cockpit_client_table,
        date_columns=fd_date_columns,
        encoding='ISO-8859-1'
    )

    logger.debug(f'Total entries loaded from cockpit client csv: {len(cockpit_client)}')

    print_null_counting(cockpit_client)

    assert not is_null(cockpit_client)

    # drop time column - since useless for this static data information
    cockpit_client = cockpit_client.drop(columns=['time'], errors='ignore')

    fd_date_columns = ['time']

    cockpit_account = load(
        folder=folder,
        table=cockpit_account_table,
        date_columns=fd_date_columns,
        encoding='ISO-8859-1'
    )

    logger.debug(f'Total entries loaded from cockpit account csv: {len(cockpit_account)}')

    print_null_counting(cockpit_account)

    assert not is_null(cockpit_account)

    cockpit_account = cockpit_account.drop(columns=['time', 'deleteFlag', 'IeAccountId'], errors='ignore')

    # _____ 2 - Save file _____
    output_path = 'data/03_preprocessed'

    logger.debug(f'Total entries before saving cockpit client to csv: {len(cockpit_client)}')
    cockpit_client.to_csv(f'{output_path}/{cockpit_client_table}.csv', index=False) if to_csv else None

    logger.debug(f'Total entries before saving cockpit account to csv: {len(cockpit_account)}')
    cockpit_account.to_csv(f'{output_path}/{cockpit_account_table}.csv', index=False) if to_csv else None

    logger.info(f'======== end cockpit client pre-processing ========\n')


if __name__ == '__main__':
    preprocess_360t_tables(override=False)
    preprocess_exceed_tables(override=False)