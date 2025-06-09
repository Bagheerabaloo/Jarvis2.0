import yfinance as yf
import pandas as pd
from sqlalchemy import func, desc
from sqlalchemy.orm import aliased
from sqlalchemy import text

from src.common.tools.library import *
from src.stock.src.db.database import session_local
from src.stock.src.db.models import CandleAnalysisCandlestickDay, CandleDataDay, Ticker, InfoTradingSession, InfoMarketAndFinancialMetrics
from stock.src.CandleAnalysisService import CandleAnalysisService
from stock.src.CandleDataInterval import CandleDataInterval
# from stock.stock_main import get_all_not_updated_analysis_symbols_from_db
from stock.src.indexes.sp500.sp500Handler import SP500Handler


def update_analysis(_session):
    symbols = get_all_not_updated_analysis_symbols_from_db(_session)

    print(f"There are {len(symbols)} tickers to analyze")

    initial_time = time()
    for symbol in symbols:
        start_time = time()
        print(f"{symbol} - Start analyzing candlestick data with interval {CandleDataInterval.DAY.value}...")
        analysis = CandleAnalysisService(session=_session, symbol=symbol, interval=CandleDataInterval.DAY)
        candle_data_ = analysis.analyze()
        analysis.handle_candle_analysis_data(candle_data_)
        print(f"        - Candlestick data analysis data handled in {time() - start_time:.2f} seconds")

    end_time = time()
    total_time = seconds_to_time(end_time - initial_time)
    print(f"{'Total elapsed time:'.ljust(25)} {total_time['hours']} hours {total_time['minutes']} min {total_time['seconds']} sec")
    print(f"{'Total tickers:'.ljust(25)} {len(symbols)}")
    print(f"{'Average time per ticker:'.ljust(25)} {round((end_time - initial_time) / len(symbols), 3)} sec")


def analysis_1():
    #  __ subquery to find the latest available date for each ticker
    latest_date_subquery = (
        session.query(
            CandleDataDay.ticker_id,  # Select ticker_id
            func.max(CandleDataDay.date).label('latest_date')  # Find the maximum date for each ticker_id
        )
        .group_by(CandleDataDay.ticker_id)  # Group results by ticker_id
    ).subquery()

    # __ aliasing CandleDataDay for clearer joins
    candle_data_alias = aliased(CandleDataDay)

    # __ main query to join tables and get the required details
    query = (
        session.query(
            CandleAnalysisCandlestickDay,  # Select all columns from CandleAnalysisDay
            Ticker.symbol,  # Select the symbol from Ticker
            candle_data_alias.date  # Select the date from the CandleDataDay alias
        )
        .join(candle_data_alias, CandleAnalysisCandlestickDay.candle_data_day_id == candle_data_alias.id)  # Join with CandleDataDay on candle_data_day_id
        .join(Ticker, candle_data_alias.ticker_id == Ticker.id)  # Join with Ticker on ticker_id
        .join(latest_date_subquery, (candle_data_alias.ticker_id == latest_date_subquery.c.ticker_id) &
              (candle_data_alias.date == latest_date_subquery.c.latest_date))  # Join with subquery to get the latest date for each ticker
    )

    # __ execute the query and fetch all results
    results = query.all()

    # __ convert the query results to a list of dictionaries
    results_as_dicts = [
        {**result[0].__dict__, 'symbol': result[1], 'latest_date': result[2]} for result in results
    ]

    # Remove the '_sa_instance_state' from each dictionary
    for record in results_as_dicts:
        record.pop('_sa_instance_state', None)

    # Create a DataFrame from the list of dictionaries
    df = pd.DataFrame(results_as_dicts)

    # Display the DataFrame
    print(df)


def build_sp_500_value(_session):
    from stock.src.indexes.sp500.sp500Handler import SP500Handler

    sp500_handler = SP500Handler()
    symbols = sp500_handler.get_sp500_from_wikipedia()

    results = get_latest_market_caps_from_db(_session)
    # Convert the results to a pandas DataFrame
    df = pd.DataFrame(results, columns=['Ticker', 'Market Cap', 'last_update'])

    filtered_df = df[df['Ticker'].isin(symbols)]

    assert len(filtered_df) == len(symbols), "The number of symbols in the DataFrame does not match the number of symbols in the S&P 500 list"

    # __ add a column specifying the weight of the single ticker __
    filtered_df['Weight'] = filtered_df['Market Cap'] / filtered_df['Market Cap'].sum()

    print('end')


def get_last_market_cap_for_ticker_from_db(_session, _symbol):
    result = (_session.query(InfoTradingSession.market_cap, Ticker.symbol).
              join(Ticker, InfoTradingSession.ticker_id == Ticker.id).
              filter(Ticker.symbol == _symbol)
              .order_by(desc(InfoTradingSession.last_update))
              .first())

    return result[0] if result else None


def get_latest_market_caps_from_db(_session):
    # Get the most recent market_cap, last_update for each ticker
    subquery = _session.query(
        InfoTradingSession.ticker_id,
        func.max(InfoTradingSession.last_update).label('latest_update')
    ).group_by(InfoTradingSession.ticker_id).subquery()

    results = (_session.query(
        Ticker.symbol,
        InfoTradingSession.market_cap,
        InfoTradingSession.last_update
    ).join(
        subquery,
        (InfoTradingSession.ticker_id == subquery.c.ticker_id) &
        (InfoTradingSession.last_update == subquery.c.latest_update)
    ).join(Ticker, InfoTradingSession.ticker_id == Ticker.id)
               .order_by(desc(InfoTradingSession.market_cap))
               .all())

    return results


def get_outstanding_shares_history_for_ticker_from_db(_session, _symbol):
    results = (_session.query(Ticker.symbol, InfoMarketAndFinancialMetrics.shares_outstanding, InfoMarketAndFinancialMetrics.last_update).
               join(Ticker, InfoMarketAndFinancialMetrics.ticker_id == Ticker.id).
               filter(Ticker.symbol == _symbol)
               .order_by(desc(InfoMarketAndFinancialMetrics.last_update))
               .all())

    return pd.DataFrame(results, columns=['Ticker', 'Shares Outstanding', 'last_update']) if results else None


def get_sp500_stocks_above_200_ma(_session) -> pd.DataFrame:
    query = text(f"""
    SELECT
        C.close,  -- Close price from CandleDataDay
        I.ma200,  -- 200-day moving average from CandleAnalysisIndicatorsDay
        T.symbol,  -- Symbol from Ticker
        C.date  -- Date from CandleDataDay
    FROM
        candle_data_day C
    JOIN
        candle_analysis_indicators_day I ON I.candle_data_day_id = C.id
    JOIN
        ticker T ON C.ticker_id = T.id
    WHERE
        EXISTS (
            SELECT 1
            FROM candle_data_day sub_C
            WHERE sub_C.ticker_id = C.ticker_id
            AND sub_C.date = C.date
            AND sub_C.date = (
                SELECT MAX(latest_sub.date)
                FROM candle_data_day latest_sub
                WHERE latest_sub.ticker_id = C.ticker_id
            )
        )
    ORDER BY T.symbol
    ;""")

    # Execute the query through the session
    results = session.execute(query).fetchall()

    # __ convert the query results to a list of dictionaries
    results_as_dicts = [
        {'close': result[0],
         'ma200': result[1],
         'symbol': result[2],
         'date': result[3]}
        for result in results
    ]

    # Create a DataFrame from the list of dictionaries
    df = pd.DataFrame(results_as_dicts)
    df['above_200_ma'] = df['close'] > df['ma200']
    df['percentage_above_200_ma'] = (df['close'] - df['ma200']) / df['ma200'] * 100

    # __ get the S&P 500 symbols __
    sp500_handler = SP500Handler()
    symbols = sp500_handler.get_sp500_from_wikipedia()

    # __ select only the S&P 500 stocks __
    filtered_df = df[df['symbol'].isin(symbols)]

    # __ print percentage of stocks above 200 ma __
    print(f"Percentage of stocks above 200-day moving average: {filtered_df['above_200_ma'].mean() * 100:.2f}%")

    return filtered_df


def get_sp500_historical_percentage_above_200_ma(_session) -> pd.DataFrame:
    query = text(f"""
    SELECT T.symbol,
            C.date,
            C.close,
            I.ma200
    FROM ticker T
    JOIN candle_data_day C ON T.id = C.ticker_id
    JOIN candle_analysis_indicators_day I ON C.id = I.candle_data_day_id
    WHERE C.date >= '2020-01-01' AND C.date <= '2024-09-29'
    ;
    """)

    # Execute the query through the session
    results = session.execute(query).fetchall()

    # __ convert the query results to a list of dictionaries
    results_as_dicts = [
        {'symbol': result[0],
         'date': result[1],
         'close': result[2],
         'ma200': result[3],
         }
        for result in results
    ]

    # Create a DataFrame from the list of dictionaries
    df = pd.DataFrame(results_as_dicts)
    df['above_200_ma'] = df['close'] > df['ma200']
    df['percentage_above_200_ma'] = (df['close'] - df['ma200']) / df['ma200'] * 100

    # __ get the S&P 500 symbols __
    sp500_handler = SP500Handler()
    symbols = sp500_handler.get_sp500_from_wikipedia()

    # __ select only the S&P 500 stocks __
    filtered_df = df[df['symbol'].isin(symbols)]

    grouped_df = (filtered_df
                  .groupby('date')
                  .agg(
        total_symbols=('symbol', 'count'),
        percentage_above_200_ma=('above_200_ma', lambda x: (x.sum() / len(x)) * 100)
    ))

    import matplotlib.pyplot as plt

    plt.figure(figsize=(10, 6))
    plt.plot(grouped_df.index, grouped_df['percentage_above_200_ma'], marker='o', linestyle='-', color='b')

    plt.title('Percentage of Symbols Above 200 MA Over Time', fontsize=14)
    plt.xlabel('Date', fontsize=12)
    plt.ylabel('Percentage Above 200 MA (%)', fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.grid(True)
    plt.show()

    return grouped_df


def get_sp500_worst_performing_stocks(_session) -> pd.DataFrame:
    query = text(f"""
    SELECT T.symbol,
            C.date,
            C.close,
            C.open
    FROM ticker T
    JOIN candle_data_day C ON T.id = C.ticker_id
    WHERE C.date >= '2020-01-01' AND C.date <= '2024-09-29'
    AND (C.close - C.open) / C.open < - 0.1
    ;
    """)

    # Execute the query through the session
    results = session.execute(query).fetchall()

    # __ convert the query results to a list of dictionaries
    results_as_dicts = [
        {'symbol': result[0],
         'date': result[1],
         'close': result[2],
         'open': result[3],
         }
        for result in results
    ]

    # Create a DataFrame from the list of dictionaries
    df = pd.DataFrame(results_as_dicts)
    df['change'] = (df['close'] - df['open']) / df['open'] * 100

    # __ get the S&P 500 symbols __
    sp500_handler = SP500Handler()
    symbols = sp500_handler.get_sp500_from_wikipedia()

    # __ select only the S&P 500 stocks __
    filtered_df = df[df['symbol'].isin(symbols)]

    return filtered_df


def get_daily_gainers_losers(_session):
    import asyncio
    from src.common.telegram_manager.TelegramBot import TelegramBot
    from src.common.file_manager.FileManager import FileManager

    # __ get the S&P 500 symbols __
    sp500_handler = SP500Handler()
    tickers = sp500_handler.get_sp500_from_wikipedia()

    data = yf.download(tickers, period="5d", interval="1d")["Adj Close"]
    daily_returns = data.pct_change().iloc[-1].reset_index()
    daily_returns.rename(columns={daily_returns.columns[1]: 'Daily Return'}, inplace=True)
    sorted_gainers = daily_returns.sort_values(by=["Daily Return"], ascending=False)
    sorted_gainers["Daily Return"] = sorted_gainers["Daily Return"] * 100
    print("Top 5 daily gainers:")
    print(sorted_gainers.head(5))
    print("\nTop 5 daily losers:")
    print(sorted_gainers.tail(5))

    telegram_token_key = "TELEGRAM_TOKEN"
    config_manager = FileManager()
    token = config_manager.get_telegram_token(database_key=telegram_token_key)
    admin_info = config_manager.get_admin()
    telegram_bot = TelegramBot(token=token)

    text1 = "Top 5 daily gainers:\n" + '\n'.join([f"{row['Ticker']}:    {round(row['Daily Return'],2)}%" for index, row in sorted_gainers.head(5).iterrows()])
    text2 = "Top 5 daily losers:\n" + '\n'.join([f"{row['Ticker']}:    {round(row['Daily Return'],2)}%" for index, row in sorted_gainers.tail(5).sort_values(by=["Daily Return"]).iterrows()])
    asyncio.run(telegram_bot.send_message(chat_id=admin_info["chat"], text=f"{text1}\n\n{text2}"))


def get_forward_pe_history_for_ticker_from_db(_session, _symbol):
    results = (_session.query(Ticker.symbol, InfoTradingSession.trailing_pe, InfoTradingSession.forward_pe, InfoTradingSession.last_update).
               join(Ticker, InfoTradingSession.ticker_id == Ticker.id).
               filter(Ticker.symbol == _symbol)
               .order_by(desc(InfoTradingSession.last_update))
               .all())

    df =  pd.DataFrame(results, columns=['Ticker', 'Trailing PE', 'Forward PE', 'last_update']) if results else None

    # Step 1: Convert 'last_update' to datetime and extract only the date
    df['last_update'] = pd.to_datetime(df['last_update'])
    df['date'] = df['last_update'].dt.date

    # Step 2: Keep the most recent entry per day
    df = df.sort_values(by='last_update', ascending=False).drop_duplicates(subset='date', keep='first')

    # Step 3: Drop the 'last_update' column
    df = df.drop(columns=['last_update'])

    # Step 4: Sort by date for plotting
    df = df.sort_values(by='date')

    # Step 5: Plotting
    import matplotlib.pyplot as plt
    plt.figure(figsize=(12, 6))
    plt.plot(df['date'], df['Trailing PE'], linestyle='-', marker='o', label='Trailing PE')
    plt.plot(df['date'], df['Forward PE'], linestyle='-', marker='o', label='Forward PE')
    plt.xlabel('Date')
    plt.ylabel('PE Ratio')
    plt.title('Trailing vs Forward PE for TSLA')
    plt.legend()
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    return df


if __name__ == "__main__":
    # __ sqlAlchemy __ create new session
    session = session_local()

    # __ update analysis __
    # update_analysis(_session=session)

    # __ get the historical percentage of S&P 500 stocks above 200 ma __
    # df = get_sp500_historical_percentage_above_200_ma(_session=session)

    # __ get sp500 stocks above 200 ma __
    # df = get_sp500_stocks_above_200_ma(_session=session)

    # __ get worst performing stocks __
    # df = get_sp500_worst_performing_stocks(_session=session)

    # __ get the outstanding shares history for a specific ticker __
    # df = get_outstanding_shares_history_for_ticker_from_db(_session=session, _symbol="AMZN")

    # __ get the daily gainers and losers for the S&P 500 stocks __
    # df = get_daily_gainers_losers(_session=session)

    # __analysis 2 __
    # build_sp_500_value(_session=session)

    # __ get the forward pe history for a specific ticker __
    df = get_forward_pe_history_for_ticker_from_db(_session=session, _symbol="AMZN")

    print('end')






