import pandas as pd
import yfinance as yf
from time import sleep

import matplotlib
from mplfinance.original_flavor import candlestick_ohlc
from src.common.tools.library import *
import matplotlib.dates as mpl_dates
import matplotlib.pyplot as plt
import mplfinance as mpf
from mplfinance.original_flavor import candlestick_ohlc
from matplotlib.animation import FuncAnimation

matplotlib.use('TkAgg')


def trend_analysis():

    ticker = "NFLX"
    candles = yf.download(tickers=ticker, interval="1d", period='max')

    # _____ Convert Date _____
    candles['Date'] = pd.to_datetime(candles.index)
    candles['Timestamp'] = candles['Date'].apply(mpl_dates.date2num)
    candles['Ticker'] = 'NFLX'
    candles.reset_index(drop=True, inplace=True)

    candles = add_all_time_high(candles)

    candles = add_trend(candles, sens_up=20, sens_down=17)
    # candles = add_intervals_to_candles(candles=candles)
    # candles = add_candlestick_patterns(candles)

    candles = candles[['Open', 'High', 'Low', 'Close', 'Date', 'Trend']]

    plot_candles_with_trend_lines_animated(candles)

    # candles_2 = candles[['Date', 'currMax', 'currMin', 'DownFromHigh', 'UpFromLow', 'Trend', 'reversing', 'spinningTop', 'umbrellaLine', 'hammer', 'ATR%', '%lowerShadow']]

    # candles['Date'] = pd.to_datetime(candles.index)
    # candles['Date'] = candles['Date'].apply(mpl_dates.date2num)
    # candles = candles.astype(float)
    # candles = candles[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]

    # plot_candles(candles, intervals)


def add_all_time_high(candles):

    # __ sort the DataFrame by 'Date' in ascending order to ensure chronological order __
    candles.sort_values(by='Date', inplace=True)

    # __ calculate the all-time high for each row __
    candles['AllTimeHigh'] = candles['High'].cummax()

    # __ calculate the percentage decline from the all-time high __
    candles['DownFromAllTimeHigh%'] = (candles['AllTimeHigh'] - candles['Close']) / candles['AllTimeHigh'] * 100

    # __ copy the 'Date' column to use for grouping and merging later __
    candles['timestamp'] = candles['Date'].copy()

    # __ reset index ith drop=True to avoid adding the old index as a column __
    candles.reset_index(drop=True, inplace=True)

    # __ add a session index to represent the trading sessions __
    candles['SessionIndex'] = candles.index

    # __ group by AllTimeHigh and find the first occurrence of each high, including the session index __
    first_all_time_high = candles.groupby('AllTimeHigh')[['timestamp', 'SessionIndex']].first().reset_index()

    # __ merge the first occurrence data with the original DataFrame
    candles = candles.merge(first_all_time_high, on='AllTimeHigh', suffixes=('', '_first'))

    # __ rename the merged timestamp column to indicate it is the date of the all-time high __
    candles.rename(columns={'timestamp_first': 'DateAllTimeHigh'}, inplace=True)

    # __ remove temporary columns that are no longer needed __
    del candles['timestamp']
    del candles['Timestamp']

    # __ calculate the number of days since the all-time high for each row __
    candles['DaysFromAllTimeHigh'] = (candles['Date'] - candles['DateAllTimeHigh']).dt.days

    # __ calculate the number of trading sessions since the all-time high __
    candles['SessionsFromAllTimeHigh'] = candles['SessionIndex'] - candles['SessionIndex_first']

    # __ calculate the slope of the decline from the all-time high (days) __
    candles['SlopeDays'] = candles['DownFromAllTimeHigh%'] / np.sqrt(candles['DaysFromAllTimeHigh'])

    # __ calculate the slope of the decline from the all-time high (sessions) __
    candles['SlopeSessions'] = candles['DownFromAllTimeHigh%'] / np.sqrt(candles['SessionsFromAllTimeHigh'])

    return candles


def add_trend(candles, sens_up=10, sens_down=9):

    curr_min = candles['Low'].iloc[0]
    curr_max = candles['High'].iloc[0]
    all_time_high = candles['High'].iloc[0]
    all_time_high_index = 0
    trend = ''

    for index, candle in candles.iterrows():

        up_from_low = (candle['High'] - curr_min) / curr_min * 100
        down_from_high = (curr_max - candle['Low']) / curr_max * 100

        if up_from_low >= sens_up and trend != 'up':
            trend = 'up'
            curr_max = candle['High']
        elif down_from_high >= sens_down and trend != 'down':
            trend = 'down'
            curr_min = candle['Low']

        if candle['High'] > curr_max:
            curr_max = candle['High']

        if candle['High'] > all_time_high:
            all_time_high = candle['High']
            all_time_high_index = index

        if candle['Low'] < curr_min:
            curr_min = candle['Low']

        down_from_high = (curr_max - candle['Close']) / curr_max * 100
        up_from_low = (candle['Close'] - curr_min) / curr_min * 100
        down_from_all_time_high = (all_time_high - candle['Close']) / all_time_high * 100
        days_from_all_time_high = index - all_time_high_index

        candles.at[index, 'TrendAllTimeHigh'] = all_time_high
        candles.at[index, 'TrendDownFromAllTimeHigh'] = down_from_all_time_high
        candles.at[index, 'TrendDaysFromAllTimeHigh'] = days_from_all_time_high
        candles.at[index, 'currMax'] = curr_max
        candles.at[index, 'currMin'] = curr_min
        candles.at[index, 'DownFromHigh'] = down_from_high
        candles.at[index, 'UpFromLow'] = up_from_low
        candles.at[index, 'Trend'] = trend

        if trend == 'up' and down_from_high > up_from_low:
            candles.at[index, 'reversing'] = True
        elif trend == 'down' and up_from_low > down_from_high:
            candles.at[index, 'reversing'] = True
        else:
            candles.at[index, 'reversing'] = False

    return candles


def plot_candles_with_trend_lines(candles):
    # Convert the 'Date' column to datetime if it's not already
    candles['Date'] = pd.to_datetime(candles['Date'])

    # Convert dates to matplotlib's float format
    candles['Date'] = candles['Date'].apply(mpl_dates.date2num)

    # Sort the data by date
    candles = candles.sort_values('Date')

    # Check the data range and reduce if necessary for better visualization
    if len(candles) > 500:
        candles = candles.iloc[-500:]  # Show only the last 100 rows for better visualization

    # Identify the points where the trend changes
    intervals = []
    for i in range(1, len(candles)):
        if candles['Trend'].iloc[i] != candles['Trend'].iloc[i - 1]:
            trend = candles['Trend'].iloc[i]
            intervals.append({'index_stop': i, 'trend': trend})

    # Creating Subplots
    fig, ax = plt.subplots(figsize=(14, 7))

    # Plotting the candlestick chart
    candlestick_ohlc(ax, candles[['Date', 'Open', 'High', 'Low', 'Close']].values, width=0.6, colorup='green', colordown='red', alpha=0.8)

    # Adding vertical lines where the trend changes
    for interval in intervals:
        ax.axvline(x=candles.iloc[interval['index_stop']]['Date'], linestyle='--',
                   color='green' if interval['trend'] == 'up' else 'red', alpha=0.5, linewidth=1)

    # Formatting Date
    ax.xaxis.set_major_formatter(mpl_dates.DateFormatter('%d-%m-%Y'))
    ax.set_xlabel('Date')
    ax.set_ylabel('Price')
    fig.suptitle('Candlestick Chart with Trend Change Lines')
    fig.autofmt_xdate()
    fig.tight_layout()
    ax.grid()
    plt.show()


def plot_candles_with_trend_lines_animated(candles):
    # Remove rows with empty values in the 'Trend' column
    candles = candles[candles['Trend'] != '']

    # Convert the 'Date' column to datetime if it's not already
    if not np.issubdtype(candles['Date'].dtype, np.datetime64):
        candles['Date'] = pd.to_datetime(candles['Date'], errors='coerce')

    # Convert dates to matplotlib's float format
    candles['Date'] = candles['Date'].apply(mpl_dates.date2num)

    # Ensure numeric columns are float and handle conversion errors
    for col in ['Open', 'High', 'Low', 'Close']:
        candles[col] = pd.to_numeric(candles[col], errors='coerce')

    # Drop rows with NaN values that couldn't be converted
    candles = candles.dropna()

    # Sort the data by date
    candles = candles.sort_values('Date')

    # Identify the points where the trend changes
    intervals = []
    for i in range(1, len(candles)):
        if candles['Trend'].iloc[i] != candles['Trend'].iloc[i - 1]:
            trend = candles['Trend'].iloc[i]
            intervals.append({'index_stop': i, 'trend': trend})

    # Creating Subplots
    fig, ax = plt.subplots(figsize=(14, 7))
    plt.ion()  # Enable interactive mode
    plt.show(block=False)  # Show the plot without blocking the execution

    for i in range(1, len(candles)):
        ax.clear()
        ax.xaxis.set_major_formatter(mpl_dates.DateFormatter('%d-%m-%Y'))
        ax.set_xlabel('Date')
        ax.set_ylabel('Price')
        fig.suptitle('Candlestick Chart with Trend Change Lines')
        fig.autofmt_xdate()
        ax.grid()

        # Plot up to the current candle
        candlestick_ohlc(ax, candles.iloc[:i+1][['Date', 'Open', 'High', 'Low', 'Close']].values, width=0.6, colorup='green', colordown='red', alpha=0.8)

        # Add vertical lines where the trend changes up to the current candle
        for interval in intervals:
            if interval['index_stop'] <= i:
                ax.axvline(x=candles.iloc[interval['index_stop']]['Date'], linestyle='--',
                           color='green' if interval['trend'] == 'up' else 'red', alpha=0.5, linewidth=1)

        fig.canvas.draw()  # Draw the canvas to ensure it updates
        plt.pause(0.1)  # A shorter pause to allow the plot to update

        # Pause for 2 seconds if the current candle is a trend change point
        if i in [interval['index_stop'] for interval in intervals]:
            sleep(2)

    plt.ioff()
    plt.show()  # Keep the plot open after the loop


def all_time_high_analysis(override=False):

    tickers = sp500_list()
    counter = 0
    total = len(tickers)

    # tickers = {key: value for (key, value) in list(tickers.items())[400:]}

    files = os.listdir('C:/Users/Vale/PycharmProjects/Jarvis/data/')

    for ticker in tickers:
        counter += 1
        if f"{ticker}.csv" in files and not override:
            print(f"{counter}/{total} - Skipping {ticker}")
            continue
        candles = get_candles_for_ticker(ticker, tickers)

        candles.to_csv('C:/Users/Vale/PycharmProjects/Jarvis/data/{}.csv'.format(ticker))
        print(f"{counter}/{total} - Writing {ticker}")
        sleep(2)

    final = pd.DataFrame(columns=['Ticker', 'AllTimeHigh', 'DownFromAllTimeHigh'])

    counter = 0
    for ticker in tickers:
        counter += 1
        data = pd.read_csv('C:/Users/Vale/PycharmProjects/Jarvis/data/{}.csv'.format(ticker))
        if len(data) > 0:
            financial_data = si.get_quote_table(ticker)
            financial_data = financial_data if financial_data else {}
            final = final.append({**{'Ticker': ticker,
                                     'AllTimeHigh': data.iloc[-1]['AllTimeHigh'],
                                     'DownFromAllTimeHigh': data.iloc[-1]['DownFromAllTimeHigh']}, **financial_data}, ignore_index=True)

            print(f"{counter}/{total} - Getting financial data {ticker}")
            sleep(0.5)

    final.to_csv('C:/Users/Vale/PycharmProjects/Jarvis/data/final.csv')

    print('end')


def all_time_high_nasdaq(override=False):

    tickers = pd.read_csv('C:/Users/Vale/PycharmProjects/Jarvis/data/NASDAQ_list.csv')['Symbol'].values.tolist()
    counter = 0
    total = len(tickers)

    # tickers = {key: value for (key, value) in list(tickers.items())[400:]}

    files = os.listdir('C:/Users/Vale/PycharmProjects/Jarvis/data/')

    for ticker in tickers:
        counter += 1
        if f"{ticker}.csv" in files and not override:
            print(f"{counter}/{total} - Skipping {ticker}")
            continue
        candles = get_candles_for_ticker_v2(ticker, tickers)

        candles.to_csv('C:/Users/Vale/PycharmProjects/Jarvis/data/{}.csv'.format(ticker))
        print(f"{counter}/{total} - Writing {ticker}")
        sleep(2)

    if override:
        final = pd.DataFrame(columns=['Ticker', 'AllTimeHigh', 'DownFromAllTimeHigh'])

        counter = 0
        for ticker in tickers:
            counter += 1
            data = pd.read_csv('C:/Users/Vale/PycharmProjects/Jarvis/data/{}.csv'.format(ticker))
            if len(data) > 0:
                try:
                    financial_data = si.get_quote_table(ticker)
                except:
                    print(f"{counter}/{total} - No financial data for {ticker} - Skipping")
                    continue
                financial_data = financial_data if financial_data else {}
                final = final.append({**{'Ticker': ticker,
                                         'AllTimeHigh': data.iloc[-1]['AllTimeHigh'],
                                         'DownFromAllTimeHigh': data.iloc[-1]['DownFromAllTimeHigh']}, **financial_data}, ignore_index=True)

                print(f"{counter}/{total} - Getting financial data {ticker}")
                sleep(0.5)

        final.to_csv('C:/Users/Vale/PycharmProjects/Jarvis/data/final.csv')
    else:
        final = pd.read_csv('C:/Users/Vale/PycharmProjects/Jarvis/data/final.csv')

    final[['52WeeksDown', '52WeeksUp']] = final['52 Week Range'].str.split(' - ', 1, expand=True)
    final['52WeeksDown'] = final['52WeeksDown'].str.replace(',', '').astype(float)
    final['52WeeksUp'] = final['52WeeksUp'].str.replace(',', '').astype(float)
    final['52Week%Down'] = (1 - final['52WeeksDown'] / final['52WeeksUp']) * 100
    final['52Week%Current'] = (1 - final['Quote Price'] / final['52WeeksUp']) * 100
    final['52WeekDiff'] = final['52Week%Down'] - final['52Week%Current']
    final['52%Up'] = (final['Quote Price'] - final['52WeeksDown']) / final['52WeeksUp'] * 100

    def market_cap_split(x):
        if type(x) != str:
            return np.nan
        elif 'M' in x:
            return float(x.split('M')[0])
        elif 'B' in x:
            return float(x.split('B')[0]) * 1000
        elif 'T' in x:
            return float(x.split('T')[0]) * 1000000
        else:
            print(x)
            return 0

    final['Market Cap (M)'] = final['Market Cap'].apply(lambda x: market_cap_split(x))
    final['PE Ratio (TTM)'] = final['PE Ratio (TTM)'].replace('∞', np.nan).astype(float)

    # final_f = final[(final['PE Ratio (TTM)'] < 20) &
    #                 (final['Market Cap (M)'] >= 1000) &
    #                 (final['52WeekDiff'] < 30)]
    # del final_f['Unnamed: 0']

    final_f = final[['Ticker', 'DownFromAllTimeHigh', '52Week%Down', 'Market Cap (M)',
                     'PE Ratio (TTM)', 'EPS (TTM)', '1y Target Est', '52%Up',
                     '52 Week Range', '52WeekDiff', 'Quote Price',
                     '52WeeksUp', '52WeeksDown',
                     '52Week%Current', 'Beta (5Y Monthly)', 'Ask',
                     'Bid', 'Avg. Volume', "Day's Range",
                     'Expense Ratio (net)', 'Inception Date', 'NAV',
                     'Net Assets', 'Open', 'Previous Close',
                     'Volume', 'YTD Daily Total Return', 'Yield',
                     'Earnings Date', 'Ex-Dividend Date', 'Forward Dividend & Yield',
                     'Market Cap', '5y Average Return', 'Average for Category',
                     'Category', 'Holdings Turnover', 'Last Cap Gain', 'Last Dividend',
                     'Morningstar Rating', 'Morningstar Risk Rating', 'Sustainability Rating',
                     'YTD Return',  'AllTimeHigh', ]]

    print('end')


def get_candles_for_ticker(ticker, tickers):

    candles = yf.download(tickers=ticker, interval="1d", period='50000d')

    # _____ Convert Date _____
    candles['Date'] = pd.to_datetime(candles.index)
    candles['Timestamp'] = candles['Date'].apply(mpl_dates.date2num)
    candles['Ticker'] = ticker
    candles.reset_index(drop=True, inplace=True)

    date_first_added = tickers[ticker]
    try:
        candles = candles[candles['Date'] >= pd.to_datetime(tickers[ticker], format='%Y-%m-%d')]
    except:
        pass

    candles = add_all_time_high(candles)

    return candles


def get_candles_for_ticker_v2(ticker, tickers):

    candles = yf.download(tickers=ticker, interval="1d", period='50000d')

    # _____ Convert Date _____
    candles['Date'] = pd.to_datetime(candles.index)
    candles['Timestamp'] = candles['Date'].apply(mpl_dates.date2num)
    candles['Ticker'] = ticker
    candles.reset_index(drop=True, inplace=True)

    candles = add_all_time_high(candles)

    return candles





def add_intervals_to_candles(candles):

    intervals = find_intervals(candles=candles, sens_up=0.1, sens_down=0.075)
    intervals_medium = find_intervals(candles=candles, sens_up=0.25, sens_down=0.2)
    intervals_long = find_intervals(candles=candles, sens_up=0.5, sens_down=0.33)

    for interval in intervals:
        index_start = interval['index_start']
        index_stop = interval['index_stop']
        if interval['trend'] == 'down':
            candles.at[index_start, 'TopS'] = True
            candles.at[index_stop, 'BottomS'] = True
        elif interval['trend'] == 'up':
            candles.at[index_start, 'BottomS'] = True
            candles.at[index_stop, 'TopS'] = True

    for interval in intervals_medium:
        index_start = interval['index_start']
        index_stop = interval['index_stop']
        if interval['trend'] == 'down':
            candles.at[index_start, 'TopM'] = True
            candles.at[index_stop, 'BottomM'] = True
        elif interval['trend'] == 'up':
            candles.at[index_start, 'BottomM'] = True
            candles.at[index_stop, 'TopM'] = True

    for interval in intervals_long:
        index_start = interval['index_start']
        index_stop = interval['index_stop']
        if interval['trend'] == 'down':
            candles.at[index_start, 'TopL'] = True
            candles.at[index_stop, 'BottomL'] = True
        elif interval['trend'] == 'up':
            candles.at[index_start, 'BottomL'] = True
            candles.at[index_stop, 'TopL'] = True

    return candles


def plot_candles(candles, intervals=None):

    # Creating Subplots
    fig, ax = plt.subplots()

    candlestick_ohlc(ax, candles.values, width=0.6, colorup='green', colordown='red', alpha=0.8)

    # Setting labels & titles
    ax.set_xlabel('Date')
    ax.set_ylabel('Price')
    fig.suptitle('TSLA D')

    if intervals:
        for interval in intervals:
            plt.axvline(x=candles.iloc[interval['index_stop']]['Date'], linestyle='--',
                        color='green' if interval['trend'] == 'up' else 'red', alpha=0.3)

    # Formatting Date
    date_format = mpl_dates.DateFormatter('%d-%m-%Y')
    ax.xaxis.set_major_formatter(date_format)
    ax.set_yscale('log')
    fig.autofmt_xdate()
    fig.tight_layout()
    ax.grid()
    plt.show()


def find_intervals(candles, sens_up=0.05, sens_down=0.05, start=0):

    up_int = run_up_interval(candles, sens_up, None)
    down_int = run_down_interval(candles, sens_down, None)

    intervals = sorted(up_int + down_int, key=lambda k: (k['index_start'], k['index_stop']))

    if len(intervals) == 0:
        return None

    for interval in intervals:
        interval['index_start'] += start
        interval['index_stop'] += start

    intervals = split_int(intervals)

    # ____ Recursion ___
    # if len(intervals) > 0:
    #     candles = candles.iloc[intervals[-1]['index_stop'] - start:]
    #     start = intervals[-1]['index_stop']
    #     int_plus = find_intervals(candles, sens_up, sens_down, start)
    #     int_plus = split_int(int_plus)
    #     if int_plus:
    #         intervals = sorted(intervals + int_plus, key=lambda k: (k['index_start'], k['index_stop']))

    return precision_intervals(intervals=intervals, candles=candles, sens_up=sens_up, sens_down=sens_down)


def run_down_interval(candles, sens_down, sens_time=None, include_last=True, start_next=False):

    end_of_df = False
    overwrite = False
    i = 0 if not start_next else 1
    out = []
    r = len(candles) - 1

    while not end_of_df and r > 0:

        max_value = candles['High'].iloc[i]
        end_of_interval = False
        max_perc = 0
        new_min = 0
        i_stop = 0
        j = i
        rj = r

        # ___ If the following candle high is not greater than the current candle high ___
        if candles['High'].iloc[i + 1] < candles['High'].iloc[i]:

            while not end_of_interval and rj > 0:

                # ___ If the following candle low is less than the minimum desired decrement ___
                if candles['Low'].iloc[j + 1] <= max_value * (1 - sens_down):
                    new_perc = candles['Low'].iloc[j + 1] / max_value - 1
                    if new_perc < max_perc:
                        max_perc = new_perc
                        new_min = candles['Low'].iloc[j + 1]
                        i_stop = j + 1

                if candles['High'].iloc[j + 1] >= max_value:
                    end_of_interval = True
                    if max_perc < 0:
                        if overwrite:
                            out[-1] = {'delta': max_value - new_min, 'perc': max_perc * 100,
                                       'index_start': i, 'index_stop': i_stop, 'trend': 'down',
                                       'min': new_min, 'max': max_value}
                        else:
                            out.append({'delta': max_value - new_min, 'perc': max_perc * 100,
                                       'index_start': i, 'index_stop': i_stop, 'trend': 'down',
                                       'min': new_min, 'max': max_value})
                            overwrite = False
                else:
                    j += 1
                    rj -= 1
                    if not rj and include_last and i_stop > 0:
                        out.append({'delta': max_value - new_min, 'perc': max_perc * 100,
                                       'index_start': i, 'index_stop': i_stop, 'trend': 'down',
                                       'min': new_min, 'max': max_value})
                        overwrite = True

        i = j + 1
        if i >= len(candles):
            end_of_df = True
        else:
            r = len(candles) - i - 1

    return out


def run_up_interval(candles, sens_up, sens_time=None, include_last=True, start_next=False):

    end_of_df = False
    overwrite = False
    i = 0 if not start_next else 1
    out = []
    r = len(candles) - 1

    while not end_of_df and r > 0:

        min_value = candles['Low'].iloc[i]
        if min_value == 0:
            r = 0
            continue
        end_of_interval = False
        max_perc = 0
        new_max = 0
        i_stop = 0
        j = i
        rj = r

        # ___ If the following candle low is not lower than the current candle low ___
        if candles['Low'].iloc[i + 1] > candles['Low'].iloc[i]:

            while not end_of_interval and rj > 0:

                # ___ If the following candle high is higher than the minimum desired increment ___
                if candles['High'].iloc[j + 1] >= min_value * (1 + sens_up):
                    new_perc = candles['High'].iloc[j + 1] / min_value - 1
                    if new_perc > max_perc:
                        max_perc = new_perc
                        new_max = candles['High'].iloc[j + 1]
                        i_stop = j + 1

                if candles['Low'].iloc[j + 1] <= min_value:
                    end_of_interval = True
                    if max_perc > 0:
                        if overwrite:
                            out[-1] = {'delta': new_max - min_value, 'perc': max_perc * 100,
                                       'index_start': i, 'index_stop': i_stop, 'trend': 'up',
                                       'min': min_value, 'max': new_max}
                        else:
                            out.append({'delta': new_max - min_value, 'perc': max_perc * 100,
                                        'index_start': i, 'index_stop': i_stop, 'trend': 'up',
                                        'min': min_value, 'max': new_max})
                            overwrite = False
                else:
                    j += 1
                    rj -= 1
                    if rj == 0 and include_last and i_stop > 0:
                        out.append({'delta': new_max - min_value, 'perc': max_perc * 100,
                                    'index_start': i, 'index_stop': i_stop, 'trend': 'up',
                                    'min': min_value, 'max': new_max})
                        overwrite = True

        i = j + 1
        if i >= len(candles):
            end_of_df = True
        else:
            r = len(candles) - i - 1

    return out


def mix(up, down):

    return sorted(up + down, key=lambda k: (k['index_start'], k['index_stop']))


def split_int(intervals):

    if not intervals:
        return intervals

    s = len(intervals)
    completed = False
    i = 0

    if s == 0:
        return None

    while not completed:

        if intervals[i]['index_stop'] >= intervals[i + 1]['index_stop'] and intervals[i + 1]['index_start'] < intervals[i]['index_stop']:

            i_stop = intervals[i]['index_stop']
            max_value = intervals[i]['max']
            min_value = intervals[i]['min']
            intervals[i]['index_stop'] = intervals[i + 1]['index_start']
            if intervals[i]['trend'] == 'up':
                intervals[i]['max'] = intervals[i + 1]['max']
                intervals[i]['delta'] = intervals[i]['max'] - intervals[i]['min']
                intervals[i]['perc'] = (intervals[i]['max'] / intervals[i]['min'] - 1) * 100
            else:
                intervals[i]['min'] = intervals[i + 1]['min']
                intervals[i]['delta'] = intervals[i]['max'] - intervals[i]['min']
                intervals[i]['perc'] = (1 - intervals[i]['min'] / intervals[i]['max']) * 100

            new_interval = {'index_start': intervals[i + 1]['index_stop'],
                            'index_stop': i_stop,
                            'trend': intervals[i]['trend']}
            if intervals[i]['trend'] == 'up':
                new_interval['min'] = intervals[i + 1]['min']
                new_interval['max'] = max_value
                new_interval['delta'] = new_interval['max'] - new_interval['min']
                new_interval['perc'] = (new_interval['max'] / new_interval['min'] - 1) * 100
            else:
                new_interval['min'] = min_value
                new_interval['max'] = intervals[i + 1]['max']
                new_interval['delta'] = new_interval['max'] - new_interval['min']
                new_interval['perc'] = (1 - new_interval['min'] / new_interval['max']) * 100

            intervals = intervals[:i + 2] + [new_interval] + intervals[i + 2:]
            i += 2

        else:
            i += 1

        if i >= len(intervals) - 1:
            completed = True

    return intervals


def precision_intervals(intervals, candles, sens_up=0.05, sens_down=0.05, sens_t=0):

    i = 0
    completed = False

    while not completed:

        mint = []

        if intervals[i]['index_stop'] - intervals[i]['index_start'] > 1:

            data_temp = candles.iloc[intervals[i]['index_start']:intervals[i]['index_stop'] + 1]
            if intervals[i]['trend'] == 'up':
                mint = run_down_interval(data_temp, sens_down, sens_t, False, start_next=True)
            else:
                mint = run_up_interval(data_temp, sens_up, sens_t, False, start_next=True)

            if len(mint) > 0:

                index_stop = intervals[i]['index_stop']
                for row in mint:
                    row['index_start'] += intervals[i]['index_start']
                    row['index_stop'] += intervals[i]['index_start']

                intervals = sorted(intervals + mint, key=lambda k: (k['index_start'], k['index_stop']))
                intervals = split_int(intervals)

                # index_start_list = [x['index_start'] for x in intervals]
                # i = [x['index_start'] for x in intervals].index(index_stop) - 1 if index_stop in index_start_list else len(intervals)

        i += 1
        if i > len(intervals) - 1:
            completed = True

    return intervals


def sp500_list():
    import bs4 as bs
    import pickle
    import requests

    resp = requests.get('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    soup = bs.BeautifulSoup(resp.text, 'lxml')
    table = soup.find('table', {'class': 'wikitable sortable'})
    tickers = {}
    for row in table.findAll('tr')[1:]:
        ticker = row.findAll('td')[0].text.strip('\n')
        date_first_added = row.findAll('td')[6].text
        tickers.update({ticker: date_first_added})

    # with open("sp500tickers.pickle", "wb") as f:
    #     pickle.dump(tickers, f)

    return tickers


if __name__ == '__main__':

    # all_time_high_analysis(override=False)
    trend_analysis()
    # sp500_list()

    all_time_high_nasdaq()

    import nasdaqdatalink

    data = nasdaqdatalink.get('NSE/OIL')

    final = pd.read_csv(r'C:\Users\Vale\PycharmProjects\Jarvis\data\final.csv')
    final[['52WeeksDown', '52WeeksUp']] = final['52 Week Range'].str.split(' - ', 1, expand=True)
    final['52WeeksDown'] = final['52WeeksDown'].str.replace(',', '').astype(float)
    final['52WeeksUp'] = final['52WeeksUp'].str.replace(',', '').astype(float)
    final['52Week%Down'] = (1 - final['52WeeksDown'] / final['52WeeksUp']) * 100
    final['52Week%Current'] = (1 - final['Quote Price'] / final['52WeeksUp']) * 100
    final['52WeekDiff'] = final['52Week%Down'] - final['52Week%Current']

    print('end')

    '49q6bW4uF42GiJhmhWS1'