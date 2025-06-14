import pandas as pd
import yfinance as yf
from mpl_finance import candlestick_ohlc
from src.Tools.library import *
import matplotlib.dates as mpl_dates


def candles_analysis():

    try:
        candles = yf.download(tickers="AAPL", interval="1d", period='3000d')
    except ValueError as e:
        print('Down')
        return

    # _____ Convert Date _____
    candles['Date'] = pd.to_datetime(candles.index)
    candles['Timestamp'] = candles['Date'].apply(mpl_dates.date2num)
    candles.reset_index(drop=True, inplace=True)

    candles = add_candlestick_patterns(candles)

    candles['Date'] = pd.to_datetime(candles.index)
    candles['Date'] = candles['Date'].apply(mpl_dates.date2num)
    candles = candles.astype(float)
    candles = candles[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]

    # Creating Subplots
    fig, ax = plt.subplots()

    candlestick_ohlc(ax, candles.values, width=0.6, colorup='green', colordown='red', alpha=0.8)

    # Setting labels & titles
    ax.set_xlabel('Date')
    ax.set_ylabel('Price')
    fig.suptitle('TSLA D')

    # Formatting Date
    date_format = mpl_dates.DateFormatter('%d-%m-%Y')
    ax.xaxis.set_major_formatter(date_format)
    ax.set_yscale('log')
    fig.autofmt_xdate()

    fig.tight_layout()

    ax.grid()
    plt.show()


def add_candlestick_patterns(candles):

    """
    :bullish            --> If Close is strictly greater than Open - The color of the candle (red or green)
    :bodyDelta          --> The extension of the body (Close - Open) (absolute value)
    :shadowDelta        --> The extension of the full candle (High - Low)
    :%body              --> The ratio between the body extension and the full candle extension in % (bodyDelta/shadowDelta)
    :%upperShadow       --> The ratio between the upper shadow and the full candle extension in %
    :%lowerShadow       --> The ratio between the lower shadow and the full candle extension in %
    :longCandle         --> %body > 70% - A candle that is constituted mainly by the body
    :shadowImbalance    --> It's the ratio of the longest shadow (upper or lower) and the shortest shadow.
                            It's range goes from 1 (perfectly balanced) to infinity (completely imbalanced)
    :shavenHead         --> If the candle has no upper shadow (%upperShadow = 0)
    :shavenBottom       --> If the candle has no lower shadow (%lowerShadow = 0)
    :doji               --> If the candle has no body (%body = 0)
    :spinningTop        --> If the % of the body is below 30%
    :umbrellaLine       --> If the candle is a spinningTop and the upperShadow is less than half the body --> from here we have Hammer and Hanging Man
    :umbrellaLineInverted   --> If the candle is a spinningTop and the lowerShadow is less than half the body --> from here we have the Shooting Star and the Inverted Hammer
    :midBody            --> Price of the mid point between Close and Open
    :engulfingBullish   --> If the candle is green (bullish), it's not a spinningTop, the previous candle is red or doji (not bullish)
                            and the Open is less or equal to the previous close and the Close is greater or equal to the previous Open
    :engulfingBearish   --> If the candle is red (not bullish), it's not a spinningTop, the previous candle is green or doji (bullish)
                            and the Open is greater or equal to the previous close and the Close is less or equal to the previous Open
    :MA50               --> The 50 samples rolling moving average of the close prices
    :MA100              --> The 100 samples rolling moving average of the close prices
    :MA200              --> The 200 samples rolling moving average of the close prices
    :TR                 --> True range of each candle
    :ATR                --> Average True Range (14 samples window)
    :ATR%               --> Average True Range divided by the Close price
    :RSI                --> Relative Strength Index (14 samples window)
    :bodyATR%           --> The extension of the body divided by the Open price in %
    :body2ATR%          --> bodyATR% / ATR%' * 100 - It gives the sensibility of the body volatility with respect to the average volatility (ATR)
                            If this number is greater than 50%, it means it's a long volatility candle
    :longATRCandle      --> body2ATR% > 50% - Long volatility body
    :body2ATR%2shadowImbalanceRatio     --> body2ATR% / sqrt(shadowImbalance). The higher the body volatility the higher this ratio
                                            The lowest the shadow imbalance the higher this ratio.
                                            Higher values of this value means that the candle is a long bullish or long bearish session

    :trigger            --> ( %body^2 * body2ATR%2shadowImbalanceRatio ) / 100^2

    :longCandle         --> If the candle is a longATRCandle (long volatility body)
                            and %body > 90%
                                 or (%body > 80% and body2ATR%2shadowImbalanceRatio > 10
                                 or (%body > 70% and body2ATR%2shadowImbalanceRatio > 50)
    :longCandleBullish  --> If it's a longCandle and the candle Close is greater or equal than the max between the Open and the Close of the last two session (including this)
    :longCandleBearish  --> If it's a longCandle and the candle Close is less or equal than the max between the Open and the Close of the last two session (including this)
    """

    # _____ One Candle properties _____
    candles['bullish'] = candles['Close'] > candles['Open']
    candles['bodyDelta'] = abs(candles['Close'] - candles['Open'])
    candles['shadowDelta'] = candles['High'] - candles['Low']
    candles['%body'] = candles['bodyDelta'] / candles['shadowDelta'] * 100
    candles['%upperShadow'] = 100 * (candles['High'] - candles[['Close', 'Open']].max(axis=1)) / candles['shadowDelta']
    candles['%lowerShadow'] = 100 * (candles[['Close', 'Open']].min(axis=1) - candles['Low']) / candles['shadowDelta']
    candles['longBody'] = candles['%body'] > 70
    candles['shadowImbalance'] = candles[['%upperShadow', '%lowerShadow']].max(axis=1) / candles[['%upperShadow', '%lowerShadow']].min(axis=1)
    candles['shavenHead'] = candles['%upperShadow'] == 0
    candles['shavenBottom'] = candles['%lowerShadow'] == 0
    candles['doji'] = candles['%body'] == 0
    candles['spinningTop'] = candles['%body'] < 30
    # candles['umbrellaLine'] = (candles['spinningTop']) & (candles['%upperShadow'] * 2 < candles['%body'])
    candles['umbrellaLine'] = (candles['spinningTop']) & (candles['%lowerShadow'] > 60)
    candles['umbrellaLineInverted'] = (candles['spinningTop']) & (candles['%lowerShadow'] * 2 < candles['%body'])
    candles['midBody'] = (candles['Close'] + candles['Open']) / 2

    candles['hammer'] = candles['umbrellaLine'] & (candles['Trend'] == 'down') & (candles['reversing'] == False)

    # _____ Indicators _____
    #   __ Moving Averages __
    candles['MA50'] = candles['Close'].rolling(window=50).mean()
    candles['MA100'] = candles['Close'].rolling(window=100).mean()
    candles['MA200'] = candles['Close'].rolling(window=200).mean()
    #   __ Oscillators __
    candles = add_relative_strength_index(candles, 14)
    #   __ Volatility __
    candles = add_true_range(candles)
    candles['ATR'] = candles['TR'].rolling(window=14).mean()
    candles['ATR%'] = candles['ATR'] / candles['Close'] * 100

    # _____ One Candle Properties (volatility needed) _____
    candles['bodyATR%'] = candles['bodyDelta'] / candles['Open'] * 100
    candles['body2ATR%'] = candles['bodyATR%'] / candles['ATR%'] * 100
    candles['longATRCandle'] = candles['body2ATR%'] > 50
    candles['body2ATR%2shadowImbalanceRatio'] = candles['body2ATR%'] / np.sqrt(candles['shadowImbalance'])
    # candles['trigger'] = candles['%body'].pow(2)/100 * candles['body2ATR%2shadowImbalanceRatio'] / 100
    candles['longCandleLight'] = candles['longATRCandle'] & candles['longBody']
    candles['longCandleBullishLight'] = candles['longCandleLight'] & (candles['Close'] >= candles[['Close', 'Open']].rolling(2).max().max(axis=1))
    candles['longCandleBearishLight'] = candles['longCandleLight'] & (candles['Close'] <= candles[['Close', 'Open']].rolling(2).min().min(axis=1))

    candles['longCandle'] = candles['longATRCandle'] & ((candles['%body'] > 90) | ((candles['%body'] > 80) & (candles['body2ATR%2shadowImbalanceRatio'] > 10)) | ((candles['%body'] > 70) & (candles['body2ATR%2shadowImbalanceRatio'] > 50)))
    candles['longCandleBullish'] = candles['longCandle'] & (candles['Close'] >= candles[['Close', 'Open']].rolling(2).max().max(axis=1))
    candles['longCandleBearish'] = candles['longCandle'] & (candles['Close'] <= candles[['Close', 'Open']].rolling(2).min().min(axis=1))

    # _____ Candles patterns _____
    candles = add_engulfing_pattern(candles)
    candles = add_semi_engulfing_patterns(candles)
    candles = add_stars_patterns(candles)

    # _____ Selection _____
    # candles[['Date', 'ATR%', 'bodyATR%', 'body2ATR%', 'longATRCandle', '%body', 'shadowImbalance', 'body2ATR%2shadowImbalanceRatio', 'longCandle', 'longCandleBullish', 'longCandleBearish']]

    return candles


def add_engulfing_pattern(candles):

    candles['engulfingBullish'] = False
    candles['engulfingBearish'] = False
    for index, row in candles.iterrows():
        if index == 0:
            continue
        prev_row = candles.iloc[index - 1]
        if row['bullish'] and not row['spinningTop'] and not prev_row['bullish'] and row['Open'] <= prev_row['Close'] and row['Close'] >= prev_row['Open']:
            candles.at[index, 'engulfingBullish'] = True
        elif not row['bullish'] and not row['spinningTop'] and (prev_row['bullish'] or prev_row['doji']) and row['Open'] >= prev_row['Close'] and row['Close'] <= prev_row['Open']:
            candles.at[index, 'engulfingBearish'] = True
    return candles


def add_semi_engulfing_patterns(candles):

    candles['darkCloudCover'] = False
    candles['darkCloudCoverLight'] = False
    candles['piercingPattern'] = False
    candles['piercingPatternLight'] = False
    candles['onNeckPattern'] = False
    candles['inNeckPattern'] = False
    candles['thrustingPattern'] = False
    for index, row in candles.iterrows():
        if index == 0:
            continue
        prev_row = candles.iloc[index - 1]
        if prev_row['longCandleBullishLight']:
            if row['Open'] > prev_row['High'] and prev_row['Open'] < row['Close'] < prev_row['midBody']:
                candles.at[index, 'darkCloudCover'] = True
            elif row['Open'] > prev_row['Close'] and prev_row['Open'] < row['Close'] < prev_row['midBody']:
                candles.at[index, 'darkCloudCoverLight'] = True
        elif prev_row['longCandleBearishLight']:
            if row['Open'] < prev_row['Low']:
                if row['Close'] > prev_row['midBody']:
                    candles.at[index, 'piercingPattern'] = True
                elif not row['longCandleLight'] and not row['longCandle'] and (prev_row['Low'] < row['Close'] < (prev_row['Low'] + prev_row['Close']) / 2):
                    candles.at[index, 'onNeckPattern'] = True
                elif not row['longCandleLight'] and not row['longCandle'] and (prev_row['Close'] < row['Close'] < (prev_row['Close'] + prev_row['midBody']) / 2):
                    candles.at[index, 'inNeckPattern'] = True
                elif row['longCandleLight'] and (prev_row['Close'] < row['Close'] < prev_row['midBody']):
                    candles.at[index, 'thrustingPattern'] = True
            elif row['Open'] < prev_row['Close'] and prev_row['Open'] > row['Close'] > prev_row['midBody']:
                candles.at[index, 'piercingPatternLight'] = True

    return candles


def add_stars_patterns(candles):

    candles['star'] = False
    candles['morningStar'] = False
    candles['eveningStar'] = False
    for index, row in candles.iterrows():
        if index == 0:
            continue
        prev_row = candles.iloc[index - 1]

        if prev_row['longCandleBullishLight']:
            if row['spinningTop'] and row['Open'] > prev_row['Close'] and row['Close'] > prev_row['Close']:
                candles.at[index, 'star'] = True
        elif prev_row['longCandleBearishLight']:
            if row['spinningTop'] and row['Open'] < prev_row['Close'] and row['Close'] < prev_row['Close']:
                candles.at[index, 'star'] = True

    for index, row in candles.iterrows():
        if index < 2:
            continue
        prev_row = candles.iloc[index - 1]
        prev_row_2 = candles.iloc[index - 2]

        if prev_row['star'] and prev_row_2['longCandleBullishLight']:
            if row['Close'] <= prev_row_2['midBody']:
                candles.at[index, 'eveningStar'] = True
        elif prev_row['star'] and prev_row_2['longCandleBearishLight']:
            if row['Close'] >= prev_row_2['midBody']:
                candles.at[index, 'morningStar'] = True

    return candles


def add_true_range(candles):

    if candles.shape[0] == 0:
        return candles

    candles['prevClose'] = candles['Close'].shift(1)
    candles['hl'] = candles['High'] - candles['Low']
    candles['hc'] = abs(candles['High'] - candles['prevClose'])
    candles['lc'] = abs(candles['Low'] - candles['prevClose'])

    candles['TR'] = candles[['hl', 'hc', 'lc']].max(axis=1)
    return candles.drop(columns=['hl', 'hc', 'lc'])


def add_relative_strength_index(candles, period=14):

    row_total = candles.shape[0]

    if row_total < period:
        return candles

    candles.at[0, 'incrP'] = 0
    candles.at[0, 'incrN'] = 0

    for index in range(1, row_total):

        diff = candles['Close'].iloc[index] - candles['Close'].iloc[index-1]
        if diff >= 0:
            candles.at[index, 'incrP'] = diff
            candles.at[index, 'incrN'] = 0
        else:
            candles.at[index, 'incrP'] = 0
            candles.at[index, 'incrN'] = -diff

    candles['rsP'] = np.nan
    candles['rsN'] = np.nan
    candles.at[period-1, 'rsP'] = 0
    candles.at[period-1, 'rsN'] = 0
    candles['rsP'] = candles['rsP'].fillna(method='bfill')
    candles['rsN'] = candles['rsN'].fillna(method='bfill')

    candles.at[period, 'rsP'] = candles['incrP'].iloc[:period].rolling(period).mean().iloc[period-1]
    candles.at[period, 'rsN'] = candles['incrN'].iloc[:period].rolling(period).mean().iloc[period-1]
    candles.at[period, 'RSI'] = 100 - 100 / (1 + candles['rsP'].iloc[period] / candles['rsN'].iloc[period])

    for index in range(period+1, row_total):
        candles.at[index, 'rsP'] = (candles['rsP'].iloc[index-1] * (period-1) + candles['incrP'].iloc[index]) / period
        candles.at[index, 'rsN'] = (candles['rsN'].iloc[index - 1] * (period - 1) + candles['incrN'].iloc[index]) / period
        candles.at[index, 'RSI'] = 100 - 100 / (1 + candles['rsP'].iloc[index] / candles['rsN'].iloc[index])

    return candles.drop(columns=['incrP', 'incrN', 'rsP', 'rsN'])


def rsi_forsee():

    try:
        candles = yf.download(tickers="META", interval="1d", period='50d')
    except ValueError as e:
        print('Down')
        return

    # _____ Convert Date _____
    candles['Date'] = pd.to_datetime(candles.index)
    candles['Timestamp'] = candles['Date'].apply(mpl_dates.date2num)
    candles.reset_index(drop=True, inplace=True)

    candles.at[50, 'Close'] = 183

    # _____ Convert Date _____
    candles = add_relative_strength_index(candles)

    print('end')


if __name__ == '__main__':

    candles_analysis()
    # rsi_forsee()