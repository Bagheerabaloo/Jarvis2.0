import yfinance as yf
import yahoo_fin.stock_info as si
import pytz
import json
from datetime import datetime, timedelta
from queue import Queue
from threading import Timer
from src.Tools.library import to_int, get_exception
from src.Telegram.TelegramManager import TelegramManager
from src.Apps.StockMarket.candleAnalysis import add_candlestick_patterns


class Stock(TelegramManager):

    FUNCTIONS = {}

    ADMIN_FUNCTIONS = {'analysis': {'analysis'},
                       'financial': {'financial'},
                       'show_favourites': {'showFav'},
                       'add_favourite': {'addFav'},
                       'remove_favourite': {'removeFav'}
                       }

    SIMPLE_SEND = {}

    def __init__(self, super_init=True, **kwargs):

        self.base_classes = [x.__name__ for x in self.__class__.__bases__]

        if super_init:
            super().__init__(caller="Stock", **kwargs)

        self.quote_timer = None

        self.__restore_favourites()
        self.__init_analysis_timer()

    # _____ Initialization _____
    def __init_analysis_timer(self):

        dt = datetime.now(pytz.timezone('US/Eastern'))
        weekday = dt.weekday()

        # __ Get next opening hour (30 minutes before) - 9:00 am US/Eastern time __
        eta_open = ((9 - dt.hour - 1) * 60 * 60) + ((60 - dt.minute - 1) * 60) + (60 - dt.second)
        if eta_open < 0:
            eta_open += 24*60*60

        # __ Get next closing hour (30 minutes before) - 4:30 pm US/Eastern time __
        eta_close = ((16 - dt.hour - 1) * 60 * 60) + ((30 - dt.minute - 1) * 60) + (60 - dt.second)
        if eta_close < 0:
            eta_close += 24 * 60 * 60

        # __ Excludes weekends __
        if (dt + timedelta(0, eta_open)).weekday() > 4:
            eta_open += 24*60*60 * (7 - (dt + timedelta(0, eta_open)).weekday())

        if (dt + timedelta(0, eta_close)).weekday() > 4:
            eta_close += 24*60*60 * (7 - (dt + timedelta(0, eta_close)).weekday())

        eta = min(eta_close, eta_open)

        self.logger.info('Daily Analysis set in ' + str(to_int(eta/3600)) + 'h:' + str(to_int((eta%3600)/60)) + 'm:' + str(to_int(((eta%3600)%60))) + 's:')

        self.quote_timer = Timer(eta, self.__daily_analysis)
        self.quote_timer.name = 'Daily Analysis'
        self.quote_timer.start()

    # _____ Load/Store Alerts _____
    def __restore_favourites(self):

        stock_favourites = self.load(key="stock_favourites")
        self.stock_favourites = stock_favourites if stock_favourites else []

    # _____ Close _____
    def close(self):
        self.close_stock()

    def close_stock(self, close_core_thread=True):

        if 'TelegramManager' in self.base_classes and close_core_thread:
            self.close_telegram_manager()
        if 'CoreApp' in self.base_classes and close_core_thread:
            self.close_core_app()
        self.__close_daily_analysis_timer()

    def __close_daily_analysis_timer(self):

        if self.quote_timer is None:
            return True

        try:
            self.quote_timer.cancel()
            return True
        except:
            self.logger.warning(get_exception())
            return False

    # _____ Analysis _____
    def add_favourite(self, user_x):

        # STATE 0
        if user_x.state_function == 0:

            txt = "Insert symbol"
            self.send_message(user_x, text=txt)
            return user_x.next()

        # STATE 1
        if user_x.state_function == 1:

            symbol = user_x.last_message

            if symbol not in self.stock_favourites:
                self.stock_favourites.append(symbol)
                self.save(key='stock_favourites', data=self.stock_favourites)

            return user_x.reset()

    def remove_favourite(self, user_x):

        # STATE 0
        if user_x.state_function == 0:

            txt = "Insert symbol"
            self.send_message(user_x, text=txt)
            return user_x.next()

        # STATE 1
        if user_x.state_function == 1:

            symbol = user_x.last_message

            if symbol in self.stock_favourites:
                self.stock_favourites.remove(symbol)
                self.save(key='stock_favourites', data=self.stock_favourites)

            return user_x.reset()

    def show_favourites(self, user_x):
        self.send_message(user_x, text=json.dumps(self.stock_favourites))

    def analysis(self, user_x):

        # STATE 0
        if user_x.state_function == 0:

            txt = "Select symbol"
            self.send_message(user_x, text=txt)

            return user_x.next()

        # STATE 1
        if user_x.state_function == 1:

            ticker = user_x.last_message

            self.__send_analysis(user_x, ticker)

            return user_x.reset()

    def financial(self, user_x):

        # STATE 0
        if user_x.state_function == 0:

            txt = "Select ticker"
            self.send_message(user_x, text=txt)

            return user_x.next()

        # STATE 1
        if user_x.state_function == 1:

            ticker = user_x.last_message

            self.__send_financial(user_x, ticker)

            return user_x.reset()

    def __send_analysis(self, user_x, ticker, pending=False):

        candles = yf.download(tickers=ticker, interval="1d", period='365d')

        if len(candles) == 0:
            return False

        candles = add_candlestick_patterns(candles)

        today_candles = candles.iloc[-1]
        candle_color = 'Green' if today_candles['bullish'] else 'Red'

        txt = "{}\n{} candle".format(ticker, candle_color)
        if today_candles['spinningTop']:
            txt += '\n Spinning Top'
        if today_candles['umbrellaLine']:
            txt += '\n Umbrella Line'
        if today_candles['engulfingBullish'] or today_candles['engulfingBearish']:
            txt += '\n Bullish Engulfing' if today_candles['engulfingBullish'] else '\n Bearish Engulfing'
        if today_candles['longCandleBullish'] or today_candles['longCandleBearish']:
            txt += '\n Bullish longCandle' if today_candles['longCandleBullish'] else '\n Bearish longCandle'

        self.send_message(user_x=user_x, text=txt, pending=pending)
        return True

    def __send_financial(self, user_x, ticker, pending=False):

        financial_data = si.get_quote_table(ticker)

        txt = "*{}*".format(ticker)
        txt += '\n_EPS (TTM): {}_'.format(financial_data['EPS (TTM)'])
        txt += '\n_PE Ratio (TTM): {}_'.format(financial_data['PE Ratio (TTM)'])
        txt += '\n_Market Cap: {}_'.format(financial_data['Market Cap'])
        txt += '\n_Earnings Date: {}_'.format(financial_data['Earnings Date'])
        txt += '\n_Last: {}_'.format(financial_data['Quote Price'])
        txt += '\n_1y Target Est: {}_'.format(financial_data['1y Target Est'])

        self.send_message(user_x=user_x, text=txt, pending=pending, parse_mode="Markdown")
        return True

    def __get_analysis(self, user_x, symbol):

        candles = yf.download(tickers=symbol, interval="1d", period='365d')

        if len(candles) == 0:
            return False

        candles = add_candlestick_patterns(candles)

        today_candles = candles.iloc[-1]
        candle_color = 'Green' if today_candles['bullish'] else 'Red'
        useful = False
        txt = "{}\n{} candle".format(symbol, candle_color)
        for field in ['umbrellaLine', 'umbrellaLineInverted', 'longCandleBearish', 'longCandleBullish',
                      'engulfingBullish', 'engulfingBearish', 'piercingPattern', 'piercingPatternLight',
                      'darkCloudCover', 'darkCloudCoverLight', 'onNeckPattern', 'inNeckPattern',
                      'thrustingPattern', 'star', 'morningStar', 'eveningStar']:
            if today_candles[field]:
                txt += '\n {}'.format(field)
                useful = True

        if useful:
            self.send_message(user_x=user_x, text=txt)
        return True

    def __daily_analysis(self):

        user_x = [x for x in self.users if x.is_admin][0]
        for symbol in self.stock_favourites:
            self.__get_analysis(user_x, symbol)

        self.__init_analysis_timer()


if __name__ == '__main__':

    from src.Tools.library import run_main, get_environ

    logging_queue = Queue()
    os_environ = get_environ() == 'HEROKU'

    postgre_key_var = 'DATABASE_URL' if os_environ else 'postgre_url'

    stock_app = Stock(os_environ=os_environ, logging_queue=logging_queue, postgre_key_var=postgre_key_var, telegram_token_var='JARVIS_TOKEN')
    if 'TelegramManager' not in stock_app.base_classes:
        stock_app.start_main_thread()

    run_main(stock_app, logging_queue)








