import asyncio
import pytz
from dataclasses import dataclass, field
from time import time, sleep
from datetime import datetime
from threading import Timer
from random import choice, shuffle
from typing import List, Type

from src.common.Command import Command
from src.common.telegram.TelegramMessageType import TelegramMessageType
from src.common.telegram.TelegramManager import TelegramManager
from src.common.telegram.TelegramMessage import TelegramMessage
from src.common.telegram.TelegramUser import TelegramUser
from src.common.telegram.TelegramChat import TelegramChat
from src.common.file_manager.FileManager import FileManager
from src.common.postgre.PostgreManager import PostgreManager
from src.common.tools.library import run_main, get_environ, int_timestamp_now, class_from_args, to_int, timestamp2date

from src.common.functions.Function import Function
from src.common.functions.FunctionCiao import FunctionCiao
from src.common.functions.FunctionCallback import FunctionCallback
from src.common.functions.FunctionProcess import FunctionProcess
from src.common.functions.FunctionStart import FunctionStart

from src.stock.functions.FunctionBack import FunctionBack
from src.stock.functions.FunctionStockNewUser import FunctionStockNewUser
from src.stock.StockUser import StockUser
from src.stock.StockPostgreManager import StockPostgreManager

# __ logging __
import logging
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
# from logging.handlers import TimedRotatingFileHandler
# from logger_tt import setup_logging
# config_path = Path(__file__).parent.parent.parent.joinpath('resources', 'logger.conf.yaml')
# log_conf = setup_logging(config_path=str(config_path), use_multiprocessing=True)
# LOGGER = logging.getLogger()


@dataclass
class Stock(TelegramManager):
    postgre_manager: StockPostgreManager = field(default=None)
    daily_analysis: bool = True
    name: str = "Stock"

    def __post_init__(self):
        super().__post_init__()
        self.stock_users = self.postgre_manager.get_stock_users()
        self.__init_daily_analysis_timer() if self.daily_analysis else None

    @property
    def app_users(self):
        return self.stock_users

    def instantiate_function(self, function, chat: TelegramChat, message: TelegramMessage, is_new: bool, function_id: int, user_x: TelegramUser) -> Function:
        # print(isinstance(function.__class__.__name__, Quotes))
        if "stock_user" not in dir(function):
            return function(bot=self.telegram_bot,
                            chat=chat,
                            message=message,
                            function_id=function_id,
                            is_new=is_new,
                            postgre_manager=self.postgre_manager
                            )
        else:
            return function(bot=self.telegram_bot,
                            chat=chat,
                            message=message,
                            function_id=function_id,
                            is_new=is_new,
                            postgre_manager=self.postgre_manager,
                            stock_user=[x for x in self.stock_users if x.telegram_id == user_x.telegram_id][0]
                            )  # TODO: refactor this function using **kwargs

    async def handle_app_new_user(self,
                                  admin_user: StockUser,
                                  admin_chat: TelegramChat,
                                  new_telegram_user: TelegramUser,
                                  new_telegram_chat: TelegramChat):

        function = await self.get_function_by_alias(alias='appNewUser', chat_id=admin_chat.chat_id, user_x=admin_user)
        if not function:
            return
        message = self._build_new_telegram_message(chat=admin_chat, text='appNewUser')
        admin_chat.new_message(telegram_message=message)
        instantiated_function = self.instantiate_function(function=function, chat=admin_chat, message=message, is_new=True, function_id=message.message_id, user_x=admin_user)
        instantiated_function.initialize()
        instantiated_function.telegram_function.settings["new_user"] = new_telegram_user
        instantiated_function.telegram_function.settings["new_chat"] = new_telegram_chat
        instantiated_function.telegram_function.settings["app"] = self.name
        await instantiated_function.evaluate()
        instantiated_function.post_evaluate()  # TODO: create in Function method "execute with initial settings"

    def app_update_users(self):
        print('\n### refreshing stock users ###\n')
        all_users = self.postgre_manager.get_stock_users()
        diff_users = [x for x in all_users if x.telegram_id not in [x.telegram_id for x in self.app_users]]
        print(diff_users)
        self.stock_users += diff_users

    """ __ __ ROUTINES __ __"""
    def __init_daily_analysis_timer(self):
        # TODO: write this function better
        dt = datetime.now(pytz.timezone('Europe/Rome'))
        eta = ((9 - dt.hour - 1) * 60 * 60) + ((60 - dt.minute - 1) * 60) + (60 - dt.second)

        if eta < 0:
            eta += 24*60*60

        print('Daily Quote set in ' + str(to_int(eta/3600)) + 'h:' + str(to_int((eta % 3600)/60)) + 'm:' + str(to_int(((eta % 3600) % 60))) + 's:')

        self.quote_timer = Timer(eta, self.__asyncio_daily_quote)
        self.quote_timer.name = 'Daily Quote'
        self.quote_timer.start()
        self.daily_analysis = True

    def __asyncio_daily_quote(self):
        asyncio.run(self.__daily_quote())

    async def __daily_quote(self):
        pass
        # query = """
        #         SELECT *
        #         FROM quotes
        #         ORDER BY last_random_tme
        #         LIMIT 100
        #         """
        # quotes = self.postgre_manager.select_query(query=query)
        # if len(quotes) == 0:
        #     return None
        #
        # quote = choice(quotes)
        # self.postgre_manager.update_quote_by_quote_id(quote_id=quote['quote_id'], set_params={'last_random_tme': to_int(time())})
        #
        # print(f"Start sending daily quotes at {timestamp2date(time())}")
        # for user in self.quotes_users:
        #     if user.daily_quotes:
        #         quote_in_language = self.postgre_manager.get_quote_in_language(quote=quote, user=user)
        #         author = quote['author'].replace('_', ' ')
        #         text = f"*Quote of the day*\n\n{quote_in_language}\n\n_{author}_"
        #
        #         function = await self.get_function_by_alias(alias='dailyQuote', chat_id=user.telegram_id, user_x=user)
        #         if not function:
        #             continue
        #
        #         chat = self.get_chat_from_telegram_id(telegram_id=user.telegram_id)
        #         message = self._build_new_telegram_message(chat=chat, text='dailyQuote')
        #         instantiated_function = self.instantiate_function(function=function, chat=chat, message=message, is_new=True, function_id=message.message_id, user_x=user)
        #         instantiated_function.initialize()
        #         instantiated_function.telegram_function.settings["text"] = text
        #         await instantiated_function.evaluate()
        #         instantiated_function.post_evaluate()
        #     sleep(0.2)
        # print(f"Sending daily quotes completed at {timestamp2date(time())}")


def main():

    # __ whether it's running on Heroku or local
    os_environ = get_environ() == 'HEROKU'

    # __ determine sslmode depending on environ configurations __
    sslmode = 'require' if os_environ else 'disable'

    # __ init file manager __
    config_manager = FileManager()

    # __ get telegram token __
    token = config_manager.get_telegram_token()
    postgre_url = config_manager.get_postgre_url()
    admin_info = config_manager.get_admin()
    admin_chat = config_manager.get_admin_chat()

    postgre_manager = StockPostgreManager(db_url=postgre_url)
    if not postgre_manager.connect(sslmode=sslmode):
        # logger.warning("PostgreDB connection not established: cannot connect")
        return

    admin_user = TelegramUser(telegram_id=admin_info["chat"], name=admin_info["name"], username=admin_info["username"], is_admin=True)
    admin_chat = TelegramChat(chat_id=admin_chat['chat_id'], type=admin_chat["type"], username=admin_chat["username"], first_name=admin_chat["first_name"], last_name=admin_chat["last_name"])
    telegram_users = postgre_manager.get_telegram_users(admin_user=admin_user)
    telegram_chats = postgre_manager.get_telegram_chats_from_db(admin_chat=admin_chat)

    commands = [
        Command(alias=["callback"], admin=False, function=FunctionCallback),
        Command(alias=["back"], admin=False, function=FunctionBack),
        Command(alias=["start"], admin=False, function=FunctionStart),
        Command(alias=["appNewUser"], admin=True, function=FunctionStockNewUser, restricted=True),
    ]

    stock = Stock(token=token, users=telegram_users, chats=telegram_chats, commands=commands, postgre_manager=postgre_manager)
    stock.start()
    # asyncio.run(telegram_manager.telegram_bot.send_message(chat_id=19838246, text='ciao', inline_keyboard=[['<', '>']]))

    # tables = [x[0] for x in postgre_manager.get_tables()]
    # init_tables(postgre_manager=postgre_manager, tables=tables) if init_tables else None

    while stock.run:
        sleep(0.5)

    # LOGGER.info('end')


if __name__ == '__main__':
    main()

