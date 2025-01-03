import asyncio
import pytz
from dataclasses import dataclass, field
from time import time, sleep
from datetime import datetime
from threading import Timer
from random import choice, shuffle
from typing import List, Type

from common.telegram_manager.TelegramUser import TelegramUser
from common.telegram_manager.TelegramChat import TelegramChat
from common.telegram_manager.TelegramMessage import TelegramMessage
from common.telegram_manager.telegram_manager import TelegramManager, LOGGER

from common.functions.Function import Function
from common.tools import to_int, timestamp2date, build_eta

from stock.src.app.StockUser import StockUser
from stock.src.app.StockPostgreManager import StockPostgreManager


@dataclass
class StockApp(TelegramManager):
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

    """ ###### ROUTINES ##### """
    def __init_daily_analysis_timer(self):
        eta = build_eta(target_hour=9, target_minute=00)

        print('Daily Quote set in ' + str(to_int(eta/3600)) + 'h:' + str(to_int((eta % 3600)/60)) + 'm:' + str(to_int(((eta % 3600) % 60))) + 's:')

        self.quote_timer = Timer(eta, self.__asyncio_daily_analysis)
        self.quote_timer.name = 'Daily Quote'
        self.quote_timer.start()
        self.daily_quote = True
        # self.quotes_settings['daily_quote'] = True

    def __asyncio_daily_analysis(self):
        if self.loop.is_closed():
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

        # Utilizza call_soon_threadsafe per pianificare __daily_quote
        if self.loop.is_running():
            self.loop.call_soon_threadsafe(asyncio.create_task, self.__daily_quote())
        else:
            self.loop.run_until_complete(self.__daily_quote())

    async def __daily_quote(self):
        pass
        # quotes = self.postgre_manager.get_last_random_quotes()
        #
        # quote = choice(quotes)
        # self.postgre_manager.update_quote_by_quote_id(quote_id=quote.quote_id, set_params={'last_random_tme': to_int(time())})
        #
        # print(f"Start sending daily quotes at {timestamp2date(time())}")
        # for user in self.quotes_users:
        #     if user.daily_quotes:
        #         quote_in_language = self.postgre_manager.get_quote_in_language(quote=quote, user=user)
        #         author = quote.author.replace('_', ' ')
        #         text = f"*Quote of the day*\n\n{quote_in_language}\n\n_{author}_"
        #
        #         chat = self.get_chat_from_telegram_id(telegram_id=user.telegram_id)
        #         message = self._build_new_telegram_message(chat=chat, text='dailyQuote')
        #         settings = {"text": text}
        #         await self.execute_command(user_x=user,
        #                                    command=message.text,
        #                                    message=message,
        #                                    chat=chat,
        #                                    initial_settings=settings)
        #
        #     await asyncio.sleep(0.2)
        # print(f"Sending daily quotes completed at {timestamp2date(time())}")

