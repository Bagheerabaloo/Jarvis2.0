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

from src.common.functions.Function import Function
from src.common.functions.FunctionCiao import FunctionCiao
from src.common.functions.FunctionCallback import FunctionCallback
from src.common.functions.FunctionProcess import FunctionProcess
from src.common.functions.FunctionStart import FunctionStart
from src.common.functions.FunctionHelp import FunctionHelp
from src.common.functions.FunctionTotalDBRows import FunctionTotalDBRows

from src.quotes.functions.FunctionBack import FunctionBack
from src.quotes.functions.FunctionQuotesNewUser import FunctionQuotesNewUser
from src.quotes.functions.FunctionRandomQuote import FunctionRandomQuote
from src.quotes.functions.FunctionShowQuotes import FunctionShowQuotes
from src.quotes.functions.FunctionNewQuote import FunctionNewQuote
from src.quotes.functions.FunctionNewNote import FunctionNewNote
from src.quotes.functions.FunctionShowNotes import FunctionShowNotes
from src.quotes.functions.FunctionDailyQuote import FunctionDailyQuote
from src.quotes.functions.FunctionQuotesSettings import FunctionQuotesSettings
from src.quotes.functions.FunctionBook import FunctionBook

from src.common.file_manager.FileManager import FileManager
from src.common.postgre.PostgreManager import PostgreManager
from src.common.tools.library import run_main, get_environ, int_timestamp_now, class_from_args, to_int, timestamp2date, build_eta
from quotes import QuotesUser
from quotes import QuotesPostgreManager
from quotes import Note

# import yaml
# from src.quotes.functions import FunctionBack, FunctionQuotesNewUser, FunctionRandomQuote, FunctionShowQuotes, FunctionNewQuote, FunctionNewNote, FunctionShowNotes, FunctionDailyQuote, FunctionDailyBook, FunctionQuotesSettings
# from src.common.functions import FunctionCiao, FunctionCallback, FunctionProcess, FunctionStart, FunctionHelp

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
class Quotes(TelegramManager):
    postgre_manager: QuotesPostgreManager = field(default=None)
    daily_quote: bool = True
    daily_book: bool = True
    name: str = "Quotes"

    def __post_init__(self):
        super().__post_init__()
        self.quotes_users = self.postgre_manager.get_quotes_users()
        self.__init_quote_timer() if self.daily_quote else None
        self.__init_book_timer() if self.daily_book else None

    @property
    def app_users(self):
        return self.quotes_users

    """ ###### OVERRIDING FUNCTIONS ##### """
    def instantiate_function(self, function,
                             chat: TelegramChat,
                             message: TelegramMessage,
                             is_new: bool,
                             function_id: int,
                             user_x: TelegramUser) -> Function:
        # print(isinstance(function.__class__.__name__, Quotes))
        kwargs = {"bot": self.telegram_bot,
                  "chat": chat,
                  "message": message,
                  "function_id": function_id,
                  "is_new": is_new,
                  "postgre_manager": self.postgre_manager}
        if "quotes_user" in dir(function):
            kwargs.update({"quotes_user": [x for x in self.quotes_users if x.telegram_id == user_x.telegram_id][0]})

        return function(**kwargs)

    async def call_message(self,
                           user_x: QuotesUser,
                           message: TelegramMessage,
                           chat: TelegramChat,
                           txt: str):
        if '\nby ' in message.text:
            quote, author = self.handle_new_quote(text=message.text)
            settings = {"quote": quote,
                        "author": author}
            return await self.execute_command(user_x=user_x,
                                              command="newQuote",
                                              message=message,
                                              chat=chat,
                                              initial_settings=settings,
                                              initial_state=4)

        elif user_x.is_admin:
            settings = {"note": message.text}
            return await self.execute_command(user_x=user_x,
                                              command="note",
                                              message=message,
                                              chat=chat,
                                              initial_settings=settings)

        await self.telegram_bot.send_message(chat_id=message.chat_id, text='No command running')
        return False

    async def handle_app_new_user(self,
                                  admin_user: QuotesUser,
                                  admin_chat: TelegramChat,
                                  new_telegram_user: TelegramUser,
                                  new_telegram_chat: TelegramChat):

        message = self._build_new_telegram_message(chat=admin_chat, text='appNewUser')
        admin_chat.new_message(telegram_message=message)
        settings = {"new_user": new_telegram_user,
                    "new_chat": new_telegram_chat,
                    "app": self.name}
        return await self.execute_command(user_x=admin_user,
                                          command=message.text,
                                          message=message,
                                          chat=admin_chat,
                                          initial_settings=settings)

    def app_update_users(self):
        print('\n### refreshing quotes users ###\n')
        all_users = self.postgre_manager.get_quotes_users()
        diff_users = [x for x in all_users if x.telegram_id not in [x.telegram_id for x in self.app_users]]
        print(diff_users)
        self.quotes_users += diff_users

    @staticmethod
    def handle_new_quote(text: str) -> (str, str):
        txt = text.split('by ')
        author = txt.pop().replace('_', ' ')

        txt = 'by '.join(txt) if len(txt) > 1 else txt[0]

        if 'Forwarded' in txt:
            txt = txt.split('\n')
            txt.pop(0)
            quote = '\n'.join(txt)
        else:
            quote = txt

        while quote[-1] == '\n':
            quote = quote[:-1]

        return quote, author

    """ ###### ROUTINES ##### """
    def __init_quote_timer(self):
        dt = datetime.now(pytz.timezone('Europe/Rome'))
        eta = ((9 - dt.hour - 1) * 60 * 60) + ((60 - dt.minute - 1) * 60) + (60 - dt.second)

        if eta < 0:
            eta += 24*60*60

        print('Daily Quote set in ' + str(to_int(eta/3600)) + 'h:' + str(to_int((eta % 3600)/60)) + 'm:' + str(to_int(((eta % 3600) % 60))) + 's:')

        self.quote_timer = Timer(eta, self.__asyncio_daily_quote)
        self.quote_timer.name = 'Daily Quote'
        self.quote_timer.start()
        self.daily_quote = True
        # self.quotes_settings['daily_quote'] = True

    def __asyncio_daily_quote(self):
        asyncio.run(self.__daily_quote())

    async def __daily_quote(self):
        quotes = self.postgre_manager.get_last_random_quotes()

        quote = choice(quotes)
        self.postgre_manager.update_quote_by_quote_id(quote_id=quote.quote_id, set_params={'last_random_tme': to_int(time())})

        print(f"Start sending daily quotes at {timestamp2date(time())}")
        for user in self.quotes_users:
            if user.daily_quotes:
                quote_in_language = self.postgre_manager.get_quote_in_language(quote=quote, user=user)
                author = quote.author.replace('_', ' ')
                text = f"*Quote of the day*\n\n{quote_in_language}\n\n_{author}_"

                chat = self.get_chat_from_telegram_id(telegram_id=user.telegram_id)
                message = self._build_new_telegram_message(chat=chat, text='dailyQuote')
                settings = {"text": text}
                await self.execute_command(user_x=user,
                                           command=message.text,
                                           message=message,
                                           chat=chat,
                                           initial_settings=settings)

            sleep(0.2)
        print(f"Sending daily quotes completed at {timestamp2date(time())}")

    def __init_book_timer(self):
        eta_1 = build_eta(target_hour=12, target_minute=00)
        eta_2 = build_eta(target_hour=18, target_minute=00)
        # eta_2 = 20

        print('Next Note 1 set in ' + str(to_int(eta_1/3600)) + 'h:' + str(to_int((eta_1 % 3600)/60)) + 'm:' + str(to_int(((eta_1 % 3600) % 60))) + 's:')
        print('Next Note 2 set in ' + str(to_int(eta_2/3600)) + 'h:' + str(to_int((eta_2 % 3600)/60)) + 'm:' + str(to_int(((eta_2 % 3600) % 60))) + 's:')

        self.note_timer_eta_1 = Timer(eta_1, self.__asyncio_daily_book)
        self.note_timer_eta_1.name = 'Next Note 1'
        self.note_timer_eta_1.start()

        self.note_timer_eta_2 = Timer(eta_2, self.__asyncio_daily_book)
        self.note_timer_eta_2.name = 'Next Note 2'
        self.note_timer_eta_2.start()

    def __asyncio_daily_book(self):
        asyncio.run(self.__daily_book())

    async def __daily_book(self):
        notes = self.postgre_manager.get_daily_notes()
        if len(notes) == 0:
            return None

        note = choice(notes)
        book = note.book
        notes: List[Note] = self.postgre_manager.get_notes_with_tags_by_book(book=book)
        index = next((index for (index, d) in enumerate(notes) if d.note_id == note.note_id), None)
        self.postgre_manager.update_note_by_note_id(note_id=note.note_id, set_params={'last_random_time': to_int(time())})

        print(f"Start sending daily book notes at {timestamp2date(time())}")
        for user in self.quotes_users:
            if user.daily_book:
                function = await self.get_function_by_alias(alias='showNotes', chat_id=user.telegram_id, user_x=user)
                if not function:
                    continue
                chat = self.get_chat_from_telegram_id(telegram_id=user.telegram_id)
                message = self._build_new_telegram_message(chat=chat, text='showNotes')
                settings = {"note": note.note,
                            "book": note.book,
                            "index": index,
                            "notes": notes,
                            "is_book_note": True}
                await self.execute_command(user_x=user,
                                           command=message.text,
                                           message=message,
                                           chat=chat,
                                           initial_settings=settings,
                                           initial_state=2)

            sleep(0.2)
        print(f"Sending daily book notes completed at {timestamp2date(time())}")

    """ ###### CLOSING APP ##### """
    def close(self):
        self.__close_quote_timer()
        self.__close_note_timer()
        self.close_telegram_manager()

    def __close_quote_timer(self):
        try:
            self.quote_timer.cancel()
        except:
            print('unable to close quote timer')
            pass

    def __close_note_timer(self):
        try:
            self.note_timer_eta_1.cancel()
        except:
            print('unable to close note timer eta 1')
            pass
        try:
            self.note_timer_eta_2.cancel()
        except:
            print('unable to close note timer eta 2')
            pass


def main():

    # __ whether it's running on Heroku or local
    os_environ = get_environ() == 'HEROKU'

    # __ determine ssl mode depending on environ configurations __
    ssl_mode = 'require' if os_environ else 'disable'

    # __ init file manager __
    config_manager = FileManager()

    # __ get telegram token __
    token = config_manager.get_telegram_token()
    # postgre_url = config_manager.get_postgre_url(database_key='POSTGRE_URL_LOCAL_DOCKER')
    postgre_url = config_manager.get_postgre_url()
    admin_info = config_manager.get_admin()
    admin_chat = config_manager.get_admin_chat()

    postgre_manager = QuotesPostgreManager(db_url=postgre_url, delete_permission=True)
    if not postgre_manager.connect(sslmode=ssl_mode):
        # logger.warning("PostgreDB connection not established: cannot connect")
        return

    admin_user = TelegramUser(telegram_id=admin_info["chat"],
                              name=admin_info["name"],
                              username=admin_info["username"],
                              is_admin=True)
    admin_chat = TelegramChat(chat_id=admin_chat['chat_id'],
                              type=admin_chat["type"],
                              username=admin_chat["username"],
                              first_name=admin_chat["first_name"],
                              last_name=admin_chat["last_name"])
    telegram_users = postgre_manager.get_telegram_users(admin_user=admin_user)
    telegram_chats = postgre_manager.get_telegram_chats_from_db(admin_chat=admin_chat)

    commands = [    # TODO: move to YAML file
        Command(alias=["back"], admin=False, function=FunctionBack),
        Command(alias=["help"], admin=False, function=FunctionHelp),
        Command(alias=["ciao", "hello"], admin=True, function=FunctionCiao),
        Command(alias=["callback"], admin=True, function=FunctionCallback),
        Command(alias=["process"], admin=True, function=FunctionProcess),
        Command(alias=["rows"], admin=True, function=FunctionTotalDBRows),
        Command(alias=["start"], admin=False, function=FunctionStart),
        Command(alias=["quote"], admin=False, function=FunctionRandomQuote),
        Command(alias=["showQuotes"], admin=False, function=FunctionShowQuotes),
        Command(alias=["newQuote"], admin=True, function=FunctionNewQuote),
        Command(alias=["note"], admin=True, function=FunctionNewNote),
        Command(alias=["showNotes"], admin=False, function=FunctionShowNotes),
        Command(alias=["settings"], admin=False, function=FunctionQuotesSettings),
        Command(alias=["book"], admin=False, function=FunctionBook),
        Command(alias=["appNewUser"], admin=True, function=FunctionQuotesNewUser, restricted=True),
        Command(alias=["dailyQuote"], admin=False, function=FunctionDailyQuote, restricted=True),
    ]

    quotes = Quotes(token=token,
                    users=telegram_users,
                    chats=telegram_chats,
                    commands=commands,
                    postgre_manager=postgre_manager)
    quotes.start()
    run_main(app=quotes)


if __name__ == '__main__':
    main()

