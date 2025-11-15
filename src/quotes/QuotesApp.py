import asyncio
import pytz
from dataclasses import dataclass, field
from time import time, sleep
from datetime import datetime
from threading import Timer
from random import choice, shuffle
from typing import List, Type

from src.common.telegram_manager.TelegramUser import TelegramUser
from src.common.telegram_manager.TelegramChat import TelegramChat
from src.common.telegram_manager.TelegramMessage import TelegramMessage
from src.common.telegram_manager.telegram_manager import TelegramManager, LOGGER

from src.common.functions.Function import Function
from src.common.tools.library import to_int, timestamp2date, build_eta, safe_execute

from quotes.classes.QuotesUser import QuotesUser
from quotes.classes.QuotesPostgreManager import QuotesPostgreManager
from quotes.classes.Note import Note


@dataclass
class QuotesApp(TelegramManager):
    postgre_manager: QuotesPostgreManager = field(default=None)
    daily_quote: bool = True
    daily_book: bool = True
    name: str = "Quotes"
    loop: asyncio.AbstractEventLoop = None

    def __post_init__(self):
        super().__post_init__()
        self.quotes_users = self.postgre_manager.get_quotes_users()

        # __ print quotes users __
        print('\n####### Quotes Users ########')
        for user in self.quotes_users:
            print(f"{user.telegram_id} | {user.name} | Admin: {user.is_admin} | Daily Quotes: {user.daily_quotes} | Daily Book: {user.daily_book}")
        print('###############################################################\n')

        # __ init asyncio loop __
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

        # __ init timers __
        self.__init_quote_timer() if self.daily_quote else None
        self.__init_book_timer() if self.daily_book else None

    @property
    def app_users(self):
        return self.quotes_users

    """ ###### OVERRIDING FUNCTIONS ##### """
    def instantiate_function(self,
                             function,
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
        eta = build_eta(target_hour=9, target_minute=00)

        print('Daily Quote set in ' + str(to_int(eta/3600)) + 'h:' + str(to_int((eta % 3600)/60)) + 'm:' + str(to_int(((eta % 3600) % 60))) + 's:')

        self.quote_timer = Timer(eta, self.__asyncio_daily_quote)
        self.quote_timer.name = 'Daily Quote'
        self.quote_timer.start()
        self.daily_quote = True
        # self.quotes_settings['daily_quote'] = True

    def __asyncio_daily_quote(self):
        if self.loop.is_closed():
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

        # Utilizza call_soon_threadsafe per pianificare __daily_quote
        if self.loop.is_running():
            self.loop.call_soon_threadsafe(asyncio.create_task, self.__daily_quote())
        else:
            self.loop.run_until_complete(self.__daily_quote())

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
                safe_execute(
                    None,
                    await self.execute_command(
                        user_x=user,
                        command=message.text,
                        message=message,
                        chat=chat,
                        initial_settings=settings)
                )

            await asyncio.sleep(0.2)
        print(f"Sending daily quotes completed at {timestamp2date(time())}")

    def __init_book_timer(self):
        eta_1 = build_eta(target_hour=12, target_minute=00)
        eta_2 = build_eta(target_hour=18, target_minute=00)

        print('Next Note 1 set in ' + str(to_int(eta_1/3600)) + 'h:' + str(to_int((eta_1 % 3600)/60)) + 'm:' + str(to_int(((eta_1 % 3600) % 60))) + 's:')
        print('Next Note 2 set in ' + str(to_int(eta_2/3600)) + 'h:' + str(to_int((eta_2 % 3600)/60)) + 'm:' + str(to_int(((eta_2 % 3600) % 60))) + 's:')

        self.note_timer_eta_1 = Timer(eta_1, self.__asyncio_daily_book)
        self.note_timer_eta_1.name = 'Next Note 1'
        self.note_timer_eta_1.start()

        self.note_timer_eta_2 = Timer(eta_2, self.__asyncio_daily_book)
        self.note_timer_eta_2.name = 'Next Note 2'
        self.note_timer_eta_2.start()

    def __asyncio_daily_book(self):
        if self.loop.is_closed():
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

        # Utilizza call_soon_threadsafe per pianificare __daily_book
        if self.loop.is_running():
            self.loop.call_soon_threadsafe(asyncio.create_task, self.__daily_book())
        else:
            self.loop.run_until_complete(self.__daily_book())

    async def __daily_book(self):
        daily_notes = self.postgre_manager.get_daily_notes()
        if len(daily_notes) == 0:
            return None

        note = choice(daily_notes)
        book = note.book
        # notes: List[Note] = self.postgre_manager.get_notes_with_tags_by_book(book=book)
        # index = next((index for (index, d) in enumerate(notes) if d.note_id == note.note_id), None)
        notes_ids = self.postgre_manager.get_notes_ids_by_book(book=book)
        index = next((index for (index, d) in enumerate(notes_ids) if d == note.note_id), None)

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
                            "notes_ids": notes_ids,
                            "is_book_note": True}
                await self.execute_command(user_x=user,
                                           command=message.text,
                                           message=message,
                                           chat=chat,
                                           initial_settings=settings,
                                           initial_state=2)

            await asyncio.sleep(0.2)
        print(f"Sending daily book notes completed at {timestamp2date(time())}")

    """ ###### CLOSING APP ##### """
    def close(self):
        self.__close_quote_timer()
        self.__close_note_timer()
        self.close_telegram_manager()
        if self.loop and self.loop.is_running():
            LOGGER.debug("Shutting down event loop.")
            # Utilizza call_soon_threadsafe per eseguire __shutdown_loop
            future = asyncio.run_coroutine_threadsafe(self.__shutdown_loop(self.loop), self.loop)
            future.result()
        else:
            if self.loop:
                self.loop.close()
                LOGGER.debug("Event loop closed.")

    @staticmethod
    async def __shutdown_loop(loop):
        tasks = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task(loop)]
        LOGGER.debug(f"Shutting down {len(tasks)} tasks.")
        list(map(lambda task: task.cancel(), tasks))
        await asyncio.gather(*tasks, return_exceptions=True)
        loop.stop()

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


