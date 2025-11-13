import asyncio
from dataclasses import dataclass, field
from time import sleep
import random
# Limit concurrent HTTP calls to avoid pool exhaustion
import asyncio
import httpx

import telegram
from telegram.error import RetryAfter, TimedOut, NetworkError
from telegram.request import HTTPXRequest

from src.common.telegram_manager.TelegramUser import TelegramUser
from src.common.telegram_manager.TelegramChat import TelegramChat

# TODO: create new files in common package
from src.common.tools import print_exception, get_exception
# from src.Tools.logging_class import LoggerObj


@dataclass
class TelegramBot:
    token: str
    pending_messages: list = field(default_factory=lambda: [])
    bot: telegram.Bot = field(init=False)
    _sema: asyncio.Semaphore = field(init=False, repr=False)

    def __post_init__(self):
        # Limit parallel API calls to avoid exhausting the HTTP pool
        self._sema = asyncio.Semaphore(5)  # tune 3–10 based on your use case

        # Try the most modern constructor first (PTB ≥ 21.6) with httpx_kwargs.
        try:
            import httpx
            from telegram.request import HTTPXRequest

            limits = httpx.Limits(
                max_connections=50,  # total sockets across hosts
                max_keepalive_connections=20,  # idle keep-alive pool
            )
            timeouts = httpx.Timeout(
                connect=10.0, read=240.0, write=60.0, pool=30.0  # pool=wait for a free connection
            )

            request = HTTPXRequest(
                connection_pool_size=50,  # still useful on some PTB builds
                pool_timeout=30.0,
                read_timeout=240.0,
                write_timeout=60.0,
                connect_timeout=10.0,
                http_version="1.1",
                httpx_kwargs={
                    "limits": limits,
                    "timeout": timeouts,
                    "http2": False,
                },
            )
            self.bot = telegram.Bot(token=self.token, request=request)

        # If httpx_kwargs isn't supported (older PTB), fall back to native args only.
        except TypeError:
            request = HTTPXRequest(
                connection_pool_size=50,
                pool_timeout=30.0,
                read_timeout=240.0,
                write_timeout=60.0,
                connect_timeout=10.0,
                http_version="1.1",
            )
            self.bot = telegram.Bot(token=self.token, request=request)

        # Last resort – no HTTPXRequest tuning available; use default request.
        except Exception:
            self.bot = telegram.Bot(token=self.token)

    # __ Async primitives __
    async def __send_message(self, chat_id, text, parse_mode=None, reply_mark_up=None, silent=True):
        return await self.bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode=parse_mode,
            reply_markup=reply_mark_up,
            disable_notification=silent)

    async def __send_callback(self, callback_id, text):
        return await self.bot.answer_callback_query(callback_query_id=callback_id, text=text)

    async def __send_photo(self, chat_id, photo, caption=None, parse_mode=None, reply_mark_up=None, silent=True):
        return await self.bot.send_photo(
            chat_id=chat_id,
            photo=photo,
            caption=caption,
            parse_mode=parse_mode,
            reply_markup=reply_mark_up,
            disable_notification=silent)

    async def __send_document(self, chat_id, document):
        return await self.bot.send_document(chat_id=chat_id, document=document)

    async def __edit_message_reply_markup(self, reply_markup, chat_id=None, message_id=None, inline_message_id=None):
        return await self.bot.edit_message_reply_markup(reply_markup=reply_markup, chat_id=chat_id, message_id=message_id, inline_message_id=inline_message_id)

    async def __edit_message_text(self, text, reply_markup=None, chat_id=None, message_id=None, inline_message_id=None, parse_mode=None):
        return await self.bot.edit_message_text(text=text, reply_markup=reply_markup, chat_id=chat_id, message_id=message_id, inline_message_id=inline_message_id, parse_mode=parse_mode)

    async def __edit_message_media(self, chat_id, media, message_id=None, inline_message_id=None, reply_markup=None):
        return await self.bot.edit_message_media(chat_id=chat_id, message_id=message_id, inline_message_id=inline_message_id, media=media, reply_markup=reply_markup)

    async def __edit_message_caption(self, chat_id, caption, message_id=None, inline_message_id=None, reply_markup=None, parse_mode=None):
        return await self.bot.edit_message_caption(caption=caption, reply_markup=reply_markup, chat_id=chat_id, message_id=message_id, inline_message_id=inline_message_id, parse_mode=parse_mode)

    async def __delete_message(self, chat_id, message_id):
        return await self.bot.delete_message(chat_id=chat_id, message_id=message_id)

    # __ Private methods __
    def __build_keyboard(self, remove_keyboard=None, reply_keyboard=None, inline_keyboard=None, url=None):
        if remove_keyboard:
            return telegram.ReplyKeyboardRemove()

        if inline_keyboard:
            if type(inline_keyboard) == list:
                return telegram.InlineKeyboardMarkup(self.__build_inline_keyboard(inline_keyboard))
            elif type(inline_keyboard) == dict:
                return telegram.InlineKeyboardMarkup(self.__build_one_button_inline_keyboard(inline_keyboard))

        if reply_keyboard:
            return telegram.ReplyKeyboardMarkup(keyboard=reply_keyboard, resize_keyboard=True)

        return None

    def __build_pending_message(self, chat_id, message):
        to_add = [x for x in self.pending_messages if x['chat_id'] == chat_id]
        self.pending_messages = [x for x in self.pending_messages if x['chat_id'] != chat_id]

        pending_text = ''
        for row in to_add:
            pending_text += row['text'] + '\n\n'

        return pending_text + message

    def __safe_execute(self, function, **args):
        try:
            return function(**args)
        except:
            return self.__log_exception()

    @staticmethod
    def __build_inline_keyboard(keyboard):
        return [[telegram.InlineKeyboardButton(text=x, callback_data=x) for x in row] for row in keyboard]

    @staticmethod
    def __build_one_button_inline_keyboard(keyboard):
        return [[telegram.InlineKeyboardButton(text=keyboard['text'], url=keyboard['url'])]]

    @staticmethod
    def __log_exception():
        LOGGER.error(get_exception())
        return None

    # __ Public get updates __
    async def get_updates(self, offset: int):
        return await self.bot.get_updates(offset=offset)

    # __ Robust retry wrapper __
    # Generic retry helper for Telegram API calls with exponential backoff and 429 handling
    async def _with_retries(self, coro_factory, max_tries: int = 7):
        attempt = 0
        backoff = 1.2
        while True:
            try:
                return await coro_factory()
            except RetryAfter as e:
                # Respect Telegram's rate limit
                delay = int(getattr(e, "retry_after", 5)) + 1
                await asyncio.sleep(delay)
            except (TimedOut, NetworkError) as e:
                attempt += 1
                if attempt >= max_tries:
                    raise
                # jittered backoff
                delay = backoff * (1 + random.random())
                await asyncio.sleep(delay)
                backoff *= 1.7
            except telegram.error.Forbidden:
                # Non-retryable: user blocked the bot or similar
                raise

    # __ Public send methods __
    async def send_message(self, chat_id, text, parse_mode=None, reply_keyboard=None, remove_keyboard=False, inline_keyboard=None, silent=True, pending=False):
        if len(text) == 0:
            return None

        if pending:
            self.pending_messages.append({'chat_id': chat_id, 'text': text})
            return None

        reply_mark_up = self.__build_keyboard(remove_keyboard=remove_keyboard, reply_keyboard=reply_keyboard, inline_keyboard=inline_keyboard)

        if len(self.pending_messages) > 0 and any(x for x in self.pending_messages if x['chat_id'] == chat_id):
            text = self.__build_pending_message(chat_id=chat_id, message=text)

        if len(text) > 4096:
            # from io import BytesIO
            # buf = BytesIO(text.encode("utf-8"))
            # buf.name = "riepilogo_autoscout24.txt"
            # await self.send_document(chat_id, buf)

            return await self.split_and_send_message(
                text=text,
                chat_id=chat_id,
                parse_mode=None,  # <— forza plain text per i chunk
                reply_mark_up=reply_mark_up,
                silent=silent
            )

        async with self._sema:
            return await self._with_retries(lambda: self.__send_message(
                chat_id=chat_id, text=text, parse_mode=parse_mode,
                reply_mark_up=reply_mark_up, silent=silent
            ))

    async def split_and_send_message(self, text, chat_id, parse_mode, reply_mark_up, silent):
        # Split on safe boundaries (newline > space) to avoid cutting words/entities
        parts = []
        start = 0
        MAX = 4096
        while start < len(text):
            end = min(len(text), start + MAX)

            # try to cut at newline first, then at space; otherwise hard cut
            cut = text.rfind("\n", start, end)
            if cut == -1:
                cut = text.rfind(" ", start, end)
            if cut == -1 or cut <= start:
                cut = end

            parts.append(text[start:cut])
            start = cut

        # Send sequentially to preserve order and reduce peak concurrency
        results = []
        for chunk in parts:
            res = await self._with_retries(lambda: self.__send_message(
                chat_id=chat_id,
                text=chunk,
                parse_mode=parse_mode,  # None for chunks → no Markdown parsing errors
                reply_mark_up=reply_mark_up,
                silent=silent
            ))
            results.append(res)
        return results

    async def send_pending_message(self, last_chat_id, silent=True):
        if not self.pending_messages or not any(x for x in self.pending_messages if x['chat_id'] == last_chat_id):
            return None
        text = self.__build_pending_message(chat_id=last_chat_id, message='')

        async with self._sema:
            return await self._with_retries(lambda: self.__send_message(
                chat_id=last_chat_id, text=text, silent=silent
            ))

    async def send_callback(self, callback_id, text=None):
        async with self._sema:
            return await self._with_retries(lambda: self.__send_callback(callback_id=callback_id, text=text))

    async def send_photo(self, chat_id, photo, caption=None, parse_mode=None, reply_keyboard=None, remove_keyboard=False, inline_keyboard=None, silent=True):
        reply_mark_up = self.__build_keyboard(remove_keyboard=remove_keyboard, reply_keyboard=reply_keyboard, inline_keyboard=inline_keyboard)
        async with self._sema:
            return await self._with_retries(lambda: self.__send_photo(
                chat_id=chat_id,
                photo=photo,
                caption=caption,
                parse_mode=parse_mode,
                reply_mark_up=reply_mark_up,
                silent=silent))

    async def send_document(self, chat_id, document):
        async with self._sema:
            return await self._with_retries(lambda: self.__send_document(chat_id=chat_id, document=document))

    # __ Public edit methods __
    async def edit_inline_keyboard(self, reply_markup, chat_id=None, message_id=None, inline_message_id=None):
        reply_markup = self.__build_keyboard(inline_keyboard=reply_markup) if reply_markup else None
        async with self._sema:
            return await self._with_retries(lambda: self.__edit_message_reply_markup(reply_markup=reply_markup, chat_id=chat_id, message_id=message_id, inline_message_id=inline_message_id))

    async def edit_message(self, text, reply_markup=None, chat_id=None, message_id=None, inline_message_id=None, pending=False, parse_mode=None):
        if not text:
            return None
        if pending:
            self.pending_messages.append({'chat_id': chat_id, 'text': text})
            return None
        if self.pending_messages and any(x for x in self.pending_messages if x['chat_id'] == chat_id):
            text = self.__build_pending_message(chat_id=chat_id, message=text)
        reply_markup = self.__build_keyboard(inline_keyboard=reply_markup) if reply_markup else None
        if len(text) > 4096:
            return await self.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode)

        async with self._sema:
            return await self._with_retries(lambda: self.__edit_message_text(
                text=text,
                reply_markup=reply_markup,
                chat_id=chat_id,
                message_id=message_id,
                inline_message_id=inline_message_id,
                parse_mode=parse_mode))

    async def edit_photo(self, photo=None, caption=None, reply_markup=None, chat_id=None, message_id=None, inline_message_id=None, parse_mode=None):
        if not photo and not caption:
            return None
        response = None
        reply_markup = self.__build_keyboard(inline_keyboard=reply_markup) if reply_markup else None
        if photo:
            media = telegram.InputMediaPhoto(media=photo)
            response = await self._with_retries(lambda: self.__edit_message_media(chat_id=chat_id, message_id=message_id, inline_message_id=inline_message_id, media=media, reply_mark_up=reply_markup))
            # response is a Message; keep message_id for next call if needed
            message_id = getattr(response, "message_id", message_id)
        if caption:
            response = await self._with_retries(lambda: self.__edit_message_caption(chat_id=chat_id, caption=caption, message_id=message_id, inline_message_id=inline_message_id, reply_markup=reply_markup, parse_mode=parse_mode))
        return response

    async def delete_message(self, chat_id, message_id):
        async with self._sema:
            return await self._with_retries(lambda: self.__delete_message(chat_id=chat_id, message_id=message_id))


if __name__ == '__main__':
    import logging
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    from logging.handlers import TimedRotatingFileHandler
    from pathlib import Path
    from logger_tt import setup_logging
    from time import sleep

    # config_path = Path(__file__).parent.parent.parent.parent.joinpath('resources', 'logger.conf.yaml')
    # log_conf = setup_logging(config_path=str(config_path), use_multiprocessing=True)
    # LOGGER = logging.getLogger()

    token = "521932309:AAEBJrGFDznMH1GEiH5veKRR3p787_hV_2w"
    users = [TelegramUser(telegram_id=19838246, name='Vale', username='vales2', is_admin=True)]
    chats = [TelegramChat(chat_id=19838246, type='private', username='vales2', first_name='Vale', last_name='S')]
    telegram_bot = TelegramBot(token=token)
    asyncio.run(telegram_bot.send_message(chat_id=19838246, text='ciao', inline_keyboard=[['<', '>']]))

    async def poll_for_updates():
        i = 0
        offset = 0
        while i < 30:
            updates = await telegram_bot.get_updates(offset=offset + 1)
            for update in updates:
                print(update.message)
            if len(updates) > 0:
                offset = updates[-1].update_id
            i += 1

    asyncio.run(poll_for_updates())

    print('end')



""" New Chat
{'update_id': 879617908, 
 'message': {'message_id': 9265, 
             'date': 1679134499, 
             'chat': {'id': -1001664351197, 'type': 'supergroup', 'title': 'No Time For Canarie'}, 
             'entities': [], 'caption_entities': [], 'photo': [], 
             'new_chat_members': [{'id': 5712307917, 'first_name': 'No Time for Canarie', 'is_bot': True, 'username': 'no_time_for_canarie_bot'}], 
             'new_chat_photo': [], 
             'delete_chat_photo': False, 
             'group_chat_created': False, 
             'supergroup_chat_created': False, 
             'channel_chat_created': False, 
             'from': {'id': 19838246, 'first_name': 'Vale', 'is_bot': False, 'username': 'vales2', 'language_code': 'it'}}}
"""

"""
{'update_id': 879617909,
 'message': {'message_id': 18, 
             'date': 1679134658, 
             'chat': {'id': 21397600, 'type': 'private', 'username': 'ziolomu', 'first_name': 'Andrea', 'last_name': 'L'}, 
             'text': '/start', 'entities': [{'type': 'bot_command', 'offset': 0, 'length': 6}], 
             'caption_entities': [], 'photo': [], 'new_chat_members': [], 'new_chat_photo': [], 
             'delete_chat_photo': False, 'group_chat_created': False, 'supergroup_chat_created': False, 'channel_chat_created': False,
             'from': {'id': 21397600, 'first_name': 'Andrea', 'is_bot': False, 'last_name': 'L', 'username': 'ziolomu', 'language_code': 'it'}}}
             """

"""
{'update_id': 879617910, 
 'message': {'message_id': 19, 'date': 1679134685, 
             'chat': {'id': 60481756, 'type': 'private', 'username': 'Matte091', 'first_name': 'Matteo', 'last_name': 'Pacifico'}, 
             'text': '/start', 'entities': [{'type': 'bot_command', 'offset': 0, 'length': 6}], 
             'caption_entities': [], 'photo': [], 'new_chat_members': [], 'new_chat_photo': [], 
             'delete_chat_photo': False, 'group_chat_created': False, 'supergroup_chat_created': False, 
             'channel_chat_created': False, 
             'from': {'id': 60481756, 'first_name': 'Matteo', 'is_bot': False, 'last_name': 'Pacifico', 'username': 'Matte091', 'language_code': 'it'}}}
"""

"""{'update_id': 879617910, 
 'message': {'message_id': 19, 
             'date': 1679134685, 
             'chat': {'id': 60481756, 'type': 'private', 'username': 'Matte091', 'first_name': 'Matteo', 'last_name': 'Pacifico'}, 
             'text': '/start', 
             'entities': [{'type': 'bot_command', 'offset': 0, 'length': 6}], 
             'caption_entities': [], 
             'photo': [], 
             'new_chat_members': [], 
             'new_chat_photo': [], 
             'delete_chat_photo': False, 
             'group_chat_created': False, 
             'supergroup_chat_created': False, 
             'channel_chat_created': False, 
             'from': {'id': 60481756, 'first_name': 'Matteo', 'is_bot': False, 'last_name': 'Pacifico', 'username': 'Matte091', 'language_code': 'it'}}}"""

"""
{'update_id': 879617911, 
 'message': {'message_id': 9275, 
             'date': 1679135126, 
             'chat': {'id': -1001664351197, 'type': 'supergroup', 'title': 'No Time For Canarie'}, 
             'text': '/start', 
             'entities': [{'type': 'bot_command', 'offset': 0, 'length': 6}], 
             'caption_entities': [], 
             'photo': [], 
             'new_chat_members': [], 
             'new_chat_photo': [], 
             'delete_chat_photo': False, 
             'group_chat_created': False, 
             'supergroup_chat_created': False, 
             'channel_chat_created': False, 
             'from': {'id': 19838246, 'first_name': 'Vale', 'is_bot': False, 'username': 'vales2', 'language_code': 'it'}}}
"""
