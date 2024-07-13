import telegram
from dataclasses import dataclass, field
from typing import List, Type, Optional
from queue import Queue
from time import time, sleep
from threading import Thread
import logging
import asyncio

from common.telegram_manager.Command import Command
from common.telegram_manager.TelegramBot import TelegramBot
from common.telegram_manager.TelegramChat import TelegramChat
from common.telegram_manager.TelegramMessage import TelegramMessage
from common.telegram_manager.TelegramMessageType import TelegramMessageType
from common.telegram_manager.TelegramUser import TelegramUser

from common.functions.Function import Function
from common.functions.FunctionSendCallback import FunctionSendCallback
from common.functions.FunctionStart import FunctionStart
from common.postgre.PostgreManager import PostgreManager

# TODO: create new files in common package
from common.tools import print_exception, get_exception, int_timestamp_now
# from src.Tools.logging_class import LoggerObj
LOGGER = logging.getLogger(__name__)


@dataclass
class TelegramManager:
    token: str
    users: List[TelegramUser]
    chats: List[TelegramChat]
    commands: list[Command]
    postgre_manager: PostgreManager = field(default=None)

    telegram_bot: TelegramBot = field(init=False)
    run: bool = True
    receive_messages: bool = True
    handle_messages: bool = True
    user_requests: list = field(default_factory=lambda: [])
    update_stream: Queue = Queue()  # Queue: updates from Telegram are put in this queue
    name: str = ''
    next_id: int = 0

    # ext_logger = None
    # logger_level = "DEBUG"
    # logging_queue = None

    def __post_init__(self):
        self.telegram_bot = TelegramBot(token=self.token)
        # __ init logging __
        # self.logger = self.__init_logger(self.logger_level, self.logging_queue) if not self.ext_logger else self.ext_logger

    @property
    def app_users(self):
        return self.users

    def start(self):
        # __ delete old telegram functions from db __
        self.postgre_manager.delete_old_telegram_functions()

        # __ restore telegram functions from db__
        for chat in self.chats:
            chat.running_functions = self.postgre_manager.get_telegram_functions(chat_id=chat.chat_id)

        # __ start telegram polling thread __
        self.start_polling_thread() if self.receive_messages else None

        # __ start messages handler thread __
        self.start_main_thread() if self.handle_messages else None

    """ ########### Close ############ """
    def close_telegram_manager(self):
        self.run = False
        self.save()
        self.postgre_manager.close_connection()

    def save(self) -> bool:
        success = True
        telegram_functions_ids = self.postgre_manager.get_telegram_functions_ids()
        for chat in self.chats:
            for telegram_function in chat.running_functions:
                if telegram_function.id in telegram_functions_ids:
                    # TODO: update only if update_id has changed
                    success &= self.postgre_manager.update_telegram_function(
                        telegram_function=telegram_function,
                        commit=True)
                else:
                    success &= self.postgre_manager.insert_telegram_function(
                        telegram_function=telegram_function,
                        chat_id=chat.chat_id, commit=True)
        return success

    def close(self):
        self.close_telegram_manager()

    """ ########### Thread: Get Updates ############ """
    def start_polling_thread(self):
        Thread(target=self.__updates_thread, name=f'{self.name}TelegramPollThread').start()

    async def __updates_thread_asyncio(self):
        while self.run:
            await self.__poll_for_updates()
            sleep(0.01)

        # Read updates with next_id in order to avoid to read "end" again in the following run
        try:
            await self.telegram_bot.get_updates(offset=self.next_id)
        except:
            pass

    def __updates_thread(self):
        asyncio.run(self.__updates_thread_asyncio())

    async def __poll_for_updates(self):
        try:
            try:
                updates = await self.telegram_bot.get_updates(offset=self.next_id)
            # except telegram.TelegramError:
            except:
                LOGGER.warning(get_exception())
                updates = []

            for update in updates:
                self.__handle_update(update=update.to_dict())
        except:
            LOGGER.warning(get_exception())

    def __handle_update(self, update):
        self.next_id = update["update_id"] + 1

        # __ handle a command/message __
        if 'message' in update:
            self.__handle_message(update=update)

        # __ handle a callback __
        elif 'callback_query' in update:
            self.__handle_callback(update=update)

    def __handle_message(self, update):
        message = update['message']
        text = message['text']
        chat_id = message["chat"]["id"]
        is_command = message['text'][0] == '/'
        text = text[1:] if is_command else text

        if text.upper() in {'TEST_TELEGRAM', 'TEST TELEGRAM', 'TESTTELEGRAM'}:
            return self.telegram_bot.send_message(chat_id=chat_id, text='TELEGRAM IS WORKING')

        return self.update_stream.put(
            TelegramMessage(message_type=TelegramMessageType.COMMAND if is_command else TelegramMessageType.MESSAGE,
                            chat_id=chat_id,
                            message_id=message["message_id"] if 'message_id' in message else None,
                            date=message['date'],
                            update_id=update['update_id'],
                            from_id=message["from"]["id"],
                            from_name=message["from"]["first_name"] if 'from' in message and 'first_name' in message['from'] else None,
                            from_username=message["from"]["username"] if 'from' in message and 'username' in message['from'] else None,
                            chat_last_name=message["chat"]["last_name"] if "chat" in message and "last_name" in message["chat"] else None,
                            text=text))

    def __handle_callback(self, update):
        callback = update['callback_query']
        return self.update_stream.put(
            TelegramMessage(message_type=TelegramMessageType.CALLBACK,
                            chat_id=callback['message']["chat"]["id"] if 'message' in callback and 'chat' in callback['message'] else None,
                            message_id=callback['message']["message_id"] if 'message' in callback and 'message_id' in callback['message'] else None,
                            date=callback['message']['date'] if 'message' in callback and 'date' in callback['message'] else None,
                            update_id=update['update_id'],
                            from_id=callback["from"]["id"],
                            from_name=callback["from"]["first_name"] if 'from' in callback and 'first_name' in callback['from'] else None,
                            from_username=callback["from"]["username"] if 'from' in callback and 'username' in callback['from'] else None,
                            chat_last_name=callback["chat"]["last_name"] if "chat" in callback and "last_name" in callback["chat"] else None,
                            callback_id=callback['id'],
                            data=callback['data'] if 'data' in callback else None))

    """ ########### Thread: Messages Handler ############ """
    def start_main_thread(self):
        self.run = True
        Thread(target=lambda: asyncio.run(self.main_thread()), name=f'{self.name}TgmMngThr').start()

    async def main_thread(self):
        """this function is the main thread that handles the messages received from the telegram bot"""

        while self.run:
            if not self.update_stream.empty():
                telegram_message = self.update_stream.get_nowait()
                print(telegram_message)  # TODO: delete this line
                if telegram_message.message_type == TelegramMessageType.COMMAND and telegram_message.text.upper() == 'END':
                    self.close()
                else:
                    await self.__handle_event(telegram_message=telegram_message)
            sleep(0.05)

    async def __handle_event(self, telegram_message: TelegramMessage):
        """this function is called when a new message is received from the telegram bot"""

        # __ identify the user who sent the update __
        user_x = [x for x in self.app_users if x.telegram_id == telegram_message.from_id]

        # __ handle command start from new users or existing users in other apps __
        if telegram_message.text == 'start':
            return await self.__handle_command_start(message=telegram_message, user_x=user_x)

        # __ handle commands different from start from new users (do nothing) __
        if len(user_x) == 0:
            return None

        # __ get user __
        user_x = user_x[0]

        # __ get chat __
        telegram_chats = [x for x in self.chats if x.chat_id == telegram_message.chat_id]
        if len(telegram_chats) == 0:
            return LOGGER.error('CHAT NOT FOUND')
        telegram_chat = telegram_chats[0]
        telegram_chat.new_message(telegram_message=telegram_message)
        # TODO: handle functions that are restricted -> they can't be called from here
        try:
            if telegram_message.message_type == TelegramMessageType.COMMAND:
                command = telegram_message.text.strip('/')
                await self.execute_command(user_x=user_x, command=command, message=telegram_message, chat=telegram_chat)
            elif telegram_message.message_type == TelegramMessageType.MESSAGE:
                await self.__message(user_x=user_x, message=telegram_message, chat=telegram_chat)
            elif telegram_message.message_type == TelegramMessageType.CALLBACK:
                await self.__callback(user_x=user_x, message=telegram_message, chat=telegram_chat)
        except:
            LOGGER.error(get_exception())
            # user_x.reset()

    async def get_function_by_alias(self, alias: str, chat_id: int, user_x: TelegramUser) -> Optional[Type[Function]]:
        functions = self.get_functions_by_alias(alias=alias, user_x=user_x)
        if len(functions) == 0:
            await self.telegram_bot.send_message(chat_id=chat_id, text="Function not implemented or forbidden")
            return None
        elif len(functions) > 1:
            await self.telegram_bot.send_message(chat_id=chat_id, text="Warning: more than one function associated to this alias")
            return None
        return functions[0]

    def get_functions_by_alias(self, alias: str, user_x: TelegramUser) -> List[Type[Function]]:
        # Command(alias=["ciao", "hello"], admin=True, function=FunctionCiao, needs_postgre=True),
        return [x.function for x in self.commands if alias.lower() in [y.lower() for y in x.alias] and (not x.admin or user_x.is_admin)]

    def get_not_admin_functions_by_alias(self, alias: str) -> List[Type[Function]]:
        # Command(alias=["ciao", "hello"], admin=True, function=FunctionCiao, needs_postgre=True),
        return [x.function for x in self.commands if alias.lower() in [y.lower() for y in x.alias] and not x.admin]

    def get_function_by_name(self, name: str, user_x: TelegramUser) -> List[Type[Function]]:
        # Command(alias=["ciao", "hello"], admin=True, function=FunctionCiao),
        # TODO: add restricted constraint
        return [x.function for x in self.commands if name == x.function.name and (not x.admin or user_x.is_admin)]

    async def execute_command(self,
                              user_x: TelegramUser,
                              command: str,
                              message: TelegramMessage,
                              chat: TelegramChat,
                              initial_settings: dict = None,
                              initial_state: int = 1):  # TODO: add type of output
        function = await self.get_function_by_alias(alias=command, chat_id=message.chat_id, user_x=user_x)
        if not function:
            return False
        return await self.run_new_function(function=function,
                                           user_x=user_x,
                                           chat=chat,
                                           message=message,
                                           initial_settings=initial_settings,
                                           initial_state=initial_state)
        # FunctionFactory.get_function(function_type=function_type[0], bot=self.telegram_bot, chat=chat, message=message, function_id=message.message_id, is_new=True)

    async def run_new_function(self,
                               function,
                               user_x: TelegramUser,
                               chat: TelegramChat,
                               message: TelegramMessage,
                               initial_settings: dict = None,
                               initial_state: int = 1):

        initialized_function = self.instantiate_function(function=function,
                                                         chat=chat,
                                                         message=message,
                                                         is_new=True,
                                                         function_id=message.message_id,
                                                         user_x=user_x)

        await self.__execute_function(function=initialized_function,
                                      user_x=user_x,
                                      initial_settings=initial_settings,
                                      initial_state=initial_state)
        return initialized_function

    async def __message(self, user_x: TelegramUser, message: TelegramMessage, chat: TelegramChat):
        telegram_function = chat.get_function_open_for_message()
        if telegram_function:
            functions: List[Type[Function]] = self.get_function_by_name(name=telegram_function.name, user_x=user_x)
            function = functions[0]
            return await self.run_existing_function(function=function,
                                                    function_id=telegram_function.id,
                                                    user_x=user_x,
                                                    chat=chat,
                                                    message=message)

        functions = self.get_functions_by_alias(alias=message.text.strip('/'), user_x=user_x)
        if functions:
            return await self.run_new_function(function=functions[0],
                                               user_x=user_x,
                                               chat=chat,
                                               message=message)
        else:
            await self.call_message(user_x=user_x, message=message, chat=chat, txt='')

    async def run_existing_function(self,
                                    function,
                                    function_id: int,
                                    user_x: TelegramUser,
                                    chat: TelegramChat,
                                    message: TelegramMessage):
        initialized_function = self.instantiate_function(function=function,
                                                         chat=chat,
                                                         message=message,
                                                         is_new=False,
                                                         function_id=function_id,
                                                         user_x=user_x)
        await self.__execute_function(function=initialized_function, user_x=user_x)
        return initialized_function

    async def __callback(self, user_x: TelegramUser, message: TelegramMessage, chat: TelegramChat):
        telegram_function = chat.get_function_by_callback_message_id(callback_message_id=message.message_id)
        if telegram_function:
            functions: List[Type[Function]] = self.get_function_by_name(name=telegram_function.name, user_x=user_x)
            function = functions[0]
            return await self.run_existing_function(function=function,
                                                    function_id=telegram_function.id,
                                                    user_x=user_x,
                                                    chat=chat,
                                                    message=message)
        else:
            function = FunctionSendCallback  # TODO: handle with functions run_existing_function and run_new_function
            fun = self.instantiate_function(function=function, chat=chat, message=message, is_new=True, function_id=self.__get_next_available_function_id(), user_x=user_x)
            await self.__execute_function(function=fun, user_x=user_x)

        """
        callback = update['callback_query']

        output = {'callback_id': callback['id'],
                  'message_id': callback['message']["message_id"] if 'message' in callback and 'message_id' in callback['message'] else None,
                  'chat_id': callback['message']["chat"]["id"] if 'message' in callback and 'chat' in callback['message'] else None,
                  'from_id': callback["from"]["id"],
                  'from_name': callback["from"]["first_name"],
                  'from_username': callback["from"]["username"] if 'username' in callback['from'] else None,
                  'data': callback['data'] if 'data' in callback else None
                  }

        # __ send callback to master __
        self.update_stream.put({'origin': 'telegram',
                                'type': 'callback',
                                'content': output})

        """

        # if 'data' not in update:
        #     self.telegram.send_callback(callback_id=update['callback_id'], text='Data not implemented')
        #     return False
        #
        # # user_x.is_callback = True
        # user_x.callback_id = update['callback_id']
        #
        # txt = update["data"]
        #
        # if 'message_id' in update and user_x.callback_message_id != update['message_id']:
        #     self.telegram.send_callback(callback_id=update['callback_id'], text='Function has Expired')
        #     return False
        #
        # # if 'chat_id' in update:
        # #     user_x.callback_chat_id = update['chat_id']
        #
        # # If the user is already running a command proceed
        # if user_x.last_chat_id == update["chat_id"] and user_x.is_open:
        #     user_x.last_message = txt
        #     return self.__call(user_x, user_x.name_function)
        #
        # self.telegram.send_callback(callback_id=update['callback_id'], text='Inline Keyboard has Expired')
        # return False

    async def __execute_function(self, function: Function, user_x: TelegramUser, initial_settings: dict = None, initial_state: int = 1):
        # __ execute the function normally __
        await function.execute(initial_settings=initial_settings, initial_state=initial_state)

        # __ checks if users must be refreshed __
        if function.need_to_update_users:
            self.update_users()

        # __ checks if a new function should be opened __
        if function.need_to_instantiate_new_function:
            initial_state = function.telegram_function.settings.pop("initial_state") if "initial_state" in function.telegram_function.settings else 1
            new_function = function.telegram_function.settings.pop("new_function")
            if function:
                await self.run_new_function(function=new_function,
                                            user_x=user_x,
                                            chat=function.chat,
                                            message=function.message,
                                            initial_settings=function.telegram_function.settings,
                                            initial_state=initial_state)

    def instantiate_function(self, function,
                             chat: TelegramChat,
                             message: TelegramMessage,
                             is_new: bool,
                             function_id: int,
                             user_x: TelegramUser) -> Function:
        return function(bot=self.telegram_bot,
                        chat=chat,
                        message=message,
                        function_id=function_id,
                        is_new=is_new,
                        postgre_manager=self.postgre_manager)

    async def call_message(self, user_x: TelegramUser, message: TelegramMessage, chat: TelegramChat, txt: str):
        await self.telegram_bot.send_message(chat_id=message.chat_id, text="No Message Action set")

    def _build_new_telegram_message(self, chat: TelegramChat, text: str) -> TelegramMessage:
        message_id = self.__get_next_available_function_id()
        print(message_id)
        return TelegramMessage(message_type=TelegramMessageType.COMMAND,
                               chat_id=chat.chat_id,
                               message_id=message_id,
                               date=int_timestamp_now(),
                               update_id=int_timestamp_now(),
                               from_id=chat.chat_id,
                               from_name=chat.first_name,
                               from_username=chat.username,
                               chat_last_name=chat.last_name,
                               text=text)

    def __get_next_available_function_id(self) -> int:
        taken_ids = [[x.id for x in chat.running_functions] for chat in self.chats]
        taken_ids = [x for xs in taken_ids for x in xs]
        return min([x for x in list(range(1, 100000)) if x not in taken_ids])

    """ ########### Telegram users management ############ """
    async def __handle_command_start(self, message: TelegramMessage, user_x: Type[app_users]):
        if len(user_x) == 0:
            return await self.__handle_new_user(message=message, user_x=user_x)  # TODO: handle case in which user keeps sending command /start

    async def __handle_new_user(self, message: TelegramMessage, user_x: Type[app_users]):
        if message.chat_id != message.from_id:
            return None

        telegram_user = self.create_new_user(telegram_id=message.from_id, name=message.from_name, username=message.from_username)
        telegram_chat = TelegramChat(chat_id=message.chat_id, type='private', username=message.from_username, first_name=message.from_name, last_name=message.chat_last_name)
        telegram_chat.new_message(telegram_message=message)

        """##### FunctionStart for new user #####"""
        # await self.__command(user_x=telegram_user, message=message, chat=telegram_chat)
        function = FunctionStart

        settings = {"app": self.name}
        function = await self.execute_command(user_x=user_x,
                                              command="start",
                                              message=message,
                                              chat=telegram_chat,
                                              initial_settings=settings)

        if "first_send" in function.telegram_function.settings and function.telegram_function.settings["first_send"]:
            # __ get admin user __
            admin_user = self.get_admin_user()
            admin_chat = self.get_admin_chat()

            # FunctionAppNewUser for admin user
            await self.handle_app_new_user(admin_user=admin_user,
                                           admin_chat=admin_chat,
                                           new_telegram_user=telegram_user,
                                           new_telegram_chat=telegram_chat)

    def update_users(self):
        print('refreshing telegram users')
        all_users = self.postgre_manager.get_telegram_users()
        all_chats = self.postgre_manager.get_telegram_chats_from_db()
        diff_users = [x for x in all_users if x.telegram_id not in [x.telegram_id for x in self.users]]
        diff_chats = [x for x in all_chats if x.chat_id not in [x.chat_id for x in self.chats]]
        print(diff_users)  # TODO: delete these prints
        print(diff_chats)
        self.users += diff_users
        self.chats += diff_chats
        self.app_update_users()

    def app_update_users(self):
        pass

    async def handle_app_new_user(self, admin_user: TelegramUser, admin_chat: TelegramChat, new_telegram_user: TelegramUser, new_telegram_chat: TelegramChat):
        pass

    @staticmethod
    def create_new_user(telegram_id: int, name: str = None, username: str = None) -> TelegramUser:
        return TelegramUser(telegram_id=telegram_id, name=name, username=username, is_admin=False)

    @staticmethod
    def create_new_chat(telegram_id: int, first_name: str = None, last_name: str = None, username: str = None) -> TelegramChat:
        return TelegramChat(chat_id=telegram_id, type='private', first_name=first_name, last_name=last_name, username=username)

    def get_chat_from_telegram_id(self, telegram_id: int) -> Optional[TelegramChat]:
        telegram_chats = [x for x in self.chats if x.chat_id == telegram_id]
        if len(telegram_chats) == 0:
            return None
        return telegram_chats[0]

    def get_admin_user(self) -> TelegramUser:
        return [x for x in self.app_users if x.is_admin][0]

    def get_admin_chat(self) -> TelegramChat:
        admin_user = self.get_admin_user()
        admin_user_id = admin_user.telegram_id
        return [x for x in self.chats if x.chat_id == admin_user_id][0]

    """ ########### Logger ############ """
    def __init_logger(self, logger_level, logging_queue):
        logger = LoggerObj(self.name, logger_level)
        logger.add_stream_handler("INFO")
        if logging_queue:
            logger.add_queue_handler(logging_queue, 'WARNING')

        return logger

    def set_logger_stream(self, level):
        handlers = [x for x in self.logger.logger.handlers if type(x) == self.logger.stream_class()]
        for handler in handlers:
            handler.setLevel(level)

    def set_logger_queue(self, level):
        handlers = [x for x in self.logger.logger.handlers if type(x) == self.logger.queue_class()]
        for handler in handlers:
            handler.setLevel(level)

    def set_logger(self, user_x):
        # STATE 0
        if user_x.state_function == 0:
            txt = "Select handler"

            keyboard = [['stream', 'queue']]

            self.send_message(user_x=user_x, text=txt, keyboard=keyboard)

            return user_x.next()

        # STATE 1
        if user_x.state_function == 1:

            if user_x.is_next():

                handler = user_x.last_message  # handler received

                if handler not in ['stream', 'queue']:
                    self.send_callback(user_x=user_x, text='Wrong Entry')
                    return self.go_back(user_x)

                user_x.function_variables['handler'] = handler

            txt = "Select level"

            keyboard = self.square_keyboard(["NOTSET", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])

            self.send_message(user_x=user_x, text=txt, keyboard=keyboard)

            return user_x.next()

        # STATE 2
        if user_x.state_function == 2:

            if user_x.is_next():

                level = user_x.last_message  # level received

                if level not in ["NOTSET", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
                    self.send_callback(user_x=user_x, text='Wrong Entry')
                    return self.go_back(user_x)

                user_x.function_variables['level'] = level

            level = user_x.function_variables['level']
            handler = user_x.function_variables['handler']

            if handler == 'stream':
                self.set_logger_stream(level=level)
            elif handler == 'queue':
                self.set_logger_queue(level=level)

            self.send_message(user_x=user_x, text='Level Set', end_keyboard=self.current_keyboard)

        return self.back_to_master(user_x)


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
