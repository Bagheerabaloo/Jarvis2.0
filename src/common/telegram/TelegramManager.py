import telegram
from dataclasses import dataclass, field
from typing import List, Type
from queue import Queue
from time import time, sleep
from threading import Thread
import logging
import asyncio

from src.common.Command import Command
from src.common.functions.Function import Function
from src.common.telegram.TelegramUser import TelegramUser
from src.common.telegram.TelegramChat import TelegramChat
from src.common.telegram.TelegramMessage import TelegramMessage
from src.common.telegram.TelegramMessageType import TelegramMessageType
from src.common.telegram.TelegramBot import TelegramBot

# TODO: create new files in common package
from src.common.tools.library import print_exception, get_exception
# from src.Tools.logging_class import LoggerObj
LOGGER = logging.getLogger(__name__)


@dataclass
class TelegramManager:
    token: str
    users: List[TelegramUser]
    chats: List[TelegramChat]
    telegram_bot: TelegramBot = field(init=False)

    run: bool = True
    receive_messages: bool = True
    handle_messages: bool = True

    name: str = ''
    next_id: int = 0
    update_stream: Queue = Queue()   # Queue: updates from Telegram are put in this queue
    user_requests: list = field(default_factory=lambda: [])
    commands: list[Command] = field(init=False)
    # ext_logger = None
    # logger_level = "DEBUG"
    # logging_queue = None

    def __post_init__(self):
        self.telegram_bot = TelegramBot(token=self.token)

        # __ init logging __
        # self.logger = self.__init_logger(self.logger_level, self.logging_queue) if not self.ext_logger else self.ext_logger

        # __ start telegram polling thread __
        self.start_polling_thread() if self.receive_messages else None

        # __ start messages handler thread __
        self.start_main_thread() if self.handle_messages else None

    # __ Close __
    def close_telegram_manager(self):
        self.run = False

    def close(self):
        self.close_telegram_manager()

    # __ Thread: Get Updates __
    def start_polling_thread(self):
        Thread(target=self.__updates_thread, name='{}TelegramPollThread'.format(self.name)).start()

    def __updates_thread(self):
        while self.run:
            self.__poll_for_updates()
            sleep(0.01)

        # Read updates with next_id in order to avoid to read "end" again in the following run
        try:
            asyncio.run(self.telegram_bot.get_updates(offset=self.next_id))
        except:
            pass

    def __poll_for_updates(self):
        try:
            try:
                updates = asyncio.run(self.telegram_bot.get_updates(offset=self.next_id))
            except telegram.TelegramError:
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
                            callback_id=callback['id'],
                            data=callback['data'] if 'data' in callback else None))

    # __ Thread: Messages handler __
    def start_main_thread(self):
        self.run = True
        Thread(target=lambda: self.main_thread(), name=f'{self.name}TgmMngThr').start()

    def main_thread(self):
        while self.run:
            if not self.update_stream.empty():
                telegram_message = self.update_stream.get_nowait()
                if telegram_message.message_type == TelegramMessageType.COMMAND and telegram_message.text.upper() == 'END':
                    self.close()
                else:
                    self.__handle_event(telegram_message=telegram_message)
            sleep(0.05)

    def __handle_event(self, telegram_message: TelegramMessage):
        # __ identify the user who sent the update __
        user_x = [x for x in self.users if x.id == telegram_message.from_id]
        if len(user_x) == 0:
            return self.__new_user(telegram_message)
        user_x = user_x[0]

        # __ get chat __
        telegram_chats = [x for x in self.chats if x.chat_id == telegram_message.chat_id]
        if len(telegram_chats) == 0:
            return LOGGER.error('CHAT NOT FOUND')
        telegram_chat = telegram_chats[0]
        telegram_chat.new_message(telegram_message=telegram_message)

        try:
            if telegram_message.message_type == TelegramMessageType.COMMAND:
                self.__command(user_x=user_x, message=telegram_message, chat=telegram_chat)
            elif telegram_message.message_type == TelegramMessageType.MESSAGE:
                self.__message(user_x=user_x, message=telegram_message, chat=telegram_chat)
            elif telegram_message.message_type == TelegramMessageType.CALLBACK:
                self.__callback(user_x=user_x, message=telegram_message, chat=telegram_chat)
        except:
            LOGGER.error(get_exception())
            # user_x.reset()

    def get_function_by_alias(self, alias: str, user_x: TelegramUser) -> List[Type[Function]]:
        # Command(alias=["ciao", "hello"], admin=True, function=FunctionCiao),
        return [x.function for x in self.commands if alias in [y.lower() for y in x.alias] and (not x.admin or user_x.is_admin)]

    def get_function_by_name(self, name: str, user_x: TelegramUser) -> List[Type[Function]]:
        # Command(alias=["ciao", "hello"], admin=True, function=FunctionCiao),
        return [x.function for x in self.commands if name == x.function.name and (not x.admin or user_x.is_admin)]

    def __command(self, user_x: TelegramUser, message: TelegramMessage, chat: TelegramChat):
        LOGGER.info(f'{message.text} by {user_x.name} {user_x.id}')

        functions = self.get_function_by_alias(alias=message.text.strip('/'), user_x=user_x)
        if len(functions) == 0:
            return self.telegram_bot.send_message(chat_id=message.chat_id, text="Function not implemented or forbidden")
        function = functions[0]
        function(bot=self.telegram_bot, chat=chat, message=message, function_id=message.message_id, is_new=True)
        # FunctionFactory.get_function(function_type=function_type[0], bot=self.telegram_bot, chat=chat, message=message, function_id=message.message_id, is_new=True)

    def __message(self, user_x: TelegramUser, message: TelegramMessage, chat: TelegramChat):
        telegram_function = chat.get_function_open_for_message()
        if telegram_function:
            functions: List[Type[Function]] = self.get_function_by_name(name=telegram_function.name, user_x=user_x)
            functions[0](bot=self.telegram_bot, chat=chat, message=message, function_id=telegram_function.id, is_new=False)
            # FunctionFactory.get_function(function_type=telegram_function.function_type, bot=self.telegram_bot, chat=chat, message=message, function_id=telegram_function.id, is_new=False)
        else:
            functions: List[Type[Function]] = self.get_function_by_alias(alias=message.text.strip('/'), user_x=user_x)
            if len(functions) != 0:
                functions[0](bot=self.telegram_bot, chat=chat, message=message, function_id=message.message_id, is_new=True)
            else:
                LOGGER.info('it goes to message for data')

    def __callback(self, user_x: TelegramUser, message: TelegramMessage, chat: TelegramChat):
        telegram_function = chat.get_function_by_callback_message_id(callback_message_id=message.message_id)
        if telegram_function:
            functions: List[Type[Function]] = self.get_function_by_name(name=telegram_function.name, user_x=user_x)
            functions[0](bot=self.telegram_bot, chat=chat, message=message, function_id=telegram_function.id, is_new=False)
            # FunctionFactory.get_function(function_type=telegram_function.function_type, bot=self.telegram_bot, chat=chat, message=message, function_id=telegram_function.id, is_new=False)

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

    def __call(self, user_x, func):
        pass
        # func = func.lower()
        #
        # if user_x.is_admin:
        #     return self.__admin_call(user_x, func)
        #
        # return self.__all_users_call(user_x, func)

    def __admin_call(self, user_x, func):
        if func == 'end':
            self.close()
            return user_x

        # get the function by name
        method_name = [x for x in self.admin_functions if func in [x.lower() for x in self.admin_functions[x]]]

        if len(method_name) > 0:
            method = eval('self.' + method_name[0])

            args = [user_x]
            return method(*args)

        return self.__all_users_call(user_x, func)

    def __all_users_call(self, user_x, func):

        if func in [x.lower() for x in self.simple_send]:
            self.send_message(user_x=user_x, text=self.simple_send[func]['text'], keyboard=getattr(self, self.simple_send[func]['keyboard']), bypass_inline=True)
            return user_x.reset()

        # get the function by name
        method_name = [x for x in self.functions if func in [x.lower() for x in self.functions[x]]]

        if len(method_name) > 0:
            method = eval('self.' + method_name[0])

            args = [user_x]
            return method(*args)

        self.send_message(user_x=user_x, text='Function not present or forbidden')
        return user_x.reset()

    def call_message(self, user_x, txt):

        self.send_message(user_x=user_x, text='No message data action set')
        return user_x

    # __ Telegram users management __
    def __new_user(self, telegram_message):
        new_request = telegram_message.get_from()
        if new_request not in self.user_requests:
            self.user_requests.append(new_request)
            self.send_message(chat_id=new_request.from_id, text='Application Sent')
            self.send_message(chat_id=[x for x in self.users if x.is_admin][0], text=f'New user request from {new_request.from_name}')
            return True
        return False

    def add_user_to_list(self, new_user):
        self.users.append(new_user)

    def remove_user_from_list(self, user_id):
        self.users = [x for x in self.users if x.id != user_id]

    def show_users(self, user_x):
        for user in self.users:
            self.send_message(user_x=user_x, text=user.name + ' ' + str(user.id))
            sleep(0.2)

    def validate_user(self, user_x):

        # STATE 0
        if user_x.state_function == 0:

            if len(self.user_requests) == 0:
                self.telegram.send_message(chat_id=user_x.last_chat_id, text="No user requests")
                return user_x.reset()

            new_request = self.user_requests.pop()
            user_x.function_variables['new_request'] = new_request

            txt = "Add user " + new_request['name'] + ' (chat ID: ' + str(new_request['from_id']) + ')?'

            self.send_message(user_x=user_x, text=txt)

            return user_x.next()

        # STATE 1
        if user_x.state_function == 1:

            entry = user_x.last_message.upper()

            if entry not in {'YES', 'NO'}:
                self.telegram.send_message(chat_id=user_x.last_chat_id, text='Wrong Entry')
                return user_x

            if user_x.last_message.upper() == 'YES':
                request = user_x.function_variables['new_request']

                new_user = TelegramUser(user_id=request['from_id'],
                                        name=request['name'],
                                        username=request['username'],
                                        is_admin=False)

                self.add_user_to_list(new_user)
                self.add_user_to_db(new_user)
                self.welcome_user(new_user)

        return self.back_to_master(user_x)

    def delete_user(self, user_x):

        # STATE 0
        if user_x.state_function == 0:

            if len([x for x in self.users if x.is_admin]) == 0:
                self.telegram.send_message(chat_id=user_x.last_chat_id, text="No user to delete")
                return user_x.reset()

            txt = 'Users\n'

            for user in self.users:
                txt += user.name + ' ' + str(user.id) + '\n'

            txt += 'Select user ID to delete'
            keyboard = self.square_keyboard([str(x.id) for x in self.users])
            self.send_message(user_x=user_x, text=txt, keyboard=keyboard)
            return user_x.next()

        # STATE 1
        if user_x.state_function == 1:

            user_id = int(user_x.last_message)

            if user_id not in [x.id for x in self.users]:
                self.send_message(user_x=user_x, text='Wrong Entry')
                return user_x.same()

            user_x.function_variables['user_id'] = user_id
            user_name = [x.name for x in self.users if x.id == user_id][0]

            txt = "You're about to delete user {} id: {}\nConfirm?".format(user_name, user_id)
            keyboard = [['YES', 'NO']]

            self.send_message(user_x=user_x, text=txt, keyboard=keyboard)
            return user_x.next()

        # STATE 2
        if user_x.state_function == 2:

            entry = user_x.last_message.upper()

            if entry not in ['YES', 'NO']:
                self.send_message(user_x=user_x, text='Wrong Entry')
                return user_x.same()

            if entry == 'YES':

                self.remove_user_from_list(user_x.function_variables['user_id'])
                self.remove_user_from_db(user_x.function_variables['user_id'])
                self.send_message(user_x=user_x, text='User deleted')

            else:
                self.send_message(user_x=user_x, text='Function aborted')

        return self.back_to_master(user_x)

    def welcome_user(self, user):
        self.send_message(user_x=user, text="Welcome!", end_keyboard=self.keyboard)

    # __ Logger __
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
