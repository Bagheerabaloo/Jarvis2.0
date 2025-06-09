from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Type, TYPE_CHECKING
from queue import Queue
import logging

from src.common.telegram_manager.Command import Command
from src.common.telegram_manager.TelegramBot import TelegramBot
from src.common.telegram_manager.TelegramChat import TelegramChat
from src.common.telegram_manager.TelegramMessage import TelegramMessage
from src.common.telegram_manager.TelegramUser import TelegramUser
from src.common.telegram_manager.TelegramMessageType import TelegramMessageType
from src.common.postgre.PostgreManager import PostgreManager

from src.common.tools import get_exception, int_timestamp_now

if TYPE_CHECKING:
    from src.common.telegram_manager.telegram_operations import TelegramOperations
    from src.common.telegram_manager.function_handler import FunctionHandler
    from src.common.telegram_manager.user_chat_manager import UserChatManager
    from src.common.telegram_manager.polling_handler import PollingHandler
# from common.telegram_manager.logger_config import init_logger

LOGGER = logging.getLogger(__name__)


@dataclass
class TelegramManager:
    token: str
    users: List[TelegramUser]
    chats: List[TelegramChat]
    commands: List[Command]
    postgre_manager: PostgreManager = field(default=None)

    telegram_bot: TelegramBot = field(init=False)
    run: bool = True
    polling_thread: bool = True
    main_thread: bool = True
    user_requests: List = field(default_factory=lambda: [])
    update_stream: Queue = field(default_factory=Queue)
    name: str = ''
    next_id: int = 0

    polling_handler: Optional[PollingHandler] = field(default=None, init=False)
    telegram_operations: Optional[TelegramOperations] = field(default=None, init=False)
    function_handler: Optional[FunctionHandler] = field(default=None, init=False)
    user_chat_manager: Optional[UserChatManager] = field(default=None, init=False)

    def __post_init__(self):
        from common.telegram_manager.telegram_operations import TelegramOperations
        from common.telegram_manager.function_handler import FunctionHandler
        from common.telegram_manager.user_chat_manager import UserChatManager
        from common.telegram_manager.polling_handler import PollingHandler

        self.telegram_bot = TelegramBot(token=self.token)
        self.polling_handler = PollingHandler(self)
        self.telegram_operations = TelegramOperations(self)
        self.function_handler = FunctionHandler(self)
        self.user_chat_manager = UserChatManager(self)
        # self.logger = init_logger()

    @property
    def app_users(self) -> List[TelegramUser]:
        return self.users

    def start(self):
        # __ delete old functions from db __
        self.postgre_manager.delete_old_telegram_functions()

        # __ get all functions from db and assign them to the chats __
        for chat in self.chats:
            chat.running_functions = self.postgre_manager.get_telegram_functions(chat_id=chat.chat_id)

        # __ start polling thread __
        if self.polling_thread:
            self.polling_handler.start_polling_thread()

        # __ start main thread __
        if self.main_thread:
            self.telegram_operations.start_main_thread()

    def close_telegram_manager(self):
        self.run = False
        self.save()
        self.postgre_manager.close_connection()
        self.polling_handler.stop_polling_thread()
        self.telegram_operations.stop_main_thread()

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

    async def execute_command(self,
                              user_x: TelegramUser,
                              command: str,
                              message: TelegramMessage,
                              chat: TelegramChat,
                              initial_settings: dict = None,
                              initial_state: int = 1):
        await self.function_handler.execute_command(
            user_x=user_x,
            command=command,
            message=message,
            chat=chat,
            initial_settings=initial_settings,
            initial_state=initial_state)

    async def call_message(self, user_x: TelegramUser, message: TelegramMessage, chat: TelegramChat, txt: str):
        await self.telegram_bot.send_message(chat_id=message.chat_id, text="No Message Action set")

    def update_users(self):
        self.user_chat_manager.update_users()

    def _build_new_telegram_message(self, chat: TelegramChat, text: str) -> TelegramMessage:
        message_id = self.get_next_available_function_id()
        print(message_id)
        return TelegramMessage(
            message_type=TelegramMessageType.COMMAND,
            chat_id=chat.chat_id,
            message_id=message_id,
            date=int_timestamp_now(),
            update_id=int_timestamp_now(),
            from_id=chat.chat_id,
            from_name=chat.first_name,
            from_username=chat.username,
            chat_last_name=chat.last_name,
            text=text
        )

    def get_next_available_function_id(self) -> int:
        taken_ids = [f.id for chat in self.chats for f in chat.running_functions]
        return min([x for x in range(1, 100000) if x not in taken_ids])

    async def get_function_by_alias(self, alias: str, chat_id: int, user_x: TelegramUser):
        return await self.function_handler.get_function_by_alias(alias=alias, chat_id=chat_id, user_x=user_x)

    """ ########### Telegram users management ############ """
    async def handle_command_start(self, message: TelegramMessage, user_x: Optional[Type[app_users]]):
        if not user_x:
            return await self.user_chat_manager.handle_new_user(message=message, user_x=user_x)  # TODO: handle case in which user keeps sending command /start

    def get_chat_from_telegram_id(self, telegram_id: int) -> Optional[TelegramChat]:
        telegram_chats = [x for x in self.chats if x.chat_id == telegram_id]
        if len(telegram_chats) == 0:
            return None
        return telegram_chats[0]

    def app_update_users(self):
        pass

    async def handle_app_new_user(self, admin_user: TelegramUser, admin_chat: TelegramChat, new_telegram_user: TelegramUser, new_telegram_chat: TelegramChat):
        pass
