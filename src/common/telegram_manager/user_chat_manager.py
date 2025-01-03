from dataclasses import dataclass
from typing import Optional, Type, TYPE_CHECKING

from src.common.functions.FunctionStart import FunctionStart

from src.common.telegram_manager.telegram_manager import TelegramManager, LOGGER
from src.common.telegram_manager.TelegramMessage import TelegramMessage
from src.common.telegram_manager.TelegramUser import TelegramUser
from src.common.telegram_manager.TelegramChat import TelegramChat


@dataclass
class UserChatManager:
    manager: TelegramManager

    async def handle_new_user(self, message: TelegramMessage, user_x: Optional[Type[TelegramManager.app_users]]):
        if message.chat_id != message.from_id:
            return None

        telegram_user = self.create_new_user(telegram_id=message.from_id, name=message.from_name, username=message.from_username)
        telegram_chat = self.create_new_chat(telegram_id=message.chat_id, first_name=message.from_name, last_name=message.chat_last_name, username=message.from_username)
        telegram_chat.new_message(telegram_message=message)

        """##### FunctionStart for new user #####"""
        function = FunctionStart

        settings = {"app": self.manager.name}
        function = await self.manager.function_handler.execute_command(
            user_x=telegram_user,
            command="start",
            message=message,
            chat=telegram_chat,
            initial_settings=settings)

        if function and function.telegram_function.settings.get("first_send"):
            admin_user = self.get_admin_user()
            admin_chat = self.get_admin_chat()
            await self.manager.handle_app_new_user(
                admin_user=admin_user,
                admin_chat=admin_chat,
                new_telegram_user=telegram_user,
                new_telegram_chat=telegram_chat)

    def update_users(self):
        LOGGER.info('refreshing telegram users')
        all_users = self.manager.postgre_manager.get_telegram_users()
        all_chats = self.manager.postgre_manager.get_telegram_chats_from_db()
        diff_users = [x for x in all_users if x.telegram_id not in [u.telegram_id for u in self.manager.users]]
        diff_chats = [x for x in all_chats if x.chat_id not in [c.chat_id for c in self.manager.chats]]
        print(diff_users)  # TODO: delete these prints
        print(diff_chats)
        self.manager.users += diff_users
        self.manager.chats += diff_chats
        self.manager.app_update_users()

    @staticmethod
    def create_new_user(telegram_id: int, name: str = None, username: str = None) -> TelegramUser:
        return TelegramUser(telegram_id=telegram_id, name=name, username=username, is_admin=False)

    @staticmethod
    def create_new_chat(telegram_id: int, first_name: str = None, last_name: str = None, username: str = None) -> TelegramChat:
        return TelegramChat(chat_id=telegram_id, type='private', first_name=first_name, last_name=last_name, username=username)

    def get_admin_user(self) -> TelegramUser:
        return next((x for x in self.manager.app_users if x.is_admin), None)

    def get_admin_chat(self) -> TelegramChat:
        admin_user = self.get_admin_user()
        return next((x for x in self.manager.chats if x.chat_id == admin_user.telegram_id), None)


