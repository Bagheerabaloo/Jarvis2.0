from dataclasses import dataclass, field
from random import choice, shuffle
from typing import List, Type

from src.common.functions.Function import Function
from src.common.postgre.PostgreManager import PostgreManager
from src.common.telegram.TelegramUser import TelegramUser
from src.common.telegram.TelegramChat import TelegramChat


@dataclass
class FunctionAppNewUser(Function):
    name: str = 'app_new_user'

    async def state_1(self):
        new_user: TelegramUser = self.telegram_function.settings["new_user"]
        app: str = self.telegram_function.settings["app"]
        from_ = self.build_from(new_user=new_user)
        text = f"*app {app}* - new user request from:\n\n{from_}"
        keyboard = [["Approve", "Postpone", "Ban"]]
        await self.send_message(chat_id=self.chat.chat_id, text=text, inline_keyboard=keyboard, parse_mode="Markdown")
        return self.telegram_function.next()

    async def state_2(self):
        if self.telegram_function.is_next():
            choice_ = self.message.last_message()
            match choice_:
                case 'Approve':
                    await self.approve_new_user()
                    new_user: TelegramUser = self.telegram_function.settings["new_user"]
                    app: str = self.telegram_function.settings["app"]
                    from_ = self.build_from(new_user=new_user)
                    text = f"*app {app}*\n\n{from_}\n\n*Approved*"
                    await self.edit_message(chat_id=self.chat.chat_id, text=text, parse_mode="Markdown")
                    text = '*Your application has been approved*\n\n_What can I do with this app?_ /help'
                    await self.send_message(chat_id=new_user.telegram_id, text=text, parse_mode="Markdown")
                case 'Postpone':
                    pass
                case 'Ban':
                    await self.ban_new_user()
                case _:
                    raise ValueError("Invalid choice")
        self.close_function()
        return True

    async def approve_new_user(self) -> bool:
        new_user: TelegramUser = self.telegram_function.settings["new_user"]
        new_chat: TelegramChat = self.telegram_function.settings["new_chat"]
        app: str = self.telegram_function.settings["app"]
        # TODO: add corner case in which user is already present in telegram users DB but not in App user DB
        telegram_users = self.postgre_manager.get_telegram_users()

        # __ insert new telegram entries if they don't exist __
        if not any(x for x in telegram_users if x.telegram_id == new_user.telegram_id):
            if not self.postgre_manager.add_telegram_user_to_db(user=new_user, commit=False):
                return False
            if not self.postgre_manager.add_telegram_chat_to_db(chat=new_chat, commit=False):
                return False

        # __ set pending request for this app to approved __
        if not self.postgre_manager.approve_pending_telegram_user(user=new_user, app=app, commit=False):
            return False

        # __ users variables must be updated __
        self.need_to_update_users = True

        # __ approve user for the specific app __
        if not self.approve_app_user(new_user=new_user):
            return False

        return self.postgre_manager.commit()

    @staticmethod
    def approve_app_user(new_user: TelegramUser):
        pass

    async def ban_new_user(self):
        pass


