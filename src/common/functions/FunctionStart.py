from dataclasses import dataclass, field
from src.common.functions.Function import Function
from common.telegram_manager.TelegramPendingUser import TelegramPendingUser


@dataclass
class FunctionStart(Function):
    name: str = 'start'

    async def state_1(self):
        self.telegram_function.settings["first_send"] = False
        app = self.telegram_function.settings["app"]
        from_ = self.message.get_from()
        user_id = self.message.from_id
        pending_users = self.postgre_manager.get_telegram_pending_users_by_app(app=self.telegram_function.settings["app"])
        if any(x for x in pending_users if x.telegram_id == user_id and x.app == app and not x.banned):
            return await self.send_message(chat_id=self.message.chat_id, text="Application has been already sent. Please wait for approval from admin")
        elif any(x for x in pending_users if x.telegram_id == user_id and x.app == app and x.banned):
            return False

        new_pending_user = TelegramPendingUser(telegram_id=user_id,
                                               name=from_["from_name"],
                                               username=from_["from_username"],
                                               approved=False,
                                               banned=False,
                                               app=self.telegram_function.settings["app"])

        if self.postgre_manager.add_pending_telegram_user_to_db(user=new_pending_user):
            self.telegram_function.settings["first_send"] = True
            return await self.send_message(chat_id=self.message.chat_id, text="Application sent!")

        return False


