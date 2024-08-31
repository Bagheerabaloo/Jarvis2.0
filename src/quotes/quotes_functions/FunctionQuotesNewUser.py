from dataclasses import dataclass, field

from common.telegram_manager import TelegramUser
from src.common.functions.FunctionAppNewUser import FunctionAppNewUser
from quotes.quotes_functions.QuotesFunction import QuotesFunction


@dataclass
class FunctionQuotesNewUser(FunctionAppNewUser, QuotesFunction):
    name: str = 'quotes_new_user'

    def approve_app_user(self, new_user: TelegramUser):
        new_quotes_user = self.new_quote_user(new_user=new_user)
        if not self.postgre_manager.add_quotes_user_to_db(new_quotes_user, commit=False):
            return False
        return True


