from dataclasses import dataclass, field
from random import choice, shuffle
from typing import List, Type

from src.common.functions.Function import Function
from src.common.postgre.PostgreManager import PostgreManager
from src.common.telegram.TelegramChat import TelegramChat
from src.common.telegram.TelegramUser import TelegramUser
from src.common.functions.FunctionAppNewUser import FunctionAppNewUser
from src.quotes.QuotesUser import QuotesUser
from src.quotes.functions.QuotesFunction import QuotesFunction


@dataclass
class FunctionQuotesNewUser(QuotesFunction, FunctionAppNewUser):
    name: str = 'quotes_new_user'

    def approve_app_user(self, new_user: TelegramUser):
        new_quotes_user = self.new_quote_user(new_user=new_user)
        if not self.postgre_manager.add_quotes_user_to_db(new_quotes_user, commit=False):
            return False
        return True


