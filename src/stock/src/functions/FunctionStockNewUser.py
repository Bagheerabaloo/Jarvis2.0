from dataclasses import dataclass

from src.common.telegram.TelegramUser import TelegramUser
from src.common.functions.FunctionAppNewUser import FunctionAppNewUser
from stock.src.functions.StockFunction import StockFunction


@dataclass
class FunctionStockNewUser(StockFunction, FunctionAppNewUser):
    name: str = 'stock_new_user'

    def approve_app_user(self, new_user: TelegramUser):
        new_stock_user = self.new_stock_user(new_user=new_user)
        if not self.postgre_manager.add_stock_user_to_db(new_stock_user, commit=False):
            return False
        return True



