from dataclasses import dataclass, field

from src.common.functions.Function import Function
from src.common.telegram.TelegramUser import TelegramUser
from stock.src.StockUser import StockUser
from stock.src.StockPostgreManager import StockPostgreManager


@dataclass
class StockFunction(Function):
    stock_user: StockUser = field(default=None)
    postgre_manager: StockPostgreManager = field(default=None)

    @property
    def name(self):
        return "StockFunction"

    @property
    def default_keyboard(self):
        return [['Callback'], ['showQuotes', 'showNotes']]

    @property
    def app_user(self):
        return self.stock_user

    @staticmethod
    def new_stock_user(new_user: TelegramUser):
        return StockUser(telegram_id=new_user.telegram_id,
                         name=new_user.name,
                         username=new_user.username,
                         is_admin=False)

