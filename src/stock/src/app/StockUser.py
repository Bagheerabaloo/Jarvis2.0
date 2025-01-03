from dataclasses import dataclass
from src.common.telegram_manager.TelegramUser import TelegramUser


@dataclass
class StockUser(TelegramUser):
    super_user: bool = False
    daily_analysis: bool = False
