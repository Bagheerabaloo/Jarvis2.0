from dataclasses import dataclass
from src.common.telegram.TelegramUser import TelegramUser


@dataclass
class QuotesUser(TelegramUser):
    auto_detect: bool = False
    show_counter: bool = False
    only_favourites: bool = False
    language: str = "ITA"
    super_user: bool = False
    daily_quotes: bool = True
    daily_book: bool = False


if __name__ == '__main__':
    pass
