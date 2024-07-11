from dataclasses import dataclass
from common.telegram_manager.TelegramManager import TelegramUser


@dataclass
class QuotesUser(TelegramUser):
    auto_detect: bool = False
    show_counter: bool = False
    only_favourites: bool = False
    language: str = "ITA"
    super_user: bool = False
    daily_quotes: bool = True
    daily_book: bool = False

    def get_attribute(self, attribute: str):
        match attribute:
            case "auto_detect":
                return self.auto_detect
            case "show_counter":
                return self.show_counter
            case "only_favourites":
                return self.only_favourites
            case "language":
                return self.language
            case "daily_quotes":
                return self.daily_quotes
            case "daily_book":
                return self.daily_book
            case _:
                raise ValueError("Invalid state")

    def set_attribute(self, attribute: str, value):
        match attribute:
            case "auto_detect":
                self.auto_detect = value
            case "show_counter":
                self.show_counter = value
            case "only_favourites":
                self.only_favourites = value
            case "language":
                self.language = value
            case "daily_quotes":
                self.daily_quotes = value
            case "daily_book":
                self.daily_book = value
            case _:
                raise ValueError("Invalid state")


if __name__ == '__main__':
    pass
