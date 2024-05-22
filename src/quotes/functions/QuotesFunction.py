from dataclasses import dataclass, field
from random import choice, shuffle

from src.common.tools.library import get_human_date_from_timestamp
from src.common.functions.Function import Function
from src.common.postgre.PostgreManager import PostgreManager
from src.common.telegram.TelegramUser import TelegramUser
from src.quotes.QuotesUser import QuotesUser
from src.quotes.QuotesPostgreManager import QuotesPostgreManager
from src.quotes.Note import Note


@dataclass
class QuotesFunction(Function):
    quotes_user: QuotesUser = field(default=None)
    postgre_manager: QuotesPostgreManager = field(default=None)

    @property
    def name(self):
        return "QuoteFunction"

    @property
    def default_keyboard(self):
        return [['Quote'], ['showQuotes', 'showNotes'], ["Settings"]]

    @property
    def main_settings(self):
        return {"auto_detect":      {"long_descr": "automatically detect language when you search through the quotes",
                                     "short_descr": "Auto Detect Language",
                                     "value": self.quotes_user.auto_detect},
                "show_counter":     {"long_descr": "show counter when you go through the quotes",
                                     "short_descr": "Show Quotes Counter",
                                     "value": self.quotes_user.show_counter},
                "only_favourites":  {"long_descr":  "show only quotes you've added to your favourites",
                                     "short_descr": "Show Only Favorites",
                                     "value": self.quotes_user.only_favourites},
                "language":         {"long_descr":  "set language of quotes (beta)",
                                     "short_descr": "Language",
                                     "value": self.quotes_user.language},
                "daily_quotes":     {"long_descr":  "set/unset daily quote",
                                     "short_descr": "Daily Quote",
                                     "value": self.quotes_user.daily_quotes}
                }

    @property
    def super_user_settings(self):
        return {"daily_book":     {"long_descr":   "set/unset daily book notes",
                                   "short_descr": "Daily Book",
                                   "value": self.quotes_user.daily_book},
                }

    def set_attribute(self, attribute: str, value):
        self.quotes_user.set_attribute(attribute=attribute, value=value)

    def get_attribute(self, attribute: str):
        return self.quotes_user.get_attribute(attribute=attribute)

    @property
    def app_user(self):
        return self.quotes_user

    @staticmethod
    def new_quote_user(new_user: TelegramUser):
        return QuotesUser(telegram_id=new_user.telegram_id,
                          name=new_user.name,
                          username=new_user.username,
                          is_admin=False)

    def build_note(self,
                   note: Note,
                   index: int = 0,
                   user_x: QuotesUser = None,
                   show_counter: bool = False,
                   book_in_bold: bool = False):
        # book_markdown_1 = "_" if not book_in_bold else ""
        # book_markdown_2 = "" if not book_in_bold else "*"
        pag = f" - pag. {note.pag}" if note.pag else ""
        book = f"*{note.book}*{pag}\n\n" if note.book else ""
        joined_tags = '\n    • '.join(note.tags) if len(note.tags) > 0 else ''
        tags = f"Tags:\n    _• {joined_tags}_\n\n" if len(note.tags) > 0 else ""
        creation_data = '_Creation date: {}_'.format(get_human_date_from_timestamp(note.created))
        show_counter = f"\n\n_{index + 1}/{len(self.telegram_function.settings['notes'])}_\n\n" if user_x.show_counter or show_counter else ''

        text = f"{book}{note.note}\n\n{tags}{creation_data}{show_counter}"
        return text

    def get_last_books(self, max_books: int = 4):
        sorted_notes = self.postgre_manager.get_notes(sorted_by_created=True)

        # books = list(set([x['book'] for x in sorted_notes if x['book']]))
        books = set()
        books_add = books.add
        books = [x.book for x in sorted_notes if not (x.book in books or books_add(x.book)) and x.book]
        if len(books) > max_books:
            books = books[:max_books]
        return books

    def get_last_page(self, book: str = None) -> int:
        sorted_notes = self.postgre_manager.get_notes(sorted_by_created=True)

        if book:
            sorted_notes = [x for x in sorted_notes if x.book == book]
            if len(sorted_notes) > 0:
                pages = [int(x.pag) for x in sorted_notes if x.pag]
                return max(pages) if len(pages) > 0 else 1
            return 1

        # last_pages = [x for x in sorted_notes if x is not None]
        # if len(last_pages) > 0:
        #     return last_pages[0]

        return 1

