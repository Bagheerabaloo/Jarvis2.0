from dataclasses import dataclass, field
from random import choice, shuffle

from src.common.tools.library import get_human_date_from_timestamp
from src.common.functions.Function import Function
from src.common.postgre.PostgreManager import PostgreManager
from src.common.telegram.TelegramUser import TelegramUser
from src.quotes.QuotesUser import QuotesUser
from src.quotes.QuotesPostgreManager import QuotesPostgreManager


@dataclass
class QuotesFunction(Function):
    quotes_user: QuotesUser = field(default=None)
    postgre_manager: QuotesPostgreManager = field(default=None)

    @property
    def name(self):
        return "QuoteFunction"

    @property
    def default_keyboard(self):
        return [['Quote'], ['showQuotes', 'showNotes']]

    @property
    def app_user(self):
        return self.quotes_user

    def build_note(self, note: dict, index: int = 0, user_x: QuotesUser = None):
        show_counter = f"_{index + 1}/{len(self.telegram_function.settings['notes'])}_\n\n" if user_x and 'show_counter' in user_x.settings and user_x.settings['show_counter'] else ''
        pag = f" - pag. {note['pag']}" if 'pag' in note and note['pag'] else ""
        book = f"_Book: {note['book']}{pag}_\n" if 'book' in note and note['book'] else ""
        joined_tags = '\n '.join(note['tags']) if 'tags' in note and len(note['tags']) > 0 else ''
        tags = f"Tags:\n_{joined_tags}_" if 'tags' in note and len(note['tags']) > 0 else "_No tags_"
        creation_data = '_Creation date: {}_'.format(get_human_date_from_timestamp(note['created']))
        text = f"{show_counter}{note['note']}\n\n{book}{tags}\n{creation_data}"
        return text

