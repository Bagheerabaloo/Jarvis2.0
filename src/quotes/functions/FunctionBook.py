from dataclasses import dataclass, field
from random import choice, shuffle
from typing import List, Type

from src.common.tools.library import class_from_args, int_timestamp_now
from src.common.functions.Function import Function
from src.common.postgre.PostgreManager import PostgreManager
from src.quotes.QuotesUser import QuotesUser
from src.quotes.functions.QuotesFunction import QuotesFunction
from src.quotes.functions.FunctionShowNotes import FunctionShowNotes
from src.quotes.Note import Note


@dataclass
class FunctionBook(QuotesFunction):
    name: str = 'book'

    async def state_1(self):
        books = self.postgre_manager.get_books()
        self.telegram_function.settings["books"] = books

        if len(books) == 0:
            await self.send_message(chat_id=self.chat.chat_id, text="No books in database")
            return self.close_function()

        txt = "Select book:"
        keyboard = self.square_keyboard([x for x in books])
        await self.send_message(chat_id=self.chat.chat_id, text=txt, parse_mode="Markdown", keyboard=keyboard)
        self.telegram_function.next()

    async def state_2(self):
        book = self.message.last_message()

        if book not in self.telegram_function.settings["books"]:
            await self.send_callback(chat=self.chat, message=self.message, text="Invalid book")
            return self.telegram_function.same()

        self.telegram_function.settings["book"] = book
        notes = self.postgre_manager.get_notes_with_tags_by_book(book=book)
        self.telegram_function.settings["notes"] = notes
        self.telegram_function.settings["index"] = 0
        self.telegram_function.settings["is_book_note"] = False
        self.telegram_function.settings["initial_state"] = 2
        self.need_to_instantiate_new_function = True
        self.telegram_function.settings["new_function"] = FunctionShowNotes
        self.close_function()
