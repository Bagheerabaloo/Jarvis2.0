from dataclasses import dataclass, field
from random import choice, shuffle
from typing import List, Type

from src.common.functions.Function import Function
from src.common.postgre.PostgreManager import PostgreManager
from quotes.classes.QuotesUser import QuotesUser
from quotes.quotes_functions.QuotesFunction import QuotesFunction


@dataclass
class FunctionRandomQuote(QuotesFunction):
    name: str = 'random_quote'

    async def state_1(self):
        chat_id = self.chat.chat_id

        quotes = self.postgre_manager.get_all_quotes()
        if len(quotes) == 0:
            await self.send_message(chat_id=chat_id, text='No quotes saved in DB')
            return self.close_function()

        quote = choice(quotes)
        quote_body = self.postgre_manager.get_quote_in_language(quote=quote, user=self.user)
        author = quote.author.replace('_', ' ')
        text = f"{quote_body}\n\n_{author}_"
        await self.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")
        self.close_function()
