from dataclasses import dataclass, field
from random import choice, shuffle
from typing import List, Type

from src.common.functions.Function import Function
from src.common.postgre.PostgreManager import PostgreManager
from quotes import QuotesUser
from quotes import QuotesFunction
from quotes import Quote


@dataclass
class FunctionNewQuote(QuotesFunction):
    name: str = 'new_quote'

    async def state_1(self):
        text = "Insert quote"
        await self.send_message(chat_id=self.chat.chat_id, text=text)
        self.telegram_function.next()

    async def state_2(self):
        quote = self.message.last_message()
        self.telegram_function.settings["quote"] = quote
        text = "Insert author"
        await self.send_message(chat_id=self.chat.chat_id, text=text)
        self.telegram_function.next()

    async def state_3(self):
        author = self.message.last_message()
        self.telegram_function.settings["author"] = author
        await self.state_4()

    async def state_4(self):
        quote = self.telegram_function.settings["quote"]
        author = self.telegram_function.settings["author"]

        similar_quotes = self.postgre_manager.check_for_similar_quotes(quote=quote)
        if similar_quotes and len(similar_quotes) > 0:
            text = 'There is already a similar quote in DB:\n\n' + similar_quotes[0].quote + '\n\n_' + similar_quotes[0].author.replace('_', ' ') + '_'
            await self.send_message(chat_id=self.chat.chat_id, text=text, parse_mode="Markdown")
            self.close_function()
            return False

        quote_obj = Quote(quote=quote, author=author, telegram_id=self.quotes_user.telegram_id)
        if not self.postgre_manager.insert_quote(quote=quote_obj):
            await self.send_message(chat_id=self.chat.chat_id, text='Quote already present in DB')
        else:
            # self.logger.warning('New quote added by: ' + user_x.name + ' ' + str(user_x.id))
            await self.send_message(chat_id=self.chat.chat_id, text='Quote added to DB')

        self.close_function()
        return True


