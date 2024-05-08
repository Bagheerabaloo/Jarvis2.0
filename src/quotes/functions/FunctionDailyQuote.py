from dataclasses import dataclass, field
from random import choice, shuffle
from typing import List, Type

from src.common.functions.Function import Function
from src.common.postgre.PostgreManager import PostgreManager
from src.quotes.QuotesUser import QuotesUser
from src.quotes.functions.QuotesFunction import QuotesFunction


@dataclass
class FunctionDailyQuote(QuotesFunction):
    name: str = 'daily_quote'

    async def state_1(self):
        if not await self.send_message(chat_id=self.chat.chat_id, text=self.telegram_function.settings["text"], parse_mode="Markdown"):
            pass
            # self.logger.warning('Unable to send quote to {}: {}'.format(user.name, user.id))
        self.close_function()




