from dataclasses import dataclass, field
from random import choice, shuffle
from typing import List, Type

from src.common.functions.Function import Function
from src.common.postgre.PostgreManager import PostgreManager
from src.quotes.QuotesUser import QuotesUser
from src.quotes.functions.QuotesFunction import QuotesFunction


@dataclass
class FunctionDailyBook(QuotesFunction):
    name: str = 'daily_book'

    async def state_1(self):
        note = self.telegram_function.settings["note"]
        notes = self.telegram_function.settings["notes"]

        if len(notes) == 0:
            print("No book notes in database")
            # await self.send_message(chat_id=self.chat.chat_id, text="No book notes in database")
        elif len(notes) == 1:
            text = self.build_note(note=note['note'], index=0, user_x=self.quotes_user)
            await self.send_message(chat_id=self.chat.chat_id, text=text, parse_mode='Markdown')
        else:
            self.telegram_function.previous_state = 1
            self.telegram_function.state = 2
            return await self.state_2()

        return self.close_function()

    async def state_2(self):
        notes = self.telegram_function.settings["notes"]
        index = self.telegram_function.settings["index"]
        if self.telegram_function.previous_state == self.telegram_function.state:
            action = self.message.last_message()
            if action == '<':
                index -= 1
            elif action == '>':
                index += 1
            elif action == '>>':
                index = min(index + 10, len(notes) - 1)
            elif action == '<<':
                index = max(index - 10, 0)

        note = notes[index]

        text = self.build_note(note=note, index=index, user_x=self.quotes_user, book_in_bold=True)
        text = f'*Book note*\n\n{text}'
        keyboard = self.build_navigation_keyboard(index=index, len_=len(notes))
        if self.telegram_function.previous_state == self.telegram_function.state:
            await self.edit_message(chat_id=self.chat.chat_id, text=text, parse_mode="Markdown", inline_keyboard=[keyboard])
        else:
            await self.send_message(chat_id=self.chat.chat_id, text=text, parse_mode="Markdown", inline_keyboard=[keyboard])  # TODO: handle error in sending telegram message

        self.telegram_function.settings["index"] = index
        self.telegram_function.previous_state = 2