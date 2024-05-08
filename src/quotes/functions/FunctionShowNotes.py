from dataclasses import dataclass, field
from random import choice, shuffle
from typing import List, Type

from src.common.functions.Function import Function
from src.common.postgre.PostgreManager import PostgreManager
from src.quotes.QuotesUser import QuotesUser
from src.quotes.functions.QuotesFunction import QuotesFunction


@dataclass
class FunctionShowNotes(QuotesFunction):
    name: str = 'show_notes'
    # quotes_ids: List[dict] = field(default_factory=lambda: [])
    # index: int = 0

    async def state_1(self):
        self.telegram_function.settings["notes"] = self.postgre_manager.get_notes_with_tags()
        notes = self.telegram_function.settings["notes"]

        if len(notes) == 0:
            await self.send_message(chat_id=self.chat.chat_id, text="No matches in database")
        elif len(notes) == 1:
            text = self.build_note(note=notes[0]['note'], index=0, user_x=self.quotes_user)
            await self.send_message(chat_id=self.chat.chat_id, text=text, parse_mode='Markdown')
        else:
            self.telegram_function.settings['index'] = 0
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

        note_id = notes[index]['note_id']
        note = self.postgre_manager.get_note_by_id(note_id=note_id)

        text = self.build_note(note=note, index=index, user_x=self.quotes_user)
        keyboard = self.build_navigation_keyboard(index=index, len_=len(notes))
        if self.telegram_function.previous_state == self.telegram_function.state:
            await self.edit_message(chat_id=self.chat.chat_id, text=text, parse_mode="Markdown", inline_keyboard=[keyboard])
        else:
            await self.send_message(chat_id=self.chat.chat_id, text=text, parse_mode="Markdown", inline_keyboard=[keyboard])

        self.telegram_function.settings["index"] = index
        self.telegram_function.previous_state = 2

    """ OLD FUNCTION:
        
        # STATE 0
        if user_x.state_function == 0:

            user_x.function_variables['notes'] = self.__get_notes_with_tags()

            if len(user_x.function_variables['notes']) == 0:
                self.send_message(user_x=user_x, text="No matches in database")
            elif len(user_x.function_variables['notes']) == 1:
                text = self.__build_note(note=user_x.function_variables['notes'][0]['note'], index=0, user_x=user_x)
                self.send_message(user_x=user_x, text=text, parse_mode='Markdown')
            else:
                user_x.function_variables['index'] = 0
                return self.go_forward(user_x=user_x)

            return self.back_to_master(user_x)

        # STATE 1
        if user_x.state_function == 1:
            if user_x.previous_state_function == user_x.state_function:
                action = user_x.last_message
                if action == '<':
                    user_x.function_variables['index'] -= 1
                elif action == '>':
                    user_x.function_variables['index'] += 1
                elif action == '>>':
                    user_x.function_variables['index'] = min(user_x.function_variables['index'] + 10, len(user_x.function_variables['notes']) - 1)
                elif action == '<<':
                    user_x.function_variables['index'] = max(user_x.function_variables['index'] - 10, 0)

            index = user_x.function_variables['index']
            note_id = user_x.function_variables['notes'][index]['note_id']
            note = self.__get_note_by_id(note_id=note_id)

            text = self.__build_note(note=note, index=index, user_x=user_x)
            keyboard = self.__build_navigation_keyboard(index=index, len_=len(user_x.function_variables['notes']))
            self.send_message(user_x=user_x, text=text, keyboard=[keyboard], parse_mode='Markdown', accept_messages=False)

            return user_x.same()

        return user_x.back_to_master()
        """
