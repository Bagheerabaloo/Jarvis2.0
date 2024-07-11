from dataclasses import dataclass, field
from random import choice, shuffle
from typing import List, Type

from src.common.functions.Function import Function
from src.common.postgre.PostgreManager import PostgreManager
from quotes import QuotesUser
from quotes import QuotesFunction
from quotes import Note


@dataclass
class FunctionShowNotes(QuotesFunction):
    name: str = 'show_notes'

    async def state_1(self):
        if not self.quotes_user.super_user:
            await self.send_message(chat_id=self.chat.chat_id,
                                    text="This is a privileged feature. Ask your administrator to proceed.")
            return self.close_function()

        notes: List[Note] = self.postgre_manager.get_notes_with_tags()
        self.telegram_function.settings["notes"] = notes  # TODO: keep in memory only notes IDs and if needed get from DB
        self.telegram_function.settings["index"] = 0
        self.telegram_function.settings["is_book_note"] = False
        self.telegram_function.next()
        return await self.state_2()

    async def state_2(self):
        self.telegram_function.settings["only_one_book"] = False
        notes: List[Note] = self.telegram_function.settings["notes"]
        if len(notes) == 0:
            await self.send_message(chat_id=self.chat.chat_id, text="No matches in database")
        elif len(notes) == 1:
            text = self.build_note(note=notes[0], index=0, user_x=self.quotes_user)
            await self.send_message(chat_id=self.chat.chat_id, text=text, parse_mode='Markdown')
        else:
            self.telegram_function.next()
            return await self.state_3()

        return self.close_function()

    async def state_3(self):
        notes: List[Note] = self.telegram_function.settings["notes"]
        index = self.telegram_function.settings["index"]
        is_book_note = self.telegram_function.settings["is_book_note"]
        only_one_book = self.telegram_function.settings["only_one_book"]

        if self.telegram_function.previous_state == self.telegram_function.state:
            action = self.message.last_message()
            if action == "Show only this book notes":
                self.telegram_function.settings["book"] = notes[index].book
                self.telegram_function.settings["only_one_book"] = True
                self.telegram_function.next()
                return await self.state_4()
            elif action == 'Edit note' and self.quotes_user.is_admin:
                self.telegram_function.next()
                return await self.state_5()
            index = self.get_new_index(index=index, action=action, len_notes=len(notes))

        note = notes[index]

        text = self.build_note(note=note, index=index, user_x=self.quotes_user)
        if is_book_note:
            text = f"*Book Note*\n\n{text}"

        keyboard = [self.build_navigation_keyboard(index=index, len_=len(notes))]
        if not is_book_note and note.is_book and not only_one_book:
            keyboard.append(["Show only this book notes"])
        if self.quotes_user.is_admin:
            keyboard.append(["Edit note"])

        await self.edit_message(chat_id=self.chat.chat_id,
                                text=text,
                                parse_mode="Markdown",
                                inline_keyboard=keyboard)

        self.telegram_function.settings["index"] = index
        self.telegram_function.same()

    async def state_4(self):
        """ ACTION: Show only this book notes """
        note_id = self.telegram_function.settings["notes"][self.telegram_function.settings["index"]].note_id
        notes: List[Note] = [x for x in self.telegram_function.settings["notes"]
                             if x.book == self.telegram_function.settings["book"]]

        index = next((index for (index, d) in enumerate(notes) if d.note_id == note_id), None)

        self.telegram_function.settings["notes"] = notes
        self.telegram_function.settings["index"] = index
        self.telegram_function.back()
        return await self.state_3()

    async def state_5(self):
        """ ACTION: Edit note """
        note_id = self.telegram_function.settings["notes"][self.telegram_function.settings["index"]].note_id
        self.telegram_function.settings["note_id"] = note_id
        text = "Write the new note"
        await self.send_message(chat_id=self.chat.chat_id,
                                text=text,
                                parse_mode="Markdown",
                                open_for_messages=True)
        self.telegram_function.next()
        return await self.state_6()

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

