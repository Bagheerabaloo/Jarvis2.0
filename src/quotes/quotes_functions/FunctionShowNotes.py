from dataclasses import dataclass, field
from random import choice, shuffle
from typing import List, Type

from quotes.quotes_functions.QuotesFunction import QuotesFunction
from quotes.quotes_functions.FunctionEditNote import FunctionEditNote
from quotes.quotes_functions.FunctionEditPag import FunctionEditPag
from quotes.quotes_functions.FunctionEditTags import FunctionEditTags
from quotes.classes.Note import Note


@dataclass
class FunctionShowNotes(QuotesFunction):
    name: str = 'show_notes'

    async def state_1(self):
        if not self.user.super_user:
            await self.send_message(chat_id=self.chat.chat_id,
                                    text="This is a privileged feature. Ask your administrator to proceed.")
            return self.close_function()

        # notes: List[Note] = self.postgre_manager.get_notes_with_tags()
        # self.telegram_function.settings["notes"] = notes  # TODO: keep in memory only notes IDs and if needed get from DB
        notes_ids = self.postgre_manager.get_notes_ids()
        self.telegram_function.settings["notes_ids"] = notes_ids

        self.telegram_function.settings["index"] = 0
        self.telegram_function.settings["is_book_note"] = False

        self.telegram_function.next()
        return await self.state_2()

    async def state_2(self):
        self.telegram_function.settings["only_one_book"] = False
        # notes: List[Note] = self.telegram_function.settings["notes"]
        notes_ids = self.telegram_function.settings["notes_ids"]
        if len(notes_ids) == 0:
            await self.send_message(chat_id=self.chat.chat_id, text="No matches in database")
        elif len(notes_ids) == 1:
            # text = self.build_note(note=notes[0], index=0, user_x=self.user)
            text = self.build_note_by_id(note_id=notes_ids[0], index=0, user_x=self.user)
            await self.send_message(chat_id=self.chat.chat_id, text=text, parse_mode='Markdown')
        else:
            self.telegram_function.next()
            return await self.state_3()

        return self.close_function()

    async def state_3(self):
        # notes: List[Note] = self.telegram_function.settings["notes"]
        notes_ids = self.telegram_function.settings["notes_ids"]
        index = self.telegram_function.settings["index"]
        is_book_note = self.telegram_function.settings["is_book_note"]
        only_one_book = self.telegram_function.settings["only_one_book"]

        if self.telegram_function.previous_state == self.telegram_function.state:
            action = self.message.last_message()
            if action == "Show only this book notes":
                # self.telegram_function.settings["book"] = notes[index].book
                current_note = self.postgre_manager.get_note_with_tags_by_id(notes_ids[index])
                self.telegram_function.settings["book"] = current_note.book
                self.telegram_function.settings["only_one_book"] = True
                self.telegram_function.next()
                return await self.state_4()
            elif action == 'Edit note' and self.user.is_admin:
                self.telegram_function.next()
                return await self.state_5()
            elif action == 'Edit pag' and self.user.is_admin:
                self.telegram_function.next()
                return await self.state_6()
            elif action == 'Edit tags' and self.user.is_admin:
                self.telegram_function.next()
                return await self.state_7()
            index = self.get_new_index(index=index, action=action, len_notes=len(notes_ids))

        current_note = self.postgre_manager.get_note_with_tags_by_id(notes_ids[index])

        text = self.build_note(note=current_note, index=index, user_x=self.user)
        if is_book_note:
            text = f"*Book Note*\n\n{text}"

        keyboard = [self.build_navigation_keyboard(index=index, len_=len(notes_ids))]
        if not is_book_note and current_note.is_book and not only_one_book:
            keyboard.append(["Show only this book notes"])
        if self.user.is_admin:
            keyboard.append(["Edit note", "Edit pag", "Edit tags"])

        await self.edit_message(chat_id=self.chat.chat_id,
                                text=text,
                                parse_mode="Markdown",
                                inline_keyboard=keyboard)

        self.telegram_function.settings["index"] = index
        self.telegram_function.same()

    async def state_4(self):
        """ ACTION: Show only this book notes """
        # note_id = self.telegram_function.settings["notes_ids"][self.telegram_function.settings["index"]].note_id
        # notes: List[Note] = [x for x in self.telegram_function.settings["notes"]
        #                      if x.book == self.telegram_function.settings["book"]]
        # index = next((index for (index, d) in enumerate(notes) if d.note_id == note_id), None)

        current_note_id = self.telegram_function.settings["notes_ids"][self.telegram_function.settings["index"]]
        new_notes_ids = self.postgre_manager.get_notes_ids_by_book(book=self.telegram_function.settings["book"])
        index = next((index for (index, d) in enumerate(new_notes_ids) if d == current_note_id), None)

        self.telegram_function.settings["notes_ids"] = new_notes_ids
        self.telegram_function.settings["index"] = index
        self.telegram_function.back()
        return await self.state_3()

    async def state_5(self):
        """ ACTION: Edit note """
        # await self.send_callback(chat=self.chat, message=self.message, text="")

        # __ modify current message __
        notes_ids = self.telegram_function.settings["notes_ids"]
        index = self.telegram_function.settings["index"]
        current_note = self.postgre_manager.get_note_with_tags_by_id(notes_ids[index])

        keyboard = [["edit this note below"]]

        await self.edit_message(chat_id=self.chat.chat_id,
                                text=current_note.note,
                                parse_mode="Markdown",
                                inline_keyboard=keyboard)

        self.need_to_instantiate_new_function = True
        self.telegram_function.settings["initial_state"] = 2
        self.telegram_function.settings["new_function"] = FunctionEditNote
        self.close_function()

    async def state_6(self):
        """ ACTION: Edit pag """
        await self.send_callback(chat=self.chat, message=self.message, text="")

        self.need_to_instantiate_new_function = True
        self.telegram_function.settings["initial_state"] = 2
        self.telegram_function.settings["new_function"] = FunctionEditPag
        self.close_function()

    async def state_7(self):
        """ ACTION: Edit tags """
        await self.send_callback(chat=self.chat, message=self.message, text="")

        self.need_to_instantiate_new_function = True
        self.telegram_function.settings["initial_state"] = 2
        self.telegram_function.settings["new_function"] = FunctionEditTags
        self.close_function()

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

