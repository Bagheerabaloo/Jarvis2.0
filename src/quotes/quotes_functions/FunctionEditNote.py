from dataclasses import dataclass, field
from random import choice, shuffle
from typing import List, Type

from quotes.quotes_functions.QuotesFunction import QuotesFunction
from quotes.classes.Note import Note


@dataclass
class FunctionEditNote(QuotesFunction):
    name: str = 'edit_note'

    async def state_2(self):
        note_id = self.telegram_function.settings["notes_ids"][self.telegram_function.settings["index"]]
        self.telegram_function.settings["note_id"] = note_id
        text = "Write the new note"
        await self.send_message(chat_id=self.chat.chat_id,
                                text=text,
                                parse_mode="Markdown",
                                open_for_messages=True)
        self.telegram_function.next()

    async def state_3(self):
        new_note = self.message.last_message()
        self.telegram_function.settings["new_note"] = new_note
        text = "Confirm?"
        keyboard = [["Yes", "No"]]
        await self.send_message(chat_id=self.chat.chat_id,
                                text=text,
                                parse_mode="Markdown",
                                inline_keyboard=keyboard)
        self.telegram_function.next()

    async def state_4(self):
        action = self.message.last_message()
        if action == "Yes":
            note_id = self.telegram_function.settings["note_id"]
            new_note = self.telegram_function.settings["new_note"]
            self.postgre_manager.update_note_by_note_id(note_id=note_id, set_params={"note": new_note})
            await self.send_callback(chat=self.chat, message=self.message, text="")
            await self.edit_message(chat_id=self.chat.chat_id, text="Confirm?\n\n*Yes*", parse_mode="Markdown")
            await self.send_message(chat_id=self.chat.chat_id, text="Note updated")
        else:
            await self.edit_message(chat_id=self.chat.chat_id, text="Confirm?\n\n*No*", parse_mode="Markdown")
            await self.send_message(chat_id=self.chat.chat_id, text="Note not updated")
        self.need_to_instantiate_new_function = True
        self.telegram_function.settings["initial_state"] = 3
        from quotes.quotes_functions.FunctionShowNotes import FunctionShowNotes
        self.telegram_function.settings["new_function"] = FunctionShowNotes
        self.close_function()


