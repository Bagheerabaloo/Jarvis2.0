from dataclasses import dataclass, field
from random import choice, shuffle
from typing import List, Type

from quotes.quotes_functions.QuotesFunction import QuotesFunction
from quotes.classes.Note import Note


@dataclass
class FunctionEditTags(QuotesFunction):
    name: str = 'edit_tags'

    async def state_2(self):
        tags = self.get_tags()
        text = "Click on one tag to remove it.\nClick on done! if you don't want to remove any tags"
        keyboard = self.square_keyboard(tags + ["done!"])
        await self.edit_message(chat_id=self.chat.chat_id,
                                text=text,
                                parse_mode="Markdown",
                                inline_keyboard=keyboard)
        self.telegram_function.next()

    async def state_3(self):
        tags = self.get_tags()
        tag = self.message.last_message()
        if tag == "done!":
            await self.edit_message(chat_id=self.chat.chat_id, text="Tags updated")
            self.telegram_function.next()
            return await self.state_4()
        if tag in tags:
            self.postgre_manager.remove_tag_from_note_by_id(note_id=self.telegram_function.settings["note_id"], tag=tag)
            self.telegram_function.back()
            return await self.state_2()

        await self.send_callback(chat=self.chat, message=self.message, text=f"Wrong choice")
        return await self.state_2()

    async def state_4(self):
        tags = self.get_tags()  # TODO: don't make a query each time
        list_of_tags = '\n    • '.join(tags)
        text = f"Current tags:\n_{list_of_tags}_\n\nWrite any tag you want to add. Click on done! if you don't want to add any tag"
        keyboard = self.square_keyboard(["done!"])
        await self.send_message(chat_id=self.chat.chat_id,
                                text=text,
                                parse_mode="Markdown",
                                keyboard=keyboard,
                                open_for_messages=True)
        self.telegram_function.next()

    async def state_5(self):
        tags = self.get_tags()
        new_tag = self.message.last_message()
        if new_tag == "done!":
            await self.send_message(chat_id=self.chat.chat_id, text="Tags updated")
            self.need_to_instantiate_new_function = True
            self.telegram_function.settings["initial_state"] = 3
            from quotes.quotes_functions.FunctionShowNotes import FunctionShowNotes
            self.telegram_function.settings["new_function"] = FunctionShowNotes
            return self.close_function()
        if new_tag in tags:
            await self.send_callback(chat=self.chat, message=self.message, text=f"Tag already added")
            return await self.state_4()
        self.postgre_manager.add_tag_to_note_by_id(note_id=self.telegram_function.settings["note_id"], tag=new_tag)
        self.telegram_function.back()
        return await self.state_4()

    def get_tags(self):
        note_id = self.telegram_function.settings["notes_ids"][self.telegram_function.settings["index"]]
        self.telegram_function.settings["note_id"] = note_id
        note_with_tags = self.postgre_manager.get_note_with_tags_by_id(note_id)
        tags = [tag.tag for tag in note_with_tags.tags]
        return tags


