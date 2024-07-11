from dataclasses import dataclass, field
from random import choice, shuffle
from typing import List, Type

from src.common.tools.library import class_from_args, int_timestamp_now
from src.common.functions.Function import Function
from src.common.postgre.PostgreManager import PostgreManager
from quotes import QuotesUser
from src.quotes.functions.QuotesFunction import QuotesFunction
from quotes import Note


@dataclass
class FunctionNewNote(QuotesFunction):
    name: str = 'new_note'
    text: str = ''

    async def state_1(self):
        if self.telegram_function.is_back(depth=2):
            page = self.message.text
            self.telegram_function.settings["pag"] = page
            self.telegram_function.settings["set_tags"] = [x for x in self.telegram_function.settings["set_tags"] if x != 'book']
            # await self.send_message(chat_id=chat_id, text='page added', pending=True)
            self.text += '_page added_\n\n'

        elif not self.telegram_function.is_back():
            self.telegram_function.settings['is_book'] = False
            self.telegram_function.settings['book'] = None
            self.telegram_function.settings['pag'] = None
            self.telegram_function.settings['tags'] = []
            set_tags = ['book'] + self.postgre_manager.get_last_tags(max_tags=3)
            self.telegram_function.settings['set_tags'] = set_tags

        txt = self.text + 'Add new tag or click done'

        keyboard = self.square_keyboard([x for x in self.telegram_function.settings['set_tags'] if x not in self.telegram_function.settings['tags']])
        keyboard += [['done']]

        await self.send_message(chat_id=self.chat.chat_id, text=txt, parse_mode="Markdown", keyboard=keyboard)
        self.telegram_function.next()

    async def state_2(self):
        tag = self.message.last_message()

        if tag == 'book':
            books = self.get_last_books(max_books=4)
            self.telegram_function.settings['is_book'] = True
            keyboard = self.square_keyboard(books)
            await self.send_message(chat_id=self.chat.chat_id, text='Which book?', keyboard=keyboard)
            self.telegram_function.next()
            return

        elif tag != 'done':
            self.telegram_function.settings['tags'].append(tag)
            # await self.send_message(chat_id=self.chat.chat_id, text=tag + ' added', pending=True)
            self.text = f"{tag} added\n\n"
            self.telegram_function.back()
            await self.state_1()
            return

        note = Note(note=self.telegram_function.settings['note'],
                    tags=self.telegram_function.settings['tags'],
                    is_book=self.telegram_function.settings['is_book'],
                    book=self.telegram_function.settings['book'],
                    pag=int(self.telegram_function.settings['pag']) if self.telegram_function.settings['pag'] else None,
                    telegram_id=self.quotes_user.telegram_id,
                    created=int_timestamp_now(),
                    last_modified=int_timestamp_now())

        if self.postgre_manager.insert_one_note(note=note, commit=True):
            await self.send_message(chat_id=self.chat.chat_id, text='Note added', default_keyboard=True)
        else:
            await self.send_message(chat_id=self.chat.chat_id, text='A problem has incurred: note not added', default_keyboard=True)
        self.close_function()

    async def state_3(self):
        book = self.message.last_message()
        self.telegram_function.settings['book'] = book
        last_pag = self.get_last_page(book=book)
        keyboard = self.square_keyboard(list(range(last_pag, last_pag + 9)))
        await self.send_message(chat_id=self.chat.chat_id, text='Insert page', keyboard=keyboard)
        return self.telegram_function.back(steps=2)

    """ Old function:
     
    # STATE 0
    if user_x.state_function == 0:
        if user_x.is_back(depth=2):
            page = user_x.last_message
            user_x.function_variables['pag'] = page
            user_x.function_variables['set_tags'] = [x for x in user_x.function_variables['set_tags'] if x != 'book']
            self.send_message(user_x=user_x, text='page added', pending=True)

        elif not user_x.is_back():
            user_x.function_variables['is_book'] = False
            user_x.function_variables['book'] = None
            user_x.function_variables['pag'] = None
            user_x.function_variables['tags'] = []
            set_tags = ['book'] + self.__get_last_tags(max_tags=3)
            user_x.function_variables['set_tags'] = set_tags

        txt = 'Add Tag or click done'

        keyboard = self.square_keyboard([x for x in user_x.function_variables['set_tags'] if x not in user_x.function_variables['tags']])
        keyboard += [['done']]

        self.send_message(user_x, text=txt, keyboard=keyboard, bypass_inline=True, accept_commands=False)
        return user_x.next()

    # STATE 1
    if user_x.state_function == 1:
        tag = user_x.last_message

        if tag == 'book':
            books = self.__get_last_books(max_books=4)
            user_x.function_variables['is_book'] = True
            keyboard = self.square_keyboard(books)
            self.send_message(user_x=user_x, text='Which book?', keyboard=keyboard, bypass_inline=True)
            return user_x.next()
        elif tag != 'done':
            user_x.function_variables['tags'].append(tag)
            self.send_message(user_x=user_x, text=tag + ' added', pending=True)
            return self.go_back(user_x)

        self.__insert_one_note(note=user_x.function_variables['note'],
                               tags=user_x.function_variables['tags'],
                               book=user_x.function_variables['book'],
                               pag=int(user_x.function_variables['pag']),
                               user_id=user_x.id)
        self.send_message(user_x=user_x, text='Note added', end_keyboard=self.keyboard)

    # STATE 2
    if user_x.state_function == 2:
        book = user_x.last_message
        user_x.function_variables['book'] = book
        last_pag = self.__get_last_page(book=book)
        keyboard = self.square_keyboard(list(range(last_pag, last_pag + 9)))
        self.send_message(user_x=user_x, text='Insert page', keyboard=keyboard, bypass_inline=True)
        return user_x.back(steps=2)

    return user_x.back_to_master()
    """


