from dataclasses import dataclass, field
from random import choice, shuffle
from typing import List, Type

from src.common.functions.Function import Function
from src.common.postgre.PostgreManager import PostgreManager
from src.quotes.QuotesUser import QuotesUser
from src.quotes.functions.QuotesFunction import QuotesFunction


@dataclass
class FunctionShowQuote(QuotesFunction):
    name: str = 'show_quotes'
    # quotes_ids: List[dict] = field(default_factory=lambda: [])
    # index: int = 0

    async def state_1(self):
        chat_id = self.chat.chat_id

        query = f"""SELECT Q.quote_id 
                    FROM quotes Q
                    JOIN favorites F ON F.quote_id = Q.quote_id
                    WHERE F.telegram_id = {self.quotes_user.telegram_id}""" if self.quotes_user.only_favourites else "SELECT quote_id FROM quotes"

        quotes_ids = self.postgre_manager.select_query(query=query)
        if len(quotes_ids) == 0:
            await self.send_message(chat_id=chat_id, text='No quotes saved in DB')
            return self.close_function()

        shuffle(quotes_ids)

        self.telegram_function.settings["quotes_ids"] = quotes_ids
        self.telegram_function.settings["index"] = 0

        self.telegram_function.previous_state = 1
        self.telegram_function.state = 2
        await self.state_2()

    async def state_2(self):
        quotes_ids = self.telegram_function.settings["quotes_ids"]
        index = self.telegram_function.settings["index"]
        if self.telegram_function.previous_state == self.telegram_function.state:
            action = self.message.data
            if action == '<':
                index -= 1
            elif action == '>':
                index += 1
            elif action == '>>':
                index = min(index + 10, len(quotes_ids) - 1)
            elif action == '<<':
                index = max(index - 10, 0)
            elif action == 'AddFavorite':
                self.postgre_manager.insert_favorite(quote_id=quotes_ids[index]['quote_id'], user_id=self.quotes_user.telegram_id)
                await self.send_callback(chat=self.chat, message=self.message, text='Added to favorites')
            elif action == 'RemoveFavorite':
                self.postgre_manager.delete_favorite(quote_id=quotes_ids[index]['quote_id'], user_id=self.quotes_user.telegram_id)
                await self.send_callback(chat=self.chat, message=self.message, text='Removed from favorites')
            # elif action == 'addTranslation' and user_x.settings['super_user']:
            #     user_x.accept_messages = True
            #     return self.system_automessage(user_x.next())
            #     # self.send_callback(user_x=user_x, text='Function not present')
            #     # user_x.nest_function(name='library', nxt=1, prev=0)
            #     # return self.system_automessage(user_x.next_function(name='add_translation_to_quote', nxt=1, reserved=True))

        keyboard = self.build_navigation_keyboard(index=index, len_=len(quotes_ids))
        params = {'quote_id': quotes_ids[index]['quote_id']}
        quote = self.postgre_manager.get_quotes(params=params)[0]
        # quote = self.__find_quotes(params)[0]

        show_counter_header = f"_{str(index + 1)}/{str(len(quotes_ids))}_\n\n" if self.quotes_user.show_counter else ''
        quote_body = self.postgre_manager.get_quote_in_language(quote=quote, user=self.quotes_user)
        quote_author = quote.author.replace('_', ' ')
        text = f"{show_counter_header}{quote_body}\n\n_{quote_author}_"

        if self.postgre_manager.is_favorite(quote_id=quote.quote_id, telegram_id=self.quotes_user.telegram_id):
            keyb = [['RemoveFavorite', 'addTranslation']] if self.quotes_user.super_user and quote.quote_ita is None else [['RemoveFavorite']]
        else:
            keyb = [['AddFavorite', 'addTranslation']] if self.quotes_user.super_user and quote.quote_ita is None else [['AddFavorite']]
        if len(keyboard) > 0:
            keyb.append(keyboard)

        if self.telegram_function.previous_state == self.telegram_function.state:
            await self.edit_message(chat_id=self.chat.chat_id, text=text, parse_mode="Markdown", inline_keyboard=keyb)
        else:
            await self.send_message(chat_id=self.chat.chat_id, text=text, parse_mode="Markdown", inline_keyboard=keyb)

        self.telegram_function.settings["index"] = index
        self.telegram_function.previous_state = 2

    # # STATE 2
    # if user_x.state_function == 2:
    #     txt = user_x.last_inline_text + '\n\nInsert Italian translation'
    #
    #     self.send_message(user_x, text=txt, parse_mode="Markdown", accept_commands=False)
    #
    #     return user_x.next()
    #
    # # STATE 3
    # if user_x.state_function == 3:
    #
    #     translation = user_x.last_message
    #     quote_id = int(user_x.function_variables['quotes_ids'][user_x.function_variables['index']]['quote_id'])
    #
    #     if self.__update_quote_by_quote_id(quote_id=quote_id, set_params={'quote_ita': translation}):
    #         self.logger.info('Added translation to quote id {} by: '.format(quote_id) + user_x.name + ' ' + str(user_x.id))
    #         self.send_message(user_x=user_x, text='Translation added', end_keyboard=self.keyboard)
    #     else:
    #         self.logger.info('Failed to add translation to quote id {} by: '.format(quote_id) + user_x.name + ' ' + str(user_x.id))
    #         self.send_message(user_x=user_x, text='Failed to add translation', end_keyboard=self.keyboard)
    #
    #     return self.system_automessage(user_x.back(steps=2))

