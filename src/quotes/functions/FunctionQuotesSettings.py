from dataclasses import dataclass, field
from random import choice, shuffle
from typing import List, Type

from src.common.functions.Function import Function
from src.common.postgre.PostgreManager import PostgreManager
from src.quotes.QuotesUser import QuotesUser
from src.quotes.functions.QuotesFunction import QuotesFunction


@dataclass
class FunctionQuotesSettings(QuotesFunction):
    name: str = 'quotes_settings'

    async def state_1(self):
        user = self.quotes_user

        settings = self.main_settings
        if user.super_user:
            settings.update(self.super_user_settings)

        parameter_changed = self.telegram_function.settings['parameter'] if 'parameter' in self.telegram_function.settings else None
        settings_text = '\n    '.join([f"_{settings[key]['short_descr']}_: "
                                       f"{'*' if parameter_changed and parameter_changed == settings[key]['short_descr'] else ''}"  # bold
                                       f"{settings[key]['value']}"
                                       f"{'*' if parameter_changed and parameter_changed == settings[key]['short_descr'] else ''}"  # bold
                                       for key in settings]
                                      )  # TODO: highlight in bold only if parameter is different from the beginning

        txt = f"Current parameters set:\n    {settings_text}\n\nSelect parameter to edit"
        parameters = [settings[key]['short_descr'] for key in settings]
        keyboard = self.square_keyboard(parameters)
        if self.telegram_function.previous_state > self.telegram_function.state:  # TODO: make this if a function
            await self.edit_message(chat_id=self.chat.chat_id, text=txt, parse_mode="Markdown", inline_keyboard=keyboard)
        else:
            await self.send_message(chat_id=self.chat.chat_id, text=txt, parse_mode="Markdown", inline_keyboard=keyboard)

        self.telegram_function.settings["settings"] = settings
        self.telegram_function.settings["accepted_parameters"] = parameters
        self.telegram_function.next()

    async def state_2(self):
        user = self.quotes_user
        parameter = self.message.last_message()
        self.telegram_function.settings['parameter'] = None
        txt = ''

        if parameter == 'Language':
            await self.send_callback(chat=self.chat, message=self.message, text="not implemented")
            return self.telegram_function.same()

        attribute = [x for x in self.telegram_function.settings["settings"] if self.telegram_function.settings["settings"][x]["short_descr"] == parameter][0]
        current_parameter_value = self.get_attribute(attribute=attribute)
        self.set_attribute(attribute=attribute, value=not current_parameter_value)
        txt = f'{parameter} Enabled' if not current_parameter_value else f'{parameter} Disabled'

        await self.send_callback(chat=self.chat, message=self.message, text=txt)

        self.postgre_manager.update_db_user_setting(user=self.quotes_user, attribute=attribute) if self.postgre_manager else None
        self.telegram_function.settings['parameter'] = parameter
        self.telegram_function.back()
        return await self.state_1()



    """ Old function:

    def modify_settings(self, user_x):

        # STATE 0
        if user_x.state_function == 0:

            user = self.get_user_from_db(user_x=user_x)
            # user = user_x

            txt = 'Current parameters set:\n'
            txt += '\n  Auto Detect Language: ' + str(user['settings']['auto_detect'])
            txt += '\n  Show Quotes Counter: ' + str(user['settings']['show_counter'])
            txt += '\n  Daily Quote: ' + str(user['settings']['daily_quote'])
            txt += '\n  Show Only Favorites: ' + str(user['settings']['only_favorites'])
            txt += '\n  Language: ' + str(user['settings']['language'])

            txt += '\n\nSelect Parameter to Edit'

            keyboard = [['autoDetectLang', 'showQuotesCounter'],
                        ['dailyQuote', 'onlyFavorites', 'language']]

            user_x.function_variables['user'] = user

            self.send_message(user_x, text=txt, keyboard=keyboard)

            return user_x.next()

        # STATE 1
        if user_x.state_function == 1:

            user = user_x.function_variables['user']

            sel = user_x.last_message
            user_x.function_variables['sel'] = None
            txt = ''

            if sel not in {'autoDetectLang', 'showQuotesCounter', 'dailyQuote', 'onlyFavorites', 'language'}:
                self.send_message(user_x=user_x, text='Wrong Entry')
                return user_x

            if sel == 'autoDetectLang':

                user['settings']['auto_detect'] = not user['settings']['auto_detect']
                txt = 'Auto Detection Enabled' if user['settings']['auto_detect'] else 'Auto Detection Disabled'

            elif sel == 'showQuotesCounter':

                user['settings']['show_counter'] = not user['settings']['show_counter']
                txt = 'Show Quotes Counter Enabled' if user['settings']['show_counter'] else 'Show Quotes Counter Disabled'

            elif sel == 'dailyQuote':

                user['settings']['daily_quote'] = not user['settings']['daily_quote']
                txt = 'Daily Quote Enabled' if user['settings']['daily_quote'] else 'Daily Quote Disabled'

            elif sel == 'onlyFavorites':

                user['settings']['only_favorites'] = not user['settings']['only_favorites']
                txt = 'Only Favorites Enabled' if user['settings']['only_favorites'] else 'Only Favorites Disabled'

            elif sel == 'language':

                user['settings']['language'] = 'ITA' if user['settings']['language'] == 'ENG' else 'ENG'
                txt = 'Language changed'

            self.send_callback(user_x=user_x, text=txt)

            user_x.settings = user['settings']
            self.update_db_user_settings(user_id=user_x.id, settings=user['settings']) if self.postgre_manager else None
            return self.go_back(user_x)

        return user_x
    """


