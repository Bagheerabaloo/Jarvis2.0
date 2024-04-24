from dataclasses import dataclass, field
from time import time, sleep

from jarvis.common.Command import Command
from jarvis.common.telegram.TelegramManager import TelegramManager
from jarvis.common.telegram.TelegramUser import TelegramUser
from jarvis.common.telegram.TelegramChat import TelegramChat

from jarvis.common.functions.FunctionCiao import FunctionCiao
from jarvis.common.functions.FunctionCallback import FunctionCallback
from jarvis.common.functions.FunctionProcess import FunctionProcess


@dataclass
class Quotes(TelegramManager):
    commands = [
        Command(alias=["ciao", "hello"], admin=True, function=FunctionCiao),
        Command(alias=["callback"], admin=True, function=FunctionCallback),
        Command(alias=["process"], admin=True, function=FunctionProcess)
    ]


if __name__ == '__main__':
    import logging
    from logging.handlers import TimedRotatingFileHandler
    from pathlib import Path
    from logger_tt import setup_logging

    config_path = Path(__file__).parent.parent.parent.joinpath('resources', 'logger.conf.yaml')
    log_conf = setup_logging(config_path=str(config_path), use_multiprocessing=True)
    LOGGER = logging.getLogger()

    token = "521932309:AAEBJrGFDznMH1GEiH5veKRR3p787_hV_2w"
    users = [TelegramUser(id=19838246, name='Vale', username='vales2', is_admin=True)]
    chats = [TelegramChat(chat_id=19838246, type='private', username='vales2', first_name='Vale', last_name='S')]
    telegram_manager = Quotes(token=token, users=users, chats=chats)
    telegram_manager.telegram_bot.send_message(chat_id=19838246, text='ciao', inline_keyboard=[['<', '>']])

    while telegram_manager.run:
        sleep(0.5)

    LOGGER.info('end')





import pytz
import requests
import re

from time import time, sleep
from datetime import datetime
from random import choice, shuffle
from threading import Timer
from src.Tools.library import get_exception, is_number, to_int, int_timestamp_now, timestamp2date, get_human_date_from_timestamp
from src.Telegram.TelegramManager import TelegramManager


class Quotes(TelegramManager):

    FUNCTIONS = {'show_quotes': {'showQuotes', 'library'},
                 'search_for_word': {'search'},
                 'modify_settings': {'settings'},
                 'random_quote': {'quote', 'random'}
                 }

    ADMIN_FUNCTIONS = {'new_quote': {'new', 'add'},
                       'edit_quote': {'edit'},
                       'add_translation_to_quote': {'Translation'},
                       'trigger_daily_quote': {'triggerDaily'},
                       'delete_quote': {'delete'},
                       'add_admin_to_db': {'addAdmin'},
                       'show_db_users': {'showDBUsers'},
                       'new_note': {'note'},
                       'show_notes': {'Notes', 'showNotes'},
                       'search_note_by_tag': {'searchNote'},
                       'show_tables': {'tables'},
                       'toggle_daily_quote': {'dailyQuote'},
                       'toggle_super_user': {'superUser'},
                       'last_added': {'last'}}

    SIMPLE_SEND = {}

    @staticmethod
    def default_settings():

        return {'auto_detect': False,
                'show_counter': False,
                'daily_quote': True,
                'only_favorites': False,
                'super_user': False,
                'language': 'ENG'}

    @staticmethod
    def get_quote_in_language(quote, user):
        quote_in_language = quote['quote_ita'] if 'language' in user.settings and user.settings['language'] == 'ITA' else quote['quote']
        return quote_in_language if quote_in_language else quote['quote']

    def __init__(self, telegram_token_var='QUOTES_TOKEN', **kwargs):

        self.daily_quote = False
        self.base_classes = [x.__name__ for x in self.__class__.__bases__]

        self.functions = {**self.FUNCTIONS, **TelegramManager.DEFAULT_FUNCTIONS, **TelegramManager.CORE_DEFAULT_FUNCTIONS}
        self.admin_functions = {**self.ADMIN_FUNCTIONS, **TelegramManager.DEFAULT_ADMIN_FUNCTIONS, **TelegramManager.CORE_DEFAULT_ADMIN_FUNCTIONS}
        self.simple_send = {**self.SIMPLE_SEND, **TelegramManager.DEFAULT_SIMPLE_SEND, **TelegramManager.CORE_DEFAULT_SIMPLE_SEND}

        super().__init__(caller="Quotes", init_tables=self.init_tables, telegram_token_var=telegram_token_var, **kwargs)

        quotes_settings = self.load(key="quotes_settings")
        self.quotes_settings = self.default_settings() if not quotes_settings else quotes_settings

        if self.quotes_settings and 'daily_quote' in self.quotes_settings and self.quotes_settings['daily_quote']:
            self.__init_quote_timer()

        self.__init_note_timer()

    # __ initialization __
    def init_tables(self, postgre_manager, tables):

        if 'users' not in tables:
            self.logger.info('Creating users table...')
            query = """
                    CREATE TABLE users (
                        telegram_id BIGINT PRIMARY KEY,
                        name VARCHAR(255) NOT NULL,
                        username VARCHAR(255) NOT NULL,
                        is_admin BOOLEAN,
                        auto_detect BOOLEAN,
                        show_counter BOOLEAN,
                        daily_quote BOOLEAN,
                        only_favorites BOOLEAN,
                        language VARCHAR(255),
                        created BIGINT,
                        last_modified BIGINT
                    )
                    """
            postgre_manager.create_query(query, commit=True)
        if 'quotes' not in tables:
            self.logger.info('Creating quotes table...')
            query = """
                    CREATE TABLE quotes (
                        quote_id SERIAL PRIMARY KEY,
                        quote TEXT NOT NULL,
                        author VARCHAR(255) NOT NULL,
                        translation TEXT,
                        last_random_tme BIGINT,
                        telegram_id BIGINT NOT NULL,
                        private BOOLEAN,
                        created BIGINT,
                        last_modified BIGINT,
                        UNIQUE (quote, author),
                        FOREIGN KEY (telegram_id)
                            REFERENCES users (telegram_id)
                            ON UPDATE CASCADE
                    )
                    """
            postgre_manager.create_query(query, commit=True)
        if 'notes' not in tables:
            self.logger.info('Creating notes table...')
            query = """
                    CREATE TABLE notes (
                        note_id SERIAL PRIMARY KEY,
                        note TEXT NOT NULL,
                        telegram_id BIGINT NOT NULL,
                        private BOOLEAN,
                        created BIGINT,
                        last_modified BIGINT,
                        FOREIGN KEY (telegram_id)
                            REFERENCES users (telegram_id)
                            ON UPDATE CASCADE ON DELETE CASCADE
                    )
                    """
            postgre_manager.create_query(query, commit=True)
        if 'favorites' not in tables:
            self.logger.info('Creating favorites table...')
            query = """
                    CREATE TABLE favorites (
                        quote_id INTEGER,
                        telegram_id BIGINT,
                        PRIMARY KEY (quote_id, telegram_id),
                        FOREIGN KEY (telegram_id)
                            REFERENCES users (telegram_id)
                            ON UPDATE CASCADE ON DELETE CASCADE,
                        FOREIGN KEY (quote_id)
                            REFERENCES quotes (quote_id)
                            ON UPDATE CASCADE ON DELETE CASCADE
                    )
                    """
            postgre_manager.create_query(query, commit=True)
        if 'tags' not in tables:
            self.logger.info('Creating tags table...')
            query = """
                    CREATE TABLE tags (
                        tag_id SERIAL PRIMARY KEY,
                        tag VARCHAR(255),
                        quote_id INTEGER,
                        note_id INTEGER,
                        UNIQUE (tag, quote_id, note_id)
                    )
                    """
            postgre_manager.create_query(query, commit=True)

    def __init_quote_timer(self):
        dt = datetime.now(pytz.timezone('Europe/Rome'))
        eta = ((9 - dt.hour - 1) * 60 * 60) + ((60 - dt.minute - 1) * 60) + (60 - dt.second)

        if eta < 0:
            eta += 24*60*60

        self.logger.info('Daily Quote set in ' + str(to_int(eta/3600)) + 'h:' + str(to_int((eta%3600)/60)) + 'm:' + str(to_int(((eta%3600)%60))) + 's:')

        self.quote_timer = Timer(eta, self.__daily_quote)
        self.quote_timer.name = 'Daily Quote'
        self.quote_timer.start()
        self.daily_quote = True
        self.quotes_settings['daily_quote'] = True

    def __init_note_timer(self):
        eta_1 = self.__build_eta(target_hour=12, target_minute=00)
        eta_2 = self.__build_eta(target_hour=18, target_minute=00)

        self.logger.info('Next Note 1 set in ' + str(to_int(eta_1/3600)) + 'h:' + str(to_int((eta_1 % 3600)/60)) + 'm:' + str(to_int(((eta_1 % 3600) % 60))) + 's:')
        self.logger.info('Next Note 2 set in ' + str(to_int(eta_2/3600)) + 'h:' + str(to_int((eta_2 % 3600)/60)) + 'm:' + str(to_int(((eta_2 % 3600) % 60))) + 's:')

        self.note_timer_eta_1 = Timer(eta_1, self.__daily_note)
        self.note_timer_eta_1.name = 'Next Note 1'
        self.note_timer_eta_1.start()

        self.note_timer_eta_2 = Timer(eta_2, self.__daily_note)
        self.note_timer_eta_2.name = 'Next Note 2'
        self.note_timer_eta_2.start()

    def __init_keyboards(self):

        self.keyboard = [['Quote'], ['Library', 'Search'], ['Settings', 'Advanced']]
        self.advanced_keyboard = [['Add', 'Edit', 'Delete', 'Translation'], ['Last', 'dailyQuote', 'tables', 'triggerDaily'], ['showDBUsers', 'addUser'], ['Back']]
        self.current_keyboard = self.keyboard.copy()

    def init_keyboards(self):
        self.__init_keyboards()

    # _____ init commands _____
    def __init_commands(self):

        self.all_commands = []

        for x in self.functions:
            for cmd in self.functions[x]:
                self.all_commands.append(cmd.lower())

        for x in self.admin_functions:
            for cmd in self.admin_functions[x]:
                self.all_commands.append(cmd.lower())

        for x in self.simple_send:
            self.all_commands.append(x.lower())

    def init_commands(self):
        self.__init_commands()

    # __ closing __
    def close(self):
        self.close_quotes()

    def close_quotes(self):
        self.__close_quote_timer()
        self.__close_note_timer()
        self.save(key="quotes_settings", data=self.quotes_settings)

        if 'TelegramManager' in self.base_classes:
            self.close_telegram_manager()
        if 'CoreApp' in self.base_classes:
            self.close_core_app()

    def __close_quote_timer(self):
        try:
            self.quote_timer.cancel()
        except:
            pass

    def __close_note_timer(self):
        try:
            self.note_timer_eta_1.cancel()
        except:
            pass
        try:
            self.note_timer_eta_2.cancel()
        except:
            pass

    # _____ call message _____
    def call_message(self, user_x, message):

        if '\nby ' in message:

            txt = message.split('by ')

            author = txt.pop().replace('_', ' ')

            if len(txt) > 1:
                txt = 'by '.join(txt)
            else:
                txt = txt[0]

            if 'Forwarded' in txt:
                txt = txt.split('\n')
                txt.pop(0)
                quote = '\n'.join(txt)
            else:
                quote = txt

            while quote[-1] == '\n':
                quote = quote[:-1]

            self.__insert_quote(user_x, quote, author)
            return True

        elif user_x.is_admin:

            user_x.reset()

            user_x.name_function = 'note'
            user_x.function_variables['note'] = message

            self.logger.debug(message + ' by ' + user_x.name + ' ' + str(user_x.id))

            return self.new_note(user_x)

        self.send_message(user_x=user_x, text='No command running')
        return False

    # _____ Quotes _____
    def toggle_daily_quote(self, user_x):

        if self.daily_quote and self.quote_timer:
            try:
                self.quote_timer.cancel()
                self.logger.info('Daily Quote canceled')
                self.send_message(user_x=user_x, text='Daily Quote canceled')
                self.daily_quote = False
                self.quotes_settings['daily_quote'] = False
            except:
                self.logger.error('Unable to delete Daily Quote')
                self.send_message(user_x=user_x, text='Unable to delete Daily Quote')
            return

        self.__init_quote_timer()
        self.send_message(user_x=user_x, text='Daily Quote enabled')

    def __daily_quote(self, trigger=False):

        query = """
                SELECT *
                FROM quotes
                ORDER BY last_random_tme
                LIMIT 100
                """
        quotes = self.postgre_manager.select_query(query=query)
        if len(quotes) == 0:
            return None

        quote = choice(quotes)

        self.__update_quote_by_quote_id(quote_id=quote['quote_id'], set_params={'last_random_tme': to_int(time())}) if not trigger else None

        for user in self.users:
            if user.settings['daily_quote']:
                text = '*Quote of the day*\n\n' + self.get_quote_in_language(quote=quote, user=user) + '\n\n_' + quote['author'].replace('_', ' ') + '_'
                try:
                    if not self.send_message(user_x=user, text=text, parse_mode="Markdown", bypass_inline=True):
                        self.logger.warning('Unable to send quote to {}: {}'.format(user.name, user.id))
                    sleep(0.2)
                except:
                    self.logger.error(get_exception())

    def trigger_daily_quote(self, user_x):
        self.__daily_quote()

    def new_quote(self, user_x):

        # STATE 0
        if user_x.state_function == 0:

            txt = 'Insert Quote'

            self.send_message(user_x, text=txt, append_done=True, accept_commands=False)

            return user_x.next()

        # STATE 1
        if user_x.state_function == 1:

            quote = user_x.last_message
            user_x.function_variables['quote'] = quote

            txt = 'Insert Author'

            self.send_message(user_x, text=txt, append_done=True, accept_commands=False)

            return user_x.next()

        # STATE 2
        if user_x.state_function == 2:

            author = user_x.last_message
            quote = user_x.function_variables['quote']

            self.__insert_quote(user_x, quote, author)

        return user_x.back_to_master()

    def random_quote(self, user_x):

        query = """
                SELECT *
                FROM quotes
                """
        quotes = self.postgre_manager.select_query(query=query)
        if len(quotes) == 0:
            self.send_message(user_x=user_x, text='No quotes saved in DB')
            return self.back_to_master(user_x)

        quote = choice(quotes)
        text = self.get_quote_in_language(quote=quote, user=user_x) + '\n\n_' + quote['author'].replace('_', ' ') + '_'
        self.send_message(user_x=user_x, text=text, parse_mode="Markdown")

        return self.back_to_master(user_x)

    def show_quotes(self, user_x):

        # STATE 0
        if user_x.state_function == 0:

            query = f"""SELECT Q.quote_id 
                        FROM quotes Q
                        JOIN favorites F ON F.quote_id = Q.quote_id
                        WHERE F.telegram_id = {user_x.id}""" if 'only_favorites' in user_x.settings and user_x.settings['only_favorites'] else "SELECT quote_id FROM quotes"

            quotes_ids = self.postgre_manager.select_query(query=query)

            if len(quotes_ids) == 0:
                self.send_message(user_x=user_x, text='No Quotes in DB')
                return user_x.reset()

            shuffle(quotes_ids)

            user_x.function_variables['quotes_ids'] = quotes_ids
            user_x.function_variables['index'] = 0

            return self.system_automessage(user_x.next())

        # STATE 1
        if user_x.state_function == 1:

            if user_x.previous_state_function == user_x.state_function:

                action = user_x.last_message
                if action == '<':
                    user_x.function_variables['index'] -= 1
                elif action == '>':
                    user_x.function_variables['index'] += 1
                elif action == '>>':
                    user_x.function_variables['index'] = min(user_x.function_variables['index'] + 10, len(user_x.function_variables['quotes_ids']) - 1)
                elif action == '<<':
                    user_x.function_variables['index'] = max(user_x.function_variables['index'] - 10, 0)
                elif action == 'AddFavorite':
                    self.__insert_favorite(quote_id=user_x.function_variables['quotes_ids'][user_x.function_variables['index']]['quote_id'],
                                           user_id=user_x.id)
                    self.send_callback(user_x=user_x, text='Added to favorites')

                elif action == 'RemoveFavorite':
                    self.__delete_favorite(quote_id=user_x.function_variables['quotes_ids'][user_x.function_variables['index']]['quote_id'],
                                           user_id=user_x.id)
                    self.send_callback(user_x=user_x, text='Removed from favorites')

                elif action == 'addTranslation' and user_x.settings['super_user']:
                    user_x.accept_messages = True
                    return self.system_automessage(user_x.next())
                    # self.send_callback(user_x=user_x, text='Function not present')
                    # user_x.nest_function(name='library', nxt=1, prev=0)
                    # return self.system_automessage(user_x.next_function(name='add_translation_to_quote', nxt=1, reserved=True))

            index = user_x.function_variables['index']

            keyboard = []
            if index > 0:
                keyboard.append('<<')
                keyboard.append('<')
            if index < len(user_x.function_variables['quotes_ids']) - 1:
                keyboard.append('>')
                keyboard.append('>>')

            params = {'quote_id': user_x.function_variables['quotes_ids'][index]['quote_id']}

            quote = self.__find_quotes(params)[0]

            text = '_' + str(index + 1) + '/' + str(len(user_x.function_variables['quotes_ids'])) + '_' + '\n\n' if user_x.settings['show_counter'] else ''
            text += self.get_quote_in_language(quote=quote, user=user_x) + '\n\n_' + quote['author'].replace('_', ' ') + '_'

            if self.__is_favorite(quote_id=quote['quote_id'], telegram_id=user_x.id):
                keyb = [['RemoveFavorite', 'addTranslation']] if user_x.settings['super_user'] and quote['quote_ita'] is None else [['RemoveFavorite']]
            else:
                keyb = [['AddFavorite', 'addTranslation']] if user_x.settings['super_user'] and quote['quote_ita'] is None else [['AddFavorite']]
            if len(keyboard) > 0:
                keyb.append(keyboard)

            self.send_message(user_x=user_x, text=text,
                              parse_mode="Markdown", keyboard=keyb, accept_messages=False)

            return user_x.same()

        # STATE 2
        if user_x.state_function == 2:

            txt = user_x.last_inline_text + '\n\nInsert Italian translation'

            self.send_message(user_x, text=txt, parse_mode="Markdown", accept_commands=False)

            return user_x.next()

        # STATE 3
        if user_x.state_function == 3:

            translation = user_x.last_message
            quote_id = int(user_x.function_variables['quotes_ids'][user_x.function_variables['index']]['quote_id'])

            if self.__update_quote_by_quote_id(quote_id=quote_id, set_params={'quote_ita': translation}):
                self.logger.info('Added translation to quote id {} by: '.format(quote_id) + user_x.name + ' ' + str(user_x.id))
                self.send_message(user_x=user_x, text='Translation added', end_keyboard=self.keyboard)
            else:
                self.logger.info('Failed to add translation to quote id {} by: '.format(quote_id) + user_x.name + ' ' + str(user_x.id))
                self.send_message(user_x=user_x, text='Failed to add translation', end_keyboard=self.keyboard)

            return self.system_automessage(user_x.back(steps=2))

    def search_for_word(self, user_x):

        # STATE 0
        if user_x.state_function == 0:

            txt = 'Insert Word'

            self.send_message(user_x, text=txt, accept_commands=False)

            return user_x.next()

        # STATE 1
        if user_x.state_function == 1:

            word = user_x.last_message
            words = [word]

            if 'auto_detect' in user_x.settings and user_x.settings['auto_detect']:

                base_url = 'https://translate.yandex.net/api/v1.5/tr.json/detect?key=trnsl.1.1.20200524T110101Z.b8b25436dd6033df.f75e4dc7543bf925ac01e4376e48185b462cf5b4&text='

                r = requests.get(base_url + word)
                lang = r.json()['lang']

                if lang != 'en':

                    r = requests.get('https://api.mymemory.translated.net/get?q=' + word + '&langpair=' + lang + '|en')
                    result = r.json()

                    words = [x['translation'] for x in result['matches'] if ' ' not in x['translation'] and x['translation'].upper() != word.upper()]
                    words.append(word)

            where = '%\' OR UPPER(quote) LIKE \'%'.join([x.upper() for x in words])
            where_author = '%\' OR UPPER(author) LIKE \'%'.join([x.upper() for x in words])
            user_x.function_variables['quotes_ids'] = self.postgre_manager.select_query(f"SELECT quote_id FROM quotes WHERE (UPPER(quote) LIKE '%{where}%') OR (UPPER(author) LIKE '%{where_author}%')")

            if len(user_x.function_variables['quotes_ids']) == 0:
                self.send_message(user_x=user_x, text="No matches in database")
            elif len(user_x.function_variables['quotes_ids']) == 1:
                for el in self.postgre_manager.select_query(f"SELECT * FROM quotes WHERE quote_id = {user_x.function_variables['quotes_ids'][0]['quote_id']}"):
                    self.send_message(user_x=user_x, text=el['quote'] + '\n\n_' + el['author'].replace('_', ' ') + '_', parse_mode="Markdown")
            else:
                user_x.function_variables['index'] = 0
                return self.system_automessage(user_x.next_function(name='showQuotes', nxt=1, prev=0))

        return user_x.back_to_master()

    def edit_quote(self, user_x):

        # STATE 0
        if user_x.state_function == 0:

            txt = 'Insert Word'

            self.send_message(user_x, text=txt, accept_commands=False)

            return user_x.next()

        # STATE 1
        if user_x.state_function == 1:

            word = user_x.last_message

            quotes = self.postgre_manager.select_query(f"SELECT * FROM quotes WHERE quote LIKE '%{word}%' LIMIT 10")

            if len(quotes) == 0:
                self.send_message(user_x=user_x, text="No matches in database")
                return user_x.back_to_master()

            for el in quotes:
                self.send_message(user_x=user_x, text='*' + str(el['quote_id']) + '*\n' + el['quote'] + '\n\n_' + el['author'].replace('_', ' ') + '_', parse_mode="Markdown",
                                  accept_commands=False)
                sleep(0.1)

            user_x.function_variables['quotes'] = {x['quote_id']: x for x in quotes}

            text = 'Select Quote to edit'

            self.send_message(user_x=user_x, text=text, keyboard=self.square_keyboard(list([x['quote_id'] for x in quotes])),
                              bypass_inline=True, append_done=True, accept_commands=False)

            return user_x.next()

        # STATE 2
        if user_x.state_function == 2:

            index = user_x.last_message

            if not (is_number(index) and int(index) in user_x.function_variables['quotes']):
                self.send_callback(user_x, text='Wrong Entry')
                return self.go_back(user_x=user_x)

            user_x.function_variables['index'] = index

            text = 'Insert Quote'

            self.send_message(user_x, text=text, accept_commands=False)

            return user_x.next()

        # STATE 3
        if user_x.state_function == 3:

            cit = user_x.last_message
            user_x.function_variables['quote'] = cit

            txt = 'Insert Author'

            self.send_message(user_x, text=txt, accept_commands=False)

            return user_x.next()

        # STATE 4
        if user_x.state_function == 4:

            author = user_x.last_message
            quote = user_x.function_variables['quote']

            self.__update_quote_by_quote_id(quote_id=int(user_x.function_variables['index']), set_params={'quote': quote, 'author': author})

            self.logger.info('Quote edited by: ' + user_x.name + ' ' + str(user_x.id))
            self.send_message(user_x=user_x, text='Quote edited', end_keyboard=self.keyboard)

        return user_x.back_to_master()

    def add_translation_to_quote(self, user_x):

        if not user_x.settings['super_user']:
            self.send_message(user_x=user_x, text='You must be super user to execute this function', end_keyboard=self.keyboard)
            return user_x.back_to_master()

        # STATE 0
        if user_x.state_function == 0:

            txt = 'Search quote: insert Word'

            self.send_message(user_x, text=txt, accept_commands=False)

            return user_x.next()

        # STATE 1
        if user_x.state_function == 1:

            word = user_x.last_message

            quotes = self.postgre_manager.select_query(f"SELECT * FROM quotes WHERE quote LIKE '%{word}%' LIMIT 10")

            if len(quotes) == 0:
                self.send_message(user_x=user_x, text="No matches in database")
                return user_x.back_to_master()

            for el in quotes:
                self.send_message(user_x=user_x, text='*' + str(el['quote_id']) + '*\n' + el['quote'] + '\n\n_' + el['author'].replace('_', ' ') + '_', parse_mode="Markdown",
                                  accept_commands=False)
                sleep(0.1)

            user_x.function_variables['quotes'] = {x['quote_id']: x for x in quotes}

            text = 'Select Quote to add translation to'

            self.send_message(user_x=user_x, text=text, keyboard=self.square_keyboard(list([x['quote_id'] for x in quotes])),
                              bypass_inline=True, append_done=True, accept_commands=False)

            return user_x.next()

        # STATE 2
        if user_x.state_function == 2:

            index = user_x.last_message

            if not (is_number(index) and int(index) in user_x.function_variables['quotes']):
                self.send_callback(user_x, text='Wrong Entry')
                return self.go_back(user_x=user_x)

            user_x.function_variables['index'] = index

            text = 'Insert Translation'

            self.send_message(user_x, text=text, accept_commands=False)

            return user_x.next()

        # STATE 3
        if user_x.state_function == 3:

            quote = user_x.last_message

            self.__update_quote_by_quote_id(quote_id=int(user_x.function_variables['index']), set_params={'quote_ita': quote})

            self.logger.info('Added translation to quote by: ' + user_x.name + ' ' + str(user_x.id))
            self.send_message(user_x=user_x, text='Translation added', end_keyboard=self.keyboard)

        return user_x.back_to_master()

    def delete_quote(self, user_x):

        # STATE 0
        if user_x.state_function == 0:

            txt = 'Insert Word'

            self.send_message(user_x, text=txt)

            return user_x.next()

        # STATE 1
        if user_x.state_function == 1:

            word = user_x.last_message

            quotes = self.postgre_manager.select_query(f"SELECT * FROM quotes WHERE quote LIKE '%{word}%' LIMIT 10")

            if len(quotes) == 0:
                self.send_message(user_x=user_x, text="No matches in database")
                return user_x.back_to_master()

            for el in quotes:
                self.send_message(user_x=user_x, text='*' + str(el['quote_id']) + '*\n' + el['quote'] + '\n\n_' + el['author'].replace('_', ' ') + '_', parse_mode="Markdown",
                                  accept_commands=False)
                sleep(0.1)

            user_x.function_variables['quotes'] = {x['quote_id']: x for x in quotes}

            text = 'Select Quote to delete'

            self.send_message(user_x=user_x, text=text, keyboard=self.square_keyboard(list([x['quote_id'] for x in quotes])),
                              bypass_inline=True, append_done=True, accept_commands=False)

            return user_x.next()

        # STATE 2
        if user_x.state_function == 2:

            index = user_x.last_message

            if not (is_number(index) or (is_number(index) and int(index) not in user_x.function_variables['quotes'])):
                self.send_callback(user_x, text='Wrong Entry')
                return self.system_automessage(user_x.back())

            self.postgre_manager.delete_query(f"DELETE FROM quotes WHERE quote_id = {int(index)}", commit=True)

            self.logger.info('Quote deleted by: ' + user_x.name + ' ' + str(user_x.id))
            self.send_message(user_x=user_x, text='Quote deleted', keyboard=self.keyboard)

        return user_x.back_to_master()

    def last_added(self, user_x):

        # STATE 0
        if user_x.state_function == 0:

            query = "SELECT * FROM USERS"
            users = self.postgre_manager.select_query(query=query)

            if len(users) == 0:
                self.send_message(user_x=user_x, text='No users in DB')
                return user_x.reset()

            text = 'Which user?'
            keyboard = self.square_keyboard([x['name'] + ' ' + str(x['telegram_id']) for x in users])
            self.send_message(user_x, text=text, keyboard=keyboard)

            user_x.function_variables['users'] = users

            return user_x.next()

        # STATE 1
        if user_x.state_function == 1:

            users = user_x.function_variables['users']
            user_id = [x['telegram_id'] for x in users if x['name'] + ' ' + str(x['telegram_id']) in user_x.last_message][0]

            query = f"""SELECT * FROM QUOTES WHERE TELEGRAM_ID = {user_id} ORDER BY CREATED DESC LIMIT 1"""
            quotes = self.postgre_manager.select_query(query=query)

            if len(quotes) == 0:
                self.send_message(user_x=user_x, text='No quotes in DB for this user')
                return user_x.reset()

            text = quotes[0]['quote'] + '\n\n_' + quotes[0]['author'].replace('_', ' ') + '_'
            self.send_message(user_x=user_x, text=text, parse_mode="Markdown")

            return self.back_to_master(user_x)

    # _____ Notes _____
    def __daily_note(self, trigger=False):
        query = """
                (
                SELECT *
                FROM notes
                WHERE is_book = TRUE
                AND last_random_time = 1
                ORDER BY RANDOM()
                LIMIT 50
                )
                UNION
                (
                SELECT *
                FROM notes
                WHERE is_book = TRUE
                AND last_random_time > 1
                ORDER BY last_random_time
                LIMIT 100
                )
                """
        notes = self.postgre_manager.select_query(query=query)
        if len(notes) == 0:
            return None

        note = choice(notes)
        full_note = self.__get_note_by_id(note_id=note['note_id'])
        text = self.__build_note(note=full_note)

        for user in self.users:
            if user.settings['daily_book']:
                try:
                    text = f'*Book note*\n\n{text}'
                    if not self.send_message(user_x=user, text=text, parse_mode="Markdown", bypass_inline=True):
                        self.logger.warning('Unable to send book note to {}: {}'.format(user.name, user.id))
                    sleep(0.2)
                except:
                    self.logger.error(get_exception())

        # admin_user = [x for x in self.users if x.is_admin or x.daily_book][0]
        # self.send_message(user_x=admin_user, text=text, parse_mode='Markdown')
        self.__update_note_by_note_id(note_id=note['note_id'], set_params={'last_random_time': to_int(time())}) if not trigger else None

    @staticmethod
    def __build_note(note, index=0, user_x=None):
        show_counter = f"_{index + 1}/{len(user_x.function_variables['notes'])}_\n\n" if user_x and 'show_counter' in user_x.settings and user_x.settings['show_counter'] else ''
        pag = f" - pag. {note['pag']}" if 'pag' in note and note['pag'] else ""
        book = f"_Book: {note['book']}{pag}_\n" if 'book' in note and note['book'] else ""
        tags = f"_Tags: {', '.join(note['tags'])}_" if 'tags' in note and len(note['tags']) > 0 else "_No tags_"
        creation_data = '_Creation date: {}_'.format(get_human_date_from_timestamp(note['created']))
        text = f"{show_counter}{note['note']}\n\n{book}{tags}\n{creation_data}"
        return text

    @staticmethod
    def __build_navigation_keyboard(index, len_):

        keyboard = []
        if index > 0:
            keyboard.append('<<')
            keyboard.append('<')
        if index < len_ - 1:
            keyboard.append('>')
            keyboard.append('>>')

        return keyboard

    def __get_last_tags(self, max_tags: int = 9):
        notes = self.__get_notes_with_tags()
        sorted_notes = sorted(notes, key=lambda d: d['created'], reverse=True)
        tags = []
        for note in sorted_notes:
            tags += note['tags']
        set_tags = []
        for tag in tags:
            if tag not in set_tags:
                set_tags += [tag]
        if len(set_tags) > max_tags:
            set_tags = set_tags[:max_tags]

        return set_tags

    def __get_last_books(self, max_books: int = 4):
        notes = self.__get_notes()
        sorted_notes = sorted(notes, key=lambda d: d['created'], reverse=True)
        # books = list(set([x['book'] for x in sorted_notes if x['book']]))
        books = set()
        books_add = books.add
        books = [x['book'] for x in sorted_notes if not (x['book'] in books or books_add(x['book']))]

        if len(books) > max_books:
            books = books[:max_books]

        return books

    def __get_last_page(self, book: str = None) -> int:
        notes = self.__get_notes()
        sorted_notes = sorted(notes, key=lambda d: d['created'], reverse=True)

        if book:
            sorted_notes = [x for x in sorted_notes if x['book'] == book]
            if len(sorted_notes) > 0:
                pages = [int(x['pag']) for x in sorted_notes if x['pag']]
                return max(pages) if len(pages) > 0 else 1
            return 1

        last_pages = [x for x in sorted_notes if x is not None]
        if len(last_pages) > 0:
            return last_pages[0]

        return 1

    def new_note(self, user_x):
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

    def search_note_by_tag(self, user_x):

        # STATE 0
        if user_x.state_function == 0:

            txt = 'Insert Tag'

            self.send_message(user_x, text=txt, accept_commands=False)

            return user_x.next()

        # STATE 1
        if user_x.state_function == 1:

            tag = user_x.last_message

            user_x.function_variables['notes'] = self.__get_notes_by_tag(tag=tag)

            if len(user_x.function_variables['notes']) == 0:
                self.send_message(user_x=user_x, text="No matches in database")
            elif len(user_x.function_variables['notes']) == 1:
                text = self.__build_note(note=user_x.function_variables['notes'][0], index=0, user_x=user_x)
                self.send_message(user_x=user_x, text=text, parse_mode='Markdown')
            else:
                user_x.function_variables['index'] = 0
                return self.go_forward(user_x=user_x)

            return self.back_to_master(user_x)

        # STATE 2
        if user_x.state_function == 2:

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

    def show_notes(self, user_x):
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

    # _____ Settings _____
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

    def toggle_super_user(self, user_x):

        # STATE 0
        if user_x.state_function == 0:

            users = self.get_users_from_db()
            user_x.function_variables['users'] = users

            if not users:
                self.send_message(user_x=user_x, text='No user in DB or connection not established')
                return self.back_to_master(user_x)

            txt = 'Users\n\n'

            for user in users:
                txt += user['name'] + ' ' + str(user['telegram_id']) + ' - isSuperUser? ' + str(user['super_user']) + '\n'

            txt += '\nSelect user ID'
            keyboard = self.square_keyboard([str(x.id) for x in self.users])
            self.send_message(user_x=user_x, text=txt, keyboard=keyboard)
            return user_x.next()

        # STATE 1
        if user_x.state_function == 1:

            user_id = int(user_x.last_message)

            if user_id not in [x.id for x in self.users]:
                self.send_message(user_x=user_x, text='Wrong Entry')
                return user_x.same()

            user = [x for x in user_x.function_variables['users'] if x['telegram_id'] == user_id][0]
            user_name = user['name']
            settings = {x: user[x] for x in user if x in self.default_settings()}
            settings['super_user'] = not settings['super_user']

            user_index = next((index for (index, d) in enumerate(self.users) if d.id == user_id), None)
            self.users[user_index].settings = settings
            self.update_db_user_settings(user_id=user_id, settings=settings) if self.postgre_manager else None

            txt = "superUser attribute removed" if user['super_user'] else "superUser attribute granted"

            self.send_callback(user_x=user_x, text=txt)

            return self.go_back(user_x)

        return self.back_to_master(user_x)

    # _____ DB: Quotes Collection _____
    def __find_quotes(self, params):
        params = ' AND '.join([key + f" = '{params[key]}'" if type(params[key]) is str else key + f" = {params[key]}" for key in params])
        return self.postgre_manager.select_query(f"SELECT * FROM quotes WHERE {params}")

    def __insert_quote(self, user_x, quote, author, translation=None, private=True, tags=None):
        # ___ Check if there are similar quotes ___
        words = re.findall(r'\w+', quote)
        where = '%\' AND UPPER(quote) LIKE \'%'.join([x.upper() for x in words])
        quotes = self.postgre_manager.select_query(f"SELECT * FROM quotes WHERE (UPPER(quote) LIKE '%{where}%')")

        if len(quotes) > 0:
            text = 'There is already a similar quote in DB:\n\n' + quotes[0]['quote'] + '\n\n_' + quotes[0]['author'].replace('_', ' ') + '_'
            self.send_message(user_x=user_x, text=text, parse_mode="Markdown")
            return False

        query = f"""
                INSERT INTO quotes
                (quote, author, translation, last_random_tme, telegram_id, private, created, last_modified)
                VALUES
                ($${quote}$$, $${author}$$, $${translation}$$, {int_timestamp_now()}, {user_x.id}, {private}, {int_timestamp_now()}, {int_timestamp_now()})
                """
        if not self.postgre_manager.insert_query(query=query, commit=True):
            self.send_message(user_x=user_x, text='Quote already present in DB', keyboard=self.keyboard)
            return False

        self.logger.warning('New quote added by: ' + user_x.name + ' ' + str(user_x.id))
        self.send_message(user_x=user_x, text='Quote added to DB')

        if not tags:
            return True

        quotes = self.__find_quotes({'quote': quote, 'author': 'author'})

        for tag in tags:
            self.__insert_tag(tag=tag, quote_id=quotes[0]['quote_id'])

        return True

    def __update_quote_by_quote_id(self, quote_id, set_params):

        set_params = ','.join([key + f" = '{set_params[key]}'" if type(set_params[key]) is str else key + f" = {set_params[key]}" for key in set_params])

        query = f"""
                 UPDATE quotes
                 SET {set_params}, last_modified = {int_timestamp_now()}
                 WHERE quote_id = {quote_id}
                 """
        updated = self.postgre_manager.update_query(query, commit=True)
        if updated:
            self.logger.warning('Quote id {} edited: '.format(quote_id))
        return updated

    def __is_favorite(self, quote_id, telegram_id):

        query = f"SELECT * FROM favorites WHERE quote_id = {quote_id} AND telegram_id = {telegram_id}"
        return len(self.postgre_manager.select_query(query)) > 0

    # _____ DB: Notes Collection _____
    def __insert_one_note(self, note: str, user_id, book: str = None, pag: int = None, tags=None):
        if book:
            query = f"""
                    INSERT INTO notes
                    (note, telegram_id, private, created, last_modified, last_random_time, is_book, book, pag)
                    VALUES
                    ($${note}$$, {user_id}, {True}, {int_timestamp_now()}, {int_timestamp_now()}, {1}, {True}, $${book}$$, {pag})
                    """
        else:
            query = f"""
                    INSERT INTO notes
                    (note, telegram_id, private, created, last_modified, last_random_time, is_book)
                    VALUES
                    ($${note}$$, {user_id}, {True}, {int_timestamp_now()}, {int_timestamp_now()}, {1}, {False})
                    """
        self.postgre_manager.insert_query(query=query, commit=True)

        query = f"""
                SELECT note_id 
                FROM notes 
                WHERE note = $${note}$$
                """
        note_id = self.postgre_manager.select_query(query=query)[0]['note_id']

        for tag in tags:
            self.__insert_tag(tag=tag, note_id=note_id)

    def __get_note_by_id(self, note_id):
        query = f"""SELECT * from notes N left join tags T on T.note_id = N.note_id where N.note_id = '{note_id}'"""
        results = self.postgre_manager.select_query(query=query)

        if not results:
            return []

        note = {key: results[0][key] for key in results[0] if key not in ['tag_id', 'tag', 'note_id']}
        note.update({'tags': [x['tag'] for x in results if x['tag']]})
        return note

    def __get_notes(self, query=None):
        query = f"""SELECT * from notes N""" if not query else query
        notes = self.postgre_manager.select_query(query=query)

        return notes

    def __get_notes_with_tags(self, query=None):
        query = f"""SELECT * from notes N join tags T on T.note_id = N.note_id""" if not query else query
        results = self.postgre_manager.select_query(query=query)

        if not results:
            return []

        notes_id = list(set([x['note_id'] for x in results]))

        notes = []
        for note_id in notes_id:
            temp_notes = [x for x in results if x['note_id'] == note_id]
            note = {key: temp_notes[0][key] for key in temp_notes[0] if key not in ['tag_id', 'tag', 'quote_id']}
            note.update({'tags': [x['tag'] for x in temp_notes]})
            notes.append(note)

        return notes

    def __get_notes_by_tag(self, tag):

        query = f"""SELECT * FROM notes N 
                    JOIN tags T ON T.note_id = N.note_id 
                    WHERE N.note_id IN (SELECT N.note_id from notes N join tags T on T.note_id = N.note_id where T.tag = '{tag}')"""
        return self.__get_notes_with_tags(query=query)

    def __update_note_by_note_id(self, note_id, set_params):
        set_params = ','.join([key + f" = '{set_params[key]}'" if type(set_params[key]) is str else key + f" = {set_params[key]}" for key in set_params])

        query = f"""
                 UPDATE notes
                 SET {set_params}, last_modified = {int_timestamp_now()}
                 WHERE note_id = {note_id}
                 """
        updated = self.postgre_manager.update_query(query, commit=True)
        if updated:
            self.logger.warning('Note id {} edited: '.format(note_id))
        return updated

    # ______ DB: Tags Collection _____
    def __insert_tag(self, tag, quote_id=None, note_id=None):

        if not quote_id and not note_id:
            return False

        name = 'quote_id' if quote_id else 'note_id'

        query = f"""
                INSERT INTO tags
                (tag, {name})
                VALUES
                ($${tag}$$, {quote_id if quote_id else note_id})
                """
        self.postgre_manager.insert_query(query=query, commit=True)

    # ______ DB: Favorites Collection _____
    def __insert_favorite(self, quote_id, user_id):

        query = f"""
                INSERT INTO favorites
                (quote_id, telegram_id)
                VALUES
                ({quote_id}, {user_id})
                """
        self.postgre_manager.insert_query(query=query, commit=True)

    def __delete_favorite(self, quote_id, user_id):

        query = f"""
                DELETE FROM favorites
                WHERE quote_id = {quote_id}
                AND telegram_id = {user_id}
                """
        self.postgre_manager.delete_query(query)

    @staticmethod
    def __build_eta(target_hour, target_minute=0):
        dt = datetime.now(pytz.timezone('Europe/Rome'))
        eta = ((target_hour - dt.hour - 1) * 60 * 60) + ((60 + target_minute - dt.minute - 1) * 60) + (60 - dt.second)
        if eta < 0:
            eta += 24*60*60
        return eta


if __name__ == '__main__':

    from src.Tools.library import run_main, get_environ
    from queue import Queue

    os_environ = get_environ() == 'HEROKU'
    sslmode = 'require' if os_environ else 'disable'
    postgre_key_var = 'DATABASE_URL' if os_environ else 'postgre_url'
    logging_queue = Queue()

    stock_app = Quotes(os_environ=os_environ, logging_queue=logging_queue, sslmode=sslmode, postgre_key_var=postgre_key_var, init_postgre_manager=True)
    if 'TelegramManager' not in stock_app.base_classes:
        stock_app.start_main_thread()

    run_main(stock_app, logging_queue)

