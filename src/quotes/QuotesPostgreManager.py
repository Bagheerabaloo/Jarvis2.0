import re
from typing import List, Type, Optional

from src.common.tools.library import class_from_args, int_timestamp_now
from src.common.postgre.PostgreManager import PostgreManager
from src.quotes.QuotesUser import QuotesUser
from src.quotes.Quote import Quote


class QuotesPostgreManager(PostgreManager):

    """ ____ DB: Users Collection _____"""
    @staticmethod
    def quote_user_from_dict(quote_user: dict) -> QuotesUser:
        return QuotesUser(telegram_id=quote_user["telegram_id"],
                          name=quote_user["name"],
                          username=quote_user["username"],
                          is_admin=quote_user["is_admin"],
                          auto_detect=quote_user["auto_detect"],
                          show_counter=quote_user["show_counter"],
                          only_favourites=quote_user["only_favourites"],
                          language=quote_user["language"],
                          super_user=quote_user["super_user"],
                          daily_quotes=quote_user["daily_quotes"],
                          daily_book=quote_user["daily_book"])

    def get_quotes_users(self) -> List[QuotesUser]:
        query = """
                SELECT * 
                FROM quotes_users Q 
                JOIN telegram_users T ON Q.telegram_id = T.telegram_id
                """
        quotes_users = self.select_query(query=query)
        if not quotes_users or len(quotes_users) == 0:
            telegram_admin_user = self.get_telegram_admin_user()
            if not telegram_admin_user:
                return []
            new_quote_user = QuotesUser(telegram_id=telegram_admin_user.telegram_id, name=telegram_admin_user.name, username=telegram_admin_user.username, is_admin=telegram_admin_user.is_admin)
            self.add_quotes_user_to_db(new_quote_user)
            return [new_quote_user]
        return [class_from_args(QuotesUser, x) for x in quotes_users]

    def add_quotes_user_to_db(self, user: QuotesUser, commit: bool = True) -> bool:
        query = f"""
                INSERT INTO quotes_users
                (telegram_id, language, created, last_modified)
                VALUES
                ({user.telegram_id}, $${user.language}$$, {int_timestamp_now()}, {int_timestamp_now()})
                """
        return self.insert_query(query=query, commit=commit)
        # self.logger.info('Quote user created')

    def update_db_user_setting(self, user: QuotesUser, attribute, commit: bool = True) -> bool:
        value = user.get_attribute(attribute=attribute)
        query = f"""
                 UPDATE quotes_users
                 SET {attribute} = {value}, last_modified = {int_timestamp_now()}
                 WHERE telegram_id = {user.telegram_id}
                 """
        return self.update_query(query, commit=commit)

    """ ____ DB: Quotes Collection _____"""
    def get_quotes(self, params: dict) -> List[Quote]:
        params = ' AND '.join([key + f" = '{params[key]}'" if type(params[key]) is str else key + f" = {params[key]}" for key in params])
        quotes = self.select_query(f"SELECT * FROM quotes WHERE {params}")
        return [class_from_args(Quote, x) for x in quotes] if quotes else []

    def get_last_random_quotes(self) -> List[Quote]:
        query = """
                SELECT *
                FROM quotes
                ORDER BY last_random_tme
                LIMIT 100
                """
        quotes = self.select_query(query=query)
        return [class_from_args(Quote, x) for x in quotes] if quotes else []

    def get_quotes_with_tags(self) -> List[Quote]:
        query = f"""
                SELECT *
                FROM quotes Q 
                LEFT JOIN (SELECT quote_id AS id FROM quotes) TEMP ON TEMP.id = Q.quote_id
                LEFT JOIN tags T ON Q.quote_id = T.quote_id
                """
        results = self.select_query(query=query)
        if not results:
            return []

        quotes_id = list(set([x['id'] for x in results]))

        quotes = []
        for quote_id in quotes_id:
            temp_quotes = [x for x in results if x['id'] == quote_id]
            quote = {key: temp_quotes[0][key] for key in temp_quotes[0] if key not in ['tag_id', 'tag', 'note_id', 'quote_id']}
            quote.update({'tags': [x['tag'] for x in temp_quotes if x["tag"]]})
            quote['quote_id'] = quote.pop('id')
            quotes.append(quote)

        return [class_from_args(Quote, x) for x in quotes]

    def check_for_similar_quotes(self, quote: str) -> List[Quote]:
        words = re.findall(r'\w+', quote)
        where = '%\' AND UPPER(quote) LIKE \'%'.join([x.upper() for x in words])
        quotes = self.select_query(f"SELECT * FROM quotes WHERE (UPPER(quote) LIKE '%{where}%')")
        if not quotes or len(quotes) == 0:
            return []
        return [class_from_args(Quote, x) for x in quotes]

    def insert_quote(self, quote: Quote, commit: bool = True) -> bool:
        query = f"""
                INSERT INTO quotes
                (quote, author, translation, quote_ita, last_random_tme, telegram_id, private, created, last_modified)
                VALUES
                ($${quote.quote}$$, $${quote.author}$$, $${quote.translation}$$, $${quote.quote_ita}$$, {int_timestamp_now()}, {quote.telegram_id}, {quote.private}, {int_timestamp_now()}, {int_timestamp_now()})
                """
        if not self.insert_query(query=query, commit=False):
            self.rollback()
            return False
        if not quote.tags:
            if commit:
                self.commit()
            return True

        quotes = self.get_quotes({'quote': quote, 'author': 'author'})
        success: bool = True
        for tag in quote.tags:
            success &= self.insert_tag(tag=tag, quote_id=quotes[0].quote_id, commit=False)
        if not success:
            self.rollback()
            return False
        if commit:
            self.commit()
        return True

    def update_quote_by_quote_id(self, quote_id: int, set_params: dict) -> bool:
        set_params = ','.join([key + f" = '{set_params[key]}'" if type(set_params[key]) is str else key + f" = {set_params[key]}" for key in set_params])

        query = f"""
                 UPDATE quotes
                 SET {set_params}, last_modified = {int_timestamp_now()}
                 WHERE quote_id = {quote_id}
                 """
        updated = self.update_query(query, commit=True)
        # if updated:
        #     self.logger.warning('Quote id {} edited: '.format(quote_id))
        return updated

    """ _____ DB: Notes Collection _____ """
    def insert_one_note(self, note: str, user_id, book: str = None, pag: int = None, tags: List[str] = None, commit: bool = True):
        if book:
            if pag:
                query = f"""
                        INSERT INTO notes
                        (note, telegram_id, private, created, last_modified, last_random_time, is_book, book, pag)
                        VALUES
                        ($${note}$$, {user_id}, {True}, {int_timestamp_now()}, {int_timestamp_now()}, {1}, {True}, $${book}$$, {pag})
                        """
            else:
                query = f"""
                        INSERT INTO notes
                        (note, telegram_id, private, created, last_modified, last_random_time, is_book, book)
                        VALUES
                        ($${note}$$, {user_id}, {True}, {int_timestamp_now()}, {int_timestamp_now()}, {1}, {True}, $${book}$$)
                        """
        else:
            query = f"""
                    INSERT INTO notes
                    (note, telegram_id, private, created, last_modified, last_random_time, is_book)
                    VALUES
                    ($${note}$$, {user_id}, {True}, {int_timestamp_now()}, {int_timestamp_now()}, {1}, {False})
                    """
        self.insert_query(query=query, commit=commit)

        query = f"""
                SELECT note_id 
                FROM notes 
                WHERE note = $${note}$$
                """
        note_id = self.select_query(query=query)[0]['note_id']

        for tag in tags:
            self.insert_tag(tag=tag, note_id=note_id, commit=commit)

    def get_notes(self) -> List[dict]:  # TODO: create class Notes, Quotes etc.
        query = f"""SELECT * from notes N"""
        notes = self.select_query(query=query)
        return notes

    def get_notes_with_tags(self) -> List[dict]:
        query = f"""SELECT * 
                    FROM notes N 
                    LEFT JOIN (SELECT note_id AS id FROM notes) TEMP ON TEMP.id = N.note_id
                    LEFT JOIN tags T ON T.note_id = N.note_id"""
        results = self.select_query(query=query)

        if not results:
            return []

        return self.__arrange_tags(results=results)

    def get_notes_with_tags_by_book(self, book: str) -> List[dict]:
        query = f"""SELECT * 
                    FROM notes N 
                    LEFT JOIN (SELECT note_id AS id FROM notes) TEMP ON TEMP.id = N.note_id
                    LEFT JOIN tags T ON T.note_id = N.note_id
                    WHERE N.book = $${book}$$
                    ORDER BY N.pag, N.created
                    """
        results = self.select_query(query=query)

        if not results:
            return []

        return self.__arrange_tags(results=results)

    @staticmethod
    def __arrange_tags(results: List[dict]) -> List[dict]:
        notes_id = list(set([x['id'] for x in results]))

        notes = []
        for note_id in notes_id:
            temp_notes = [x for x in results if x['id'] == note_id]
            note = {key: temp_notes[0][key] for key in temp_notes[0] if key not in ['tag_id', 'tag', 'quote_id', 'note_id']}
            note.update({'tags': [x['tag'] for x in temp_notes if x["tag"]]})
            notes.append(note)

        return notes

    def get_note_with_tags_by_id(self, note_id: int) -> Optional[dict]:
        query = f"""SELECT * from notes N left join tags T on T.note_id = N.note_id where N.note_id = {note_id}"""
        results = self.select_query(query=query)

        if not results:
            return None

        note = {key: results[0][key] for key in results[0] if key not in ['tag_id', 'tag', 'note_id']}
        note.update({'tags': [x['tag'] for x in results if x['tag']]})
        return note

    def update_note_by_note_id(self, note_id, set_params):  # TODO: unify with update_quote_by_quote_id
        set_params = ','.join([key + f" = '{set_params[key]}'" if type(set_params[key]) is str else key + f" = {set_params[key]}" for key in set_params])

        query = f"""
                 UPDATE notes
                 SET {set_params}, last_modified = {int_timestamp_now()}
                 WHERE note_id = {note_id}
                 """
        updated = self.update_query(query, commit=True)
        if updated:
            self.logger.warning('Note id {} edited: '.format(note_id))
        return updated

    """ ______ DB: Tags Collection _____ """
    def insert_tag(self, tag: str, quote_id: int = None, note_id: str = None, commit: bool = True) -> bool:
        if not quote_id and not note_id:
            return False

        name_ = 'quote_id' if quote_id else 'note_id'
        query = f"""
                INSERT INTO tags
                (tag, {name_})
                VALUES
                ($${tag}$$, {quote_id if quote_id else note_id})
                """
        return self.insert_query(query=query, commit=commit)

    def get_last_tags(self, max_tags: int = 9) -> List[str]:
        notes = self.get_notes_with_tags()
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

    """ ____ DB: Favorites Collection _____"""
    def is_favorite(self, quote_id, telegram_id):
        query = f"SELECT * FROM favorites WHERE quote_id = {quote_id} AND telegram_id = {telegram_id}"
        return len(self.select_query(query)) > 0

    def insert_favorite(self, quote_id: int, user_id: int) -> bool:
        query = f"""
                INSERT INTO favorites
                (quote_id, telegram_id)
                VALUES
                ({quote_id}, {user_id})
                """
        return self.insert_query(query=query, commit=True)

    def delete_favorite(self, quote_id, user_id) -> bool:
        query = f"""
                DELETE FROM favorites
                WHERE quote_id = {quote_id}
                AND telegram_id = {user_id}
                """
        return self.delete_query(query, commit=True)

    @staticmethod
    def get_quote_in_language(quote: Quote, user: QuotesUser) -> str:
        quote_in_language = quote.quote_ita if user.language == 'ITA' else quote.quote
        return quote_in_language if quote_in_language else quote.quote


if __name__ == '__main__':
    from src.common.file_manager.FileManager import FileManager
    from src.common.tools.library import run_main, get_environ
    from queue import Queue

    name = 'Example'
    os_environ = get_environ() == 'HEROKU'
    sslmode_ = 'require'
    postgre_key_var = 'POSTGRE_URL_HEROKU'
    logging_queue = Queue()

    # __ init file manager __
    config_manager = FileManager(caller=name, logging_queue=logging_queue)

    # __ init postgre manager __
    postgre_manager = QuotesPostgreManager(db_url=config_manager.get_postgre_url(database_key=postgre_key_var), caller='Example', logging_queue=Queue())
    postgre_manager.connect(sslmode=sslmode_)

    print(postgre_manager.get_tables())

    query_ = "select * from notes limit 10"
    print(postgre_manager.select_query(query_))

    postgre_manager.close_connection()










