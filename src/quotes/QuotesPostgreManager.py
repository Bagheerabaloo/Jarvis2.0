import re
from typing import List, Type, Optional

from src.common.tools.library import class_from_args, int_timestamp_now
from src.common.postgre.PostgreManager import PostgreManager
from src.quotes.QuotesUser import QuotesUser


class QuotesPostgreManager(PostgreManager):
    def get_last_books(self, max_books: int = 4):  # TODO: move to QuoteFunction
        notes = self.get_notes()
        sorted_notes = sorted(notes, key=lambda d: d['created'], reverse=True)

        # books = list(set([x['book'] for x in sorted_notes if x['book']]))
        books = set()
        books_add = books.add
        books = [x['book'] for x in sorted_notes if not (x['book'] in books or books_add(x['book']))]
        if len(books) > max_books:
            books = books[:max_books]
        return books

    def get_last_page(self, book: str = None) -> int:    # TODO: move to QuoteFunction
        notes = self.get_notes()
        sorted_notes = sorted(notes, key=lambda d: d['created'], reverse=True)

        if book:
            sorted_notes = [x for x in sorted_notes if x['book'] == book]
            if len(sorted_notes) > 0:
                pages = [int(x['pag']) for x in sorted_notes if x['pag']]
                return max(pages) if len(pages) > 0 else 1
            return 1

        # last_pages = [x for x in sorted_notes if x is not None]
        # if len(last_pages) > 0:
        #     return last_pages[0]

        return 1

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
                (telegram_id, language)
                VALUES
                ({user.telegram_id}, $${user.language}$$)
                """
        return self.insert_query(query=query, commit=commit)
        # self.logger.info('Quote user created')

    """ ____ DB: Quotes Collection _____"""
    def find_quotes(self, params):
        params = ' AND '.join([key + f" = '{params[key]}'" if type(params[key]) is str else key + f" = {params[key]}" for key in params])
        return self.select_query(f"SELECT * FROM quotes WHERE {params}")

    def check_for_similar_quotes(self, quote: str):
        words = re.findall(r'\w+', quote)
        where = '%\' AND UPPER(quote) LIKE \'%'.join([x.upper() for x in words])
        quotes = self.select_query(f"SELECT * FROM quotes WHERE (UPPER(quote) LIKE '%{where}%')")
        return quotes if len(quotes) > 0 else []

    def insert_quote(self, telegram_id: int, quote: str, author: str, translation: str = None, private: bool = True, tags: List[str] = None):
        query = f"""
                INSERT INTO quotes
                (quote, author, translation, last_random_tme, telegram_id, private, created, last_modified)
                VALUES
                ($${quote}$$, $${author}$$, $${translation}$$, {int_timestamp_now()}, {telegram_id}, {private}, {int_timestamp_now()}, {int_timestamp_now()})
                """
        if not self.insert_query(query=query, commit=True):
            return False
        if not tags:
            return True

        quotes = self.find_quotes({'quote': quote, 'author': 'author'})
        for tag in tags:
            self.insert_tag(tag=tag, quote_id=quotes[0]['quote_id'])

        return True

    def update_quote_by_quote_id(self, quote_id, set_params):
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
    def insert_one_note(self, note: str, user_id, book: str = None, pag: int = None, tags: List[str] = None):
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
        self.insert_query(query=query, commit=True)

        query = f"""
                SELECT note_id 
                FROM notes 
                WHERE note = $${note}$$
                """
        note_id = self.select_query(query=query)[0]['note_id']

        for tag in tags:
            self.insert_tag(tag=tag, note_id=note_id)

    def get_notes(self) -> List[dict]:  # TODO: create class Notes, Quotes etc.
        query = f"""SELECT * from notes N"""
        notes = self.select_query(query=query)
        return notes

    def get_notes_with_tags(self) -> List[dict]:
        query = f"""SELECT * from notes N join tags T on T.note_id = N.note_id"""
        results = self.select_query(query=query)

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

    def get_note_by_id(self, note_id: int) -> Optional[dict]:
        query = f"""SELECT * from notes N left join tags T on T.note_id = N.note_id where N.note_id = {note_id}"""
        results = self.select_query(query=query)

        if not results:
            return None

        note = {key: results[0][key] for key in results[0] if key not in ['tag_id', 'tag', 'note_id']}
        note.update({'tags': [x['tag'] for x in results if x['tag']]})
        return note

    """ ______ DB: Tags Collection _____ """
    def insert_tag(self, tag: str, quote_id: str = None, note_id: str = None) -> bool:
        if not quote_id and not note_id:
            return False

        name_ = 'quote_id' if quote_id else 'note_id'
        query = f"""
                INSERT INTO tags
                (tag, {name_})
                VALUES
                ($${tag}$$, {quote_id if quote_id else note_id})
                """
        return self.insert_query(query=query, commit=True)

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
    def get_quote_in_language(quote: dict, user: QuotesUser) -> str:
        quote_in_language = quote['quote_ita'] if user.language == 'ITA' else quote['quote']
        return quote_in_language if quote_in_language else quote['quote']


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









