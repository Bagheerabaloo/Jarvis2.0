import re
from typing import List, Type, Optional
from dataclasses import dataclass, field, asdict
from collections import OrderedDict

from src.common.tools.library import class_from_args, int_timestamp_now
from src.common.postgre.PostgreManager import PostgreManager
from quotes.classes.QuotesUser import QuotesUser
from quotes.classes.Quote import Quote
from quotes.classes.Note import Note
from quotes.classes.Tag import Tag


class QuotesPostgreManager(PostgreManager):
    TYPE_NAME_TO_CLASS = {Note.TYPE_NAME: Note.from_dict,
                          }

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
            new_quote_user = QuotesUser(telegram_id=telegram_admin_user.telegram_id,
                                        name=telegram_admin_user.name,
                                        username=telegram_admin_user.username,
                                        is_admin=telegram_admin_user.is_admin)
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
    def get_all_quotes(self) -> List[Quote]:
        quotes = self.select_query("SELECT * FROM quotes")
        if not quotes:
            return []
        return [class_from_args(Quote, x) for x in quotes] if quotes else []

    def get_quotes_by_params(self, params: dict) -> List[Quote]:
        params = ' AND '.join([key + f" = '{params[key]}'" if type(params[key]) is str else key + f" = {params[key]}"
                               for key in params])
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

        # TODO: unify this section with self.__arrange_tags
        quotes_id = list(set([x['id'] for x in results]))

        quotes = []
        for quote_id in quotes_id:
            temp_quotes = [x for x in results if x['id'] == quote_id]
            quote = {key: temp_quotes[0][key]
                     for key in temp_quotes[0] if key not in ['tag_id', 'tag', 'note_id', 'quote_id']}
            quote.update({'tags': [class_from_args(Tag, {"tag": x['tag'], "quote_id": quote_id})
                                   for x in temp_quotes if x["tag"]]
                          })
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
                ($${quote.quote}$$, $${quote.author}$$, $${quote.translation}$$, $${quote.quote_ita}$$, 
                {int_timestamp_now()}, {quote.telegram_id}, {quote.private}, 
                {int_timestamp_now()}, {int_timestamp_now()})
                """
        if not self.insert_query(query=query, commit=False):
            self.rollback()
            return False
        if not quote.tags:
            if commit:
                self.commit()
            return True

        quotes = self.get_quotes_by_params({'quote': quote.quote, 'author': quote.author})
        success: bool = True
        for tag in quote.tags:
            success &= self.insert_tag(tag=tag, commit=False)
        if not success:
            self.rollback()
            return False
        if commit:
            self.commit()
        return True

    def update_quote_by_quote_id(self, quote_id: int, set_params: dict) -> bool:
        set_params = ','.join([key + f" = '{set_params[key]}'" if type(set_params[key]) is str else key + f" = {set_params[key]}"
                               for key in set_params])

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
    def insert_one_note(self, note: Note, commit: bool = True) -> bool:
        params_to_insert = {k: v for k, v in asdict(note).items() if v is not None and k != 'tags'}
        query = f"""
                INSERT INTO notes
                ({', '.join(params_to_insert.keys())})
                VALUES
                ({', '.join([f'$${str(params_to_insert[k])}$$' if type(params_to_insert[k]) == str else str(params_to_insert[k])
                             for k in params_to_insert])})
                """
        if not self.insert_query(query=query, commit=False):
            self.rollback()
            return False

        note_id = self.get_note_id_by_note(note=note.note)
        if not note_id:
            self.rollback()
            return False

        success = True
        for tag in note.tags:
            tag.note_id = note_id
            success &= self.insert_tag(tag=tag, commit=False)
        if not success:
            self.rollback()
            return False
        if commit:
            self.commit()
        return True

    def get_notes_ids(self, sorted_by_created: bool = False) -> List[int]:
        query = f"""SELECT * from notes"""
        notes = self.select_query(query=query)
        if not notes:
            return []
        if sorted_by_created:
            notes = sorted(notes, key=lambda d: d['created'], reverse=True)
        return [x["note_id"] for x in notes]

    def get_notes_ids_by_book(self, book: str) -> List[int]:
        query = f"""SELECT * FROM notes WHERE book = $${book}$$ ORDER BY pag, created"""
        notes = self.select_query(query=query)
        if not notes:
            return []
        return [x["note_id"] for x in notes]

    def get_notes(self, sorted_by_created: bool = False) -> List[Note]:
        query = f"""SELECT * from notes N"""
        notes = self.select_query(query=query)
        if not notes:
            return []
        if sorted_by_created:
            notes = sorted(notes, key=lambda d: d['created'], reverse=True)
        return [class_from_args(Note, x) for x in notes]

    def get_notes_with_tags(self, sorted_by_created: bool = False) -> List[Note]:
        query = f"""SELECT * 
                    FROM notes N 
                    LEFT JOIN (SELECT note_id AS id FROM notes) TEMP ON TEMP.id = N.note_id
                    LEFT JOIN tags T ON T.note_id = N.note_id"""
        results = self.select_query(query=query)
        if not results:
            return []

        notes = self.__arrange_tags(results=results)
        if sorted_by_created:
            notes = sorted(notes, key=lambda d: d['created'], reverse=True)
        return [class_from_args(Note, x) for x in notes]

    def get_notes_with_tags_by_book(self, book: str, sorted_by_created: bool = False) -> List[Note]:
        # TODO: use function get_notes_with_tags passing WHERE condition
        query = f"""
                SELECT * 
                FROM notes N 
                LEFT JOIN (SELECT note_id AS id FROM notes) TEMP ON TEMP.id = N.note_id
                LEFT JOIN tags T ON T.note_id = N.note_id
                WHERE N.book = $${book}$$
                ORDER BY N.pag, N.created
                """
        results = self.select_query(query=query)
        if not results:
            return []

        notes = self.__arrange_tags(results=results)  # TODO: this function breaks the order of the notes
        if sorted_by_created:
            notes = sorted(notes, key=lambda d: (float('inf') if d["pag"] is None else d["pag"], d["created"]))
        return [class_from_args(Note, x) for x in notes]

    def get_note_id_by_note(self, note: str) -> Optional[str]:
        query = f"""
                SELECT note_id 
                FROM notes 
                WHERE note = $${note}$$
                """
        note_id = self.select_query(query=query)
        if not note_id:
            return None
        return note_id[0]['note_id']

    def get_note_with_tags_by_id(self, note_id: int) -> Optional[Note]:
        query = f"""SELECT * from notes N left join tags T on T.note_id = N.note_id where N.note_id = {note_id}"""
        results = self.select_query(query=query)
        if not results:
            return None

        note = {key: results[0][key] for key in results[0] if key not in ['tag_id', 'tag', 'note_id']}
        note.update({'tags': [class_from_args(Tag, {"tag": x['tag'], "note_id": note_id})
                              for x in results if x["tag"]]
                     })  # TODO: this line is repeated in the code, unify it
        return class_from_args(Note, note)

    def get_books(self) -> List[str]:
        query = "SELECT DISTINCT book FROM notes"
        books = self.select_query(query=query)
        return [x['book'] for x in books] if books else []

    @staticmethod
    def __arrange_tags(results: List[dict]) -> List[dict]:
        notes_id = list(set([x['id'] for x in results]))

        notes = []
        for note_id in notes_id:
            temp_notes = [x for x in results if x['id'] == note_id]
            note = {key: temp_notes[0][key]
                    for key in temp_notes[0] if key not in ['tag_id', 'tag', 'quote_id', 'note_id']}
            note.update({'tags': [class_from_args(Tag, {"tag": x['tag'], "note_id": note_id})
                                  for x in temp_notes if x["tag"]]
                         })
            note['note_id'] = note.pop('id')
            notes.append(note)

        return notes

    def update_note_by_note_id(self, note_id: int, set_params: dict) -> bool:
        # TODO: unify with update_quote_by_quote_id
        set_params = ','.join([key + f" = '{set_params[key]}'" if type(set_params[key]) is str else key + f" = {set_params[key]}"
                               for key in set_params])

        query = f"""
                 UPDATE notes
                 SET {set_params}, last_modified = {int_timestamp_now()}
                 WHERE note_id = {note_id}
                 """
        updated = self.update_query(query, commit=True)
        if updated:
            self.logger.warning('Note id {} edited: '.format(note_id))
        return updated

    def get_daily_notes(self) -> List[Note]:
        query = """
                (
                SELECT *
                FROM notes
                WHERE is_book = TRUE
                AND last_random_time = 1
                ORDER BY RANDOM()
                LIMIT 100
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
        notes = self.select_query(query=query)
        if not notes:
            return []
        return [class_from_args(Note, x) for x in notes]

    """ ______ DB: Tags Collection _____ """    # TODO: make Tag object
    def insert_tag(self, tag: Tag, commit: bool = True) -> bool:
        if not tag.quote_id and not tag.note_id:
            return False

        name_ = 'quote_id' if tag.quote_id else 'note_id'
        query = f"""
                INSERT INTO tags
                (tag, {name_})
                VALUES
                ($${tag.tag}$$, {tag.quote_id if tag.quote_id else tag.note_id})
                """
        return self.insert_query(query=query, commit=commit)

    def get_last_tags(self, max_tags: int = 9) -> List[str]:
        sorted_notes = self.get_notes_with_tags(sorted_by_created=True)
        tags = [tag for note in sorted_notes for tag in note.get_list_tags()]
        set_tags = list(OrderedDict.fromkeys(tags))[:max_tags]
        return set_tags

    def remove_tag_from_note_by_id(self, note_id: int, tag: str) -> bool:
        query = f"""
                DELETE FROM tags
                WHERE note_id = {note_id}
                AND tag = $${tag}$$
                """
        return self.delete_query(query, commit=True)

    def add_tag_to_note_by_id(self, note_id: int, tag: str) -> bool:
        query = f"""
                INSERT INTO tags
                (tag, note_id)
                VALUES
                ($${tag}$$, {note_id})
                """
        return self.insert_query(query, commit=True)

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

    @staticmethod
    def custom_deserializer(dct) -> object:
        for cls in [Note, Quote, Tag]:
            try:
                return cls.from_dict(dct)
            except (TypeError, KeyError, AttributeError):
                continue
        return dct


if __name__ == '__main__':
    from src.common.file_manager.FileManager import FileManager
    from src.common.tools.library import run_main, get_environ
    from queue import Queue

    name = 'Example'
    os_environ = get_environ() == 'HEROKU'
    ssl_mode = 'require'
    postgre_key_var = 'POSTGRE_URL_HEROKU'
    logging_queue = Queue()

    # __ init file manager __
    config_manager = FileManager(caller=name, logging_queue=logging_queue)

    # __ init postgre manager __
    postgre_manager = QuotesPostgreManager(db_url=config_manager.get_postgre_url(database_key=postgre_key_var),
                                           caller='Example',
                                           logging_queue=Queue())
    postgre_manager.connect(sslmode=ssl_mode)

    print(postgre_manager.get_tables())

    query_ = "SELECT * FROM notes limit 10"
    print(postgre_manager.select_query(query_))

    postgre_manager.close_connection()
