from src.common.postgre.PostgreManager import PostgreManager
from src.quotes.QuotesPostgreManager import QuotesPostgreManager
from src.quotes.Note import Note
from src.quotes.Quote import Quote


if __name__ == '__main__':
    from src.common.file_manager.FileManager import FileManager
    from src.common.tools.library import run_main, get_environ
    import json
    from queue import Queue

    name = 'Example'
    os_environ = get_environ() == 'HEROKU'
    logging_queue = Queue()

    # __ init file manager __
    config_manager = FileManager(caller=name, logging_queue=logging_queue)
    backup_config_manager = FileManager(folder="src/common/postgre/db_backup")

    # __ init heroku postgre manager __
    postgre_key_var_heroku = 'POSTGRE_URL_HEROKU'
    db_url_heroku = config_manager.get_postgre_url(database_key=postgre_key_var_heroku)
    postgre_manager_heroku = QuotesPostgreManager(db_url=db_url_heroku)
    postgre_manager_heroku.insert_permission = False
    postgre_manager_heroku.update_permission = False
    postgre_manager_heroku.connect(sslmode='require')

    # __ init local postgre manager __
    postgre_key_var_local = 'POSTGRE_URL_LOCAL'
    db_url_local = config_manager.get_postgre_url(database_key=postgre_key_var_local)
    postgre_manager_local = QuotesPostgreManager(db_url=db_url_local)
    postgre_manager_local.connect(sslmode="disable")

    # __ dump heroku tables to csv __
    tables = postgre_manager_heroku.get_tables()
    for table in tables:
        query_ = f"SELECT * FROM {table[0]}"
        results = postgre_manager_heroku.select_query(query_)
        try:
            backup_config_manager.save(f"{table[0]}.csv", results)
            print(f"{table[0]}: back-up completed")
        except:
            print(f"{table[0]}: unable to backup")

    # # __ copy notes from heroku postgre to local postgre __
    # notes_with_tags = postgre_manager_heroku.get_notes_with_tags()
    # for note in notes_with_tags:
    #     postgre_manager_local.insert_one_note(note=note, commit=False)
    # postgre_manager_local.commit()

    # # __ copy quotes from heroku to local postgre __a
    # quotes_with_tags = postgre_manager_heroku.get_quotes_with_tags()
    # for quote in quotes_with_tags:
    #     postgre_manager_local.insert_quote(quote=quote, commit=False)
    # postgre_manager_local.commit()

    postgre_manager_local.close_connection()
    postgre_manager_heroku.close_connection()
