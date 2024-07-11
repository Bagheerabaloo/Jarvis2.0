import os
import glob
import zipfile
from datetime import datetime

from src.common.postgre.PostgreManager import PostgreManager
from src.quotes.classes.QuotesPostgreManager import QuotesPostgreManager
from src.quotes.classes.Note import Note
from src.quotes.classes.Quote import Quote
from src.common.file_manager.FileManager import FileManager


def main():
    # __ init file manager __
    config_manager = FileManager()
    back_up_directory = "data/db_backup"
    backup_config_manager = FileManager(folder=back_up_directory)
    back_up_directory = backup_config_manager.get_absolut_path()

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
    dump_heroku_table_to_csv(postgre_manager_heroku, backup_config_manager)

    # __ zip csv files __
    zip_csv_files(back_up_directory)

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


def dump_heroku_table_to_csv(postgre_manager_heroku: QuotesPostgreManager,
                             backup_config_manager: FileManager) -> None:
    tables = postgre_manager_heroku.get_tables()
    for table in tables:
        query_ = f"SELECT * FROM {table[0]}"
        results = postgre_manager_heroku.select_query(query_)
        try:
            backup_config_manager.save(f"{table[0]}.csv", results)
            print(f"{table[0]}: back-up completed")
        except:
            print(f"{table[0]}: unable to backup")


def zip_csv_files(directory):
    # get current date and time
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    zip_filename = f"_heroku_backup_{now}.zip"

    # __ make sure the directory exists __
    os.makedirs(directory, exist_ok=True)

    # create a ZIP file
    zip_filepath = os.path.join(directory, zip_filename)
    with zipfile.ZipFile(zip_filepath, 'w') as zipf:
        # find all CSV files in the directory
        for csv_file in glob.glob(os.path.join(directory, "*.csv")):
            # add the file to the ZIP
            zipf.write(csv_file, os.path.basename(csv_file))

    print(f"Created ZIP file: {zip_filepath}")


if __name__ == '__main__':
    main()
