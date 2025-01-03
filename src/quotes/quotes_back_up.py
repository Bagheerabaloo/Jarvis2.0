import os
import glob
import zipfile
from datetime import datetime
from pathlib import Path
import openpyxl

import pandas as pd

from src.common.postgre.PostgreManager import PostgreManager
from src.quotes.classes.QuotesPostgreManager import QuotesPostgreManager
from src.quotes.classes.Note import Note
from src.quotes.classes.Quote import Quote
from src.common.file_manager.FileManager import FileManager


def back_up():
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

    query_ = f"SELECT * FROM quotes"
    results = postgre_manager_heroku.select_query(query_)
    df = pd.DataFrame(results)

    path = Path(__file__).parent.parent.parent
    path = path.joinpath("data/db_backup/quotes.xlsx")
    df.to_excel(path)


if __name__ == '__main__':
    back_up()
