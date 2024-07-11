from common.tools import run_main, get_environ
from common.file_manager.FileManager import FileManager

from common.telegram_manager.Command import Command
from common.telegram_manager.TelegramUser import TelegramUser
from common.telegram_manager.TelegramChat import TelegramChat

from common.functions.FunctionCiao import FunctionCiao
from common.functions.FunctionCallback import FunctionCallback
from common.functions.FunctionProcess import FunctionProcess
from common.functions.FunctionStart import FunctionStart
from common.functions.FunctionHelp import FunctionHelp
from common.functions.FunctionTotalDBRows import FunctionTotalDBRows

from quotes.classes.QuotesPostgreManager import QuotesPostgreManager
from quotes.quotes_functions.FunctionBack import FunctionBack
from quotes.quotes_functions.FunctionQuotesNewUser import FunctionQuotesNewUser
from quotes.quotes_functions.FunctionRandomQuote import FunctionRandomQuote
from quotes.quotes_functions.FunctionShowQuotes import FunctionShowQuotes
from quotes.quotes_functions.FunctionNewQuote import FunctionNewQuote
from quotes.quotes_functions.FunctionNewNote import FunctionNewNote
from quotes.quotes_functions.FunctionShowNotes import FunctionShowNotes
from quotes.quotes_functions.FunctionDailyQuote import FunctionDailyQuote
from quotes.quotes_functions.FunctionQuotesSettings import FunctionQuotesSettings
from quotes.quotes_functions.FunctionBook import FunctionBook

from quotes import QuotesApp

# import yaml
# from src.quotes.functions import FunctionBack, FunctionQuotesNewUser, FunctionRandomQuote, FunctionShowQuotes, FunctionNewQuote, FunctionNewNote, FunctionShowNotes, FunctionDailyQuote, FunctionDailyBook, FunctionQuotesSettings
# from src.common.functions import FunctionCiao, FunctionCallback, FunctionProcess, FunctionStart, FunctionHelp

# __ logging __
import logging
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
# from logging.handlers import TimedRotatingFileHandler
# from logger_tt import setup_logging
# config_path = Path(__file__).parent.parent.parent.joinpath('resources', 'logger.conf.yaml')
# log_conf = setup_logging(config_path=str(config_path), use_multiprocessing=True)
# LOGGER = logging.getLogger()


def main():
    # __ whether it's running on Heroku or local
    os_environ = get_environ() == 'HEROKU'

    # __ determine ssl mode depending on environ configurations __
    ssl_mode = 'require' if os_environ else 'disable'

    # __ init file manager __
    config_manager = FileManager()

    # __ get telegram token __
    token = config_manager.get_telegram_token()
    # postgre_url = config_manager.get_postgre_url(database_key='POSTGRE_URL_LOCAL_DOCKER')
    postgre_url = config_manager.get_postgre_url()
    admin_info = config_manager.get_admin()
    admin_chat = config_manager.get_admin_chat()

    postgre_manager = QuotesPostgreManager(db_url=postgre_url, delete_permission=True)
    if not postgre_manager.connect(sslmode=ssl_mode):
        # logger.warning("PostgreDB connection not established: cannot connect")
        return

    admin_user = TelegramUser(telegram_id=admin_info["chat"],
                              name=admin_info["name"],
                              username=admin_info["username"],
                              is_admin=True)
    admin_chat = TelegramChat(chat_id=admin_chat['chat_id'],
                              type=admin_chat["type"],
                              username=admin_chat["username"],
                              first_name=admin_chat["first_name"],
                              last_name=admin_chat["last_name"])
    telegram_users = postgre_manager.get_telegram_users(admin_user=admin_user)
    telegram_chats = postgre_manager.get_telegram_chats_from_db(admin_chat=admin_chat)

    commands = [    # TODO: move to YAML file
        Command(alias=["back"], admin=False, function=FunctionBack),
        Command(alias=["help"], admin=False, function=FunctionHelp),
        Command(alias=["ciao", "hello"], admin=True, function=FunctionCiao),
        Command(alias=["callback"], admin=True, function=FunctionCallback),
        Command(alias=["process"], admin=True, function=FunctionProcess),
        Command(alias=["rows"], admin=True, function=FunctionTotalDBRows),
        Command(alias=["start"], admin=False, function=FunctionStart),
        Command(alias=["quote"], admin=False, function=FunctionRandomQuote),
        Command(alias=["showQuotes"], admin=False, function=FunctionShowQuotes),
        Command(alias=["newQuote"], admin=True, function=FunctionNewQuote),
        Command(alias=["note"], admin=True, function=FunctionNewNote),
        Command(alias=["showNotes"], admin=False, function=FunctionShowNotes),
        Command(alias=["settings"], admin=False, function=FunctionQuotesSettings),
        Command(alias=["book"], admin=False, function=FunctionBook),
        Command(alias=["appNewUser"], admin=True, function=FunctionQuotesNewUser, restricted=True),
        Command(alias=["dailyQuote"], admin=False, function=FunctionDailyQuote, restricted=True),
    ]

    quotes = QuotesApp(token=token,
                       users=telegram_users,
                       chats=telegram_chats,
                       commands=commands,
                       postgre_manager=postgre_manager)
    quotes.start()
    run_main(app=quotes)


if __name__ == '__main__':
    main()

