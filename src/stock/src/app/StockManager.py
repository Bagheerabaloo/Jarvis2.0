from dataclasses import dataclass, field

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

from stock.src.app.StockPostgreManager import StockPostgreManager
from stock.src.app.stock_functions.FunctionBack import FunctionBack
from stock.src.app.stock_functions.FunctionStockNewUser import FunctionStockNewUser
from stock.src.app.stock_functions.FunctionDailyGainers import FunctionDailyGainers

from stock.src.app.StockApp import StockApp

# __ logging __
import logging
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
# from logging.handlers import TimedRotatingFileHandler
# from logger_tt import setup_logging
# config_path = Path(__file__).parent.parent.parent.joinpath('resources', 'logger.conf.yaml')
# log_conf = setup_logging(config_path=str(config_path), use_multiprocessing=True)
# LOGGER = logging.getLogger()

HEROKU_POSTGRE_KEY = "DATABASE_URL"
LOCAL_POSTGRE_KEY = "POSTGRE_URL_LOCAL"
# LOCAL_POSTGRE_KEY = "POSTGRE_URL_LOCAL_STOCK"

HEROKU_TELEGRAM_TOKEN_KEY = "QUOTES_TOKEN"
LOCAL_TELEGRAM_TOKEN_KEY = "TELEGRAM_TOKEN"


@dataclass
class StockManager:
    os_environ: bool = get_environ() == 'HEROKU'
    ssl_mode: str = field(default=None)
    telegram_token_key: str = field(default=None)
    postgre_url_key: str = field(default=None)
    config_manager: FileManager = field(default=None)
    token: str = field(default=None)
    postgre_url: str = field(default=None)
    admin_info: dict = field(default=None)
    admin_chat: dict = field(default=None)

    commands = [  # TODO: move to YAML file
        Command(alias=["back"], admin=False, function=FunctionBack),
        Command(alias=["help"], admin=False, function=FunctionHelp),
        Command(alias=["ciao", "hello"], admin=True, function=FunctionCiao),
        Command(alias=["callback"], admin=True, function=FunctionCallback),
        Command(alias=["process"], admin=True, function=FunctionProcess),
        Command(alias=["rows"], admin=True, function=FunctionTotalDBRows),
        Command(alias=["start"], admin=False, function=FunctionStart),
        Command(alias=["appNewUser"], admin=True, function=FunctionStockNewUser, restricted=True),
        Command(alias=["dailyGainers"], admin=True, function=FunctionDailyGainers, restricted=True),
    ]

    def __post_init__(self):
        # __ determine ssl mode depending on environ configurations __
        self.ssl_mode = 'require' if self.os_environ else 'disable'

        # __detect telegram token key __
        self.telegram_token_key = HEROKU_TELEGRAM_TOKEN_KEY if self.os_environ else LOCAL_TELEGRAM_TOKEN_KEY

        # __ init file manager __
        self.config_manager = FileManager(os_environ=self.os_environ)

        # __ define postgre url key __
        if not self.postgre_url_key:
            self.postgre_url_key = HEROKU_POSTGRE_KEY if self.os_environ else LOCAL_POSTGRE_KEY

    def __get_main_params(self):
        self.token = self.config_manager.get_telegram_token(database_key=self.telegram_token_key)
        self.postgre_url = self.config_manager.get_postgre_url(database_key=self.postgre_url_key)
        self.admin_info = self.config_manager.get_admin()
        self.admin_chat = self.config_manager.get_admin_chat()

    def start(self):
        self.__get_main_params()

        postgre_manager = StockPostgreManager(db_url=self.postgre_url, delete_permission=True)
        if not postgre_manager.connect(sslmode=self.ssl_mode):
            # logger.warning("PostgreDB connection not established: cannot connect")
            return

        admin_user = TelegramUser(telegram_id=self.admin_info["chat"],
                                  name=self.admin_info["name"],
                                  username=self.admin_info["username"],
                                  is_admin=True)
        admin_chat = TelegramChat(chat_id=self.admin_chat['chat_id'],
                                  type=self.admin_chat["type"],
                                  username=self.admin_chat["username"],
                                  first_name=self.admin_chat["first_name"],
                                  last_name=self.admin_chat["last_name"])
        telegram_users = postgre_manager.get_telegram_users(admin_user=admin_user)
        telegram_chats = postgre_manager.get_telegram_chats_from_db(admin_chat=admin_chat)

        stock = StockApp(token=self.token,
                         users=telegram_users,
                         chats=telegram_chats,
                         commands=self.commands,
                         postgre_manager=postgre_manager)
        stock.start()
        run_main(app=stock)


if __name__ == '__main__':
    stock_manager = StockManager()
    stock_manager.start()