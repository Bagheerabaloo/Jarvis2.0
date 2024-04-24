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
class BasePostgre(TelegramManager):
    commands = [
        Command(alias=["ciao", "hello"], admin=True, function=FunctionCiao),
        Command(alias=["callback"], admin=True, function=FunctionCallback),
        Command(alias=["process"], admin=True, function=FunctionProcess)
    ]

    def __init__(self, caller="", os_environ=True, config_folder='configs', postgre_key_var='DATABASE_URL',
                 init_postgre_manager=False, sslmode='require', init_tables=None, init_users=True, load_users_from_db=True,
                 ext_logger=None, logger_level="DEBUG", logging_queue=None, **kwargs):
        self.name = 'BasePostgre' if not caller else caller
        self.config_folder = config_folder
        self.os_environ = os_environ
        self.load_users_from_db = load_users_from_db
        self.users_main_params = ['telegram_id', 'name', 'username', 'is_admin', 'created', 'last_modified']
        self.run = True

        # __ init logging __
        # self.logger = self.__init_logger(logger_level, logging_queue) if not ext_logger else ext_logger

        # __ init postgre manager __
        # self.postgre_manager = None
        # self.postgre_manager = self.__init_postgre_manager(postgre_url=self.load(key=postgre_key_var, from_os_environ=os_environ), sslmode=sslmode, init_tables=init_tables) if init_postgre_manager else None

        # __ init keyboard __
        # self.init_keyboards()

        # __ init commands __
        # self.init_commands()

        # __init users __
        # self.admin = {"chat": 19838246, "name": "Vale", "username": "vales2"}
        # self.users = self.__init_users() if init_users else None


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
    base_postgre = BasePostgre(token=token, users=users, chats=chats)
    base_postgre.telegram_bot.send_message(chat_id=19838246, text='ciao', inline_keyboard=[['<', '>']])

    while base_postgre.run:
        sleep(0.5)

    LOGGER.info('end')
