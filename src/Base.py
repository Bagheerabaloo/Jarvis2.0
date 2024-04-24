from dataclasses import dataclass, field
from time import time, sleep

from src.common.Command import Command
from src.common.telegram.TelegramManager import TelegramManager
from src.common.telegram.TelegramUser import TelegramUser
from src.common.telegram.TelegramChat import TelegramChat

from src.common.functions.FunctionCiao import FunctionCiao
from src.common.functions.FunctionCallback import FunctionCallback
from src.common.functions.FunctionProcess import FunctionProcess


@dataclass
class Base(TelegramManager):
    commands = [
        Command(alias=["ciao", "hello"], admin=True, function=FunctionCiao),
        Command(alias=["callback"], admin=True, function=FunctionCallback),
        Command(alias=["process"], admin=True, function=FunctionProcess)
    ]


if __name__ == '__main__':
    import logging
    # from logging.handlers import TimedRotatingFileHandler
    from pathlib import Path
    from logger_tt import setup_logging
    from src.common.file_manager.FileManager import FileManager

    config_path = Path(__file__).parent.parent.joinpath('resources', 'logger.conf.yaml')
    log_conf = setup_logging(config_path=str(config_path), use_multiprocessing=True)
    LOGGER = logging.getLogger()

    # __ init file manager __
    config_manager = FileManager()

    # __ get telegram token __
    token = config_manager.get_telegram_token()

    users = [TelegramUser(id=19838246, name='Vale', username='vales2', is_admin=True)]
    chats = [TelegramChat(chat_id=19838246, type='private', username='vales2', first_name='Vale', last_name='S')]
    telegram_manager = Base(token=token, users=users, chats=chats)
    telegram_manager.telegram_bot.send_message(chat_id=19838246, text='ciao', inline_keyboard=[['<', '>']])

    while telegram_manager.run:
        sleep(0.5)

    LOGGER.info('end')
