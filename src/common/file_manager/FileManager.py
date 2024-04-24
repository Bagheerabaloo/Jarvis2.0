import os
import json
from pathlib import Path

from src.common.tools.library import file_read, file_write, get_exception, print_exception
from src.common.tools.logging_class import LoggerObj


config_path = Path(__file__).parent.parent.parent.parent


class FileManager:
    def __init__(self, folder='config', caller="", ext_logger=None, logger_level="DEBUG", logging_queue=None):
        self.name = "{}ConfigManager".format(caller)
        self.caller = caller
        self.folder = folder

        # __ init logger __
        self.logger = self.__init_logger(logger_level, logging_queue) if not ext_logger else ext_logger

    # __ Logger __
    def __init_logger(self, logger_level, logging_queue):

        logger = LoggerObj(self.name, logger_level)
        logger.add_stream_handler("INFO")
        if logging_queue:
            logger.add_queue_handler(logging_queue, 'WARNING')

        return logger

    def set_logger_stream(self, level):

        handlers = [x for x in self.logger.logger.handlers if type(x) == self.logger.stream_class()]
        for handler in handlers:
            handler.setLevel(level)

    def set_logger_queue(self, level):

        handlers = [x for x in self.logger.logger.handlers if type(x) == self.logger.queue_class()]
        for handler in handlers:
            handler.setLevel(level)

    def __log_exception(self):
        self.logger.error(get_exception()) if self.logger else print_exception()
        return None

    # __ load/save __
    def load(self, key):
        try:
            return json.loads(file_read(config_path.joinpath(self.folder).joinpath(f"{key}.txt")))
        except:
            self.logger.warning('Error in restoring {}'.format(key))
            return None

    def save(self, key, data):
        file_write(config_path.joinpath(self.folder).joinpath(key), json.dumps(data))
        return True

    # __ postgre url __
    def get_postgre_url(self, database_key='DATABASE_URL'):
        return self.load(key=database_key)

    def get_telegram_token(self, database_key='TELEGRAM_TOKEN'):
        return self.load(key=database_key)

    def get_admin(self, database_key='ADMIN_INFO'):
        return self.load(key=database_key)


if __name__ == '__main__':
    config_manager = FileManager(caller="ConfigManager")
    database_url = config_manager.get_postgre_url()
    admin_info = config_manager.get_admin()
    telegram_token = config_manager.get_telegram_token()

    print('end')

