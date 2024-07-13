import os
import json
from pathlib import Path
from dataclasses import dataclass, field

from src.common.tools.library import file_read, file_write, get_exception, print_exception
from src.common.tools.logging_class import LoggerObj


config_path = Path(__file__).parent.parent.parent.parent


@dataclass
class FileManager:
    os_environ: bool = False
    name: str = 'FileManager'
    caller: str = ''
    folder: str = 'config'
    logger: LoggerObj = field(default=None)
    logging_queue: object = field(default=None)
    logger_level: str = 'DEBUG'

    def __post_init__(self):
        self.logger = self.__init_logger(self.logger_level, self.logging_queue) if not self.logger else self.logger

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
    def load(self, key, json_loads=False):
        return self.load_from_env(key=key, json_loads=json_loads) if self.os_environ else self.load_from_file(key=key)

    def save(self, key, data):
        return self.save_to_file(key=key, data=data)

    def load_from_env(self, key, json_loads=False):
        try:
            value = os.environ[key]
            return json.loads(value) if json_loads else value
        except:
            self.logger.warning('Error in loading {} from environ'.format(key))
            return None

    def load_from_file(self, key):
        try:
            return json.loads(file_read(config_path.joinpath(self.folder).joinpath(f"{key}.txt")))
        except:
            self.logger.warning('Error in restoring {}'.format(key))
            return None

    def save_to_file(self, key, data):
        file_write(config_path.joinpath(self.folder).joinpath(key), json.dumps(data))
        return True

    # __ get absolute path __
    def get_absolut_path(self):
        return config_path.joinpath(self.folder)

    # __ get variables __
    def get_postgre_url(self, database_key='POSTGRE_URL_LOCAL'):
        return self.load(key=database_key)

    def get_telegram_token(self, database_key='TELEGRAM_TOKEN'):
        return self.load(key=database_key)

    def get_admin(self, database_key='ADMIN_INFO'):
        return self.load(key=database_key, json_loads=True)

    def get_admin_chat(self, database_key='ADMIN_CHAT'):
        return self.load(key=database_key, json_loads=True)


if __name__ == '__main__':
    config_manager = FileManager(caller="ConfigManager")
    database_url = config_manager.get_postgre_url()
    admin_info = config_manager.get_admin()
    telegram_token = config_manager.get_telegram_token()

    print('end')

