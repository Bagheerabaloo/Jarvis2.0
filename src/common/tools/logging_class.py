import logging
from logutils.queue import QueueHandler


LOGGING_LEVELS = ["NOTSET", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


class LoggerObj(object):

    @staticmethod
    def stream_class():
        return logging.StreamHandler

    @staticmethod
    def queue_class():
        return QueueHandler

    def __init__(self, name, level):

        self.logger = logging.getLogger(name)
        self.logger.setLevel(level) if level in LOGGING_LEVELS else "NOTSET"
        self.formatter = logging.Formatter('%(asctime)s:  [%(levelname)7s]  [%(threadName)15s]  [%(name)20s]  %(message)s')

    def add_stream_handler(self, level="NOTSET"):

        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(level) if level in LOGGING_LEVELS else "NOTSET"
        stream_handler.setFormatter(self.formatter)
        self.logger.addHandler(stream_handler)

    def add_file_handler(self, path, filename, level="NOTSET"):

        file_handler = logging.FileHandler("{0}/{1}.log".format(path, filename), mode="a")
        file_handler.setLevel(level) if level in LOGGING_LEVELS else "NOTSET"
        file_handler.setFormatter(self.formatter)
        self.logger.addHandler(file_handler)

    def add_queue_handler(self, queue, level="NOTSET"):

        queue_handler = QueueHandler(queue)
        queue_handler.setLevel(level) if level in LOGGING_LEVELS else "NOTSET"
        queue_handler.setFormatter(self.formatter)
        self.logger.addHandler(queue_handler)

    def debug(self, text, *args, **kwargs):

        self.logger.debug(text, *args, **kwargs)

    def info(self, text, *args, **kwargs):

        self.logger.info(text, *args, **kwargs)

    def warning(self, text, *args, **kwargs):

        self.logger.warning(text, *args, **kwargs)

    def error(self, text, *args, **kwargs):

        self.logger.error(text, *args, **kwargs)

    def critical(self, text, *args, **kwargs):

        self.logger.critical(text, *args, **kwargs)

    def set_level(self, level):

        if level in LOGGING_LEVELS:
            self.logger.setLevel(level)
