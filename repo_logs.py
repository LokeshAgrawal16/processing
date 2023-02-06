import logging
from logging.handlers import TimedRotatingFileHandler
# from dotenv import load_dotenv
import os

# load_dotenv()

loggers = {}

class Logger:
    global loggers

    def __init__(self, logger_name):
        if loggers.get(logger_name):
            self.logger = loggers[logger_name]
        else:
            self.formatter = logging.Formatter('%(levelname)s - %(asctime)s - %(name)s - %(funcName)s | %(message)s')
            self.logger_name = logger_name
            self.logger = logging.getLogger(self.logger_name)
            self.logger.setLevel(logging.INFO)
            if os.getenv('LOGPATH_LOCAL') is not None:
                self.log_path = os.getenv('LOGPATH_LOCAL')
                handler = TimedRotatingFileHandler(self.log_path, when="d", interval=1, backupCount=5)
                handler.setFormatter(self.formatter)
                self.logger.addHandler(handler)

            ch = logging.StreamHandler()
            ch.setLevel(logging.DEBUG)
            ch.setFormatter(self.formatter)
            self.logger.addHandler(ch)
            loggers[logger_name] = self.logger

    def info(self, logstr):
        self.logger.info(logstr)

    def log(self, logstr):
        self.logger.log(logstr)

    def debug(self, logstr):
        self.logger.debug(logstr)

    def error(self, logstr):
        self.logger.error(logstr)

    def warning(self, logstr):
        self.logger.warning(logstr)

    def exception(self, e):
        self.logger.exception(e)

