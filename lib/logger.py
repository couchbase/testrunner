import logging
from logging.handlers import RotatingFileHandler
import uuid

global _logger

class Logger:

    _logger = None

    @staticmethod
    def stop_logger():
        Logger._logger = None

    @staticmethod
    def get_logger():
        if not Logger._logger:
            Logger._logger = Logger.start_logger('console')
        return Logger._logger

    @staticmethod
    def start_logger(name):
    # create logger
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)

        # create console handler and set level to debug
        consoleHandler = logging.StreamHandler()
        consoleHandler.setLevel(logging.DEBUG)

        # create formatter
        formatter = logging.Formatter("[%(asctime)s] - [%(module)s] [%(thread)d] - %(levelname)s - %(message)s")

        max_size = 20 * 1024 * 1024 #max size is 50 megabytes
        fileHandler = RotatingFileHandler('{0}.log'.format(name), backupCount=2,maxBytes=max_size)

        # add formatter to ch
        consoleHandler.setFormatter(formatter)
        fileHandler.setFormatter(formatter)

        # add ch to logger
        logger.addHandler(consoleHandler)
        logger.addHandler(fileHandler)
        Logger._logger = logger
        print 'start logging to {0}.log'.format(name)
        return Logger._logger