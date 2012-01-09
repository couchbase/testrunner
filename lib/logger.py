import logging

global _logger

class Logger:

    _logger = None

    @staticmethod
    def stop_logger():
        Logger._logger = None

    @staticmethod
    def get_logger():
        return logging.getLogger()

    @staticmethod
    def start_logger(name):
    # create logger
        return logging.getLogger()


def new_logger(name):
    return logging.getLogger()
