import os
import logging
import sys
from logging.handlers import TimedRotatingFileHandler
FORMATTER = logging.Formatter(
    "%(asctime)s — %(name)s — %(levelname)s — %(message)s")
LOG_FILE = os.environ.get("LOG_FILE", "application.log")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "DEBUG")
logging.basicConfig(level=LOG_LEVEL)


def get_console_handler():
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(FORMATTER)
    return console_handler


def get_file_handler():
    file_handler = TimedRotatingFileHandler(LOG_FILE, when='midnight')
    file_handler.setFormatter(FORMATTER)
    return file_handler


def get_logger(logger_name: str):
    logger = logging.getLogger(logger_name)
    logger.addHandler(get_console_handler())
    logger.addHandler(get_file_handler())
    logger.propagate = False
    return logger