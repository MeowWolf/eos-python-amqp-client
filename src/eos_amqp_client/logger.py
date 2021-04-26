from .constants import LOG_LEVEL

import logging
from logging.handlers import TimedRotatingFileHandler


def create_logger(name, level=None, file_name=None):
    log = logging.getLogger(name)
    stream_handler = logging.StreamHandler()
    stream_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    stream_handler.setFormatter(stream_format)
    log.addHandler(stream_handler)

    level = level if level is not None else LOG_LEVEL
    log.setLevel(level)

    return log
