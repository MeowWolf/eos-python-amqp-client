from .constants import LOG_LEVEL

import logging


def create_logger(name: str, level: str = None) -> logging.Logger:
    log: logging.Logger = logging.getLogger(name)
    stream_handler = logging.StreamHandler()
    stream_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    stream_handler.setFormatter(stream_format)
    log.addHandler(stream_handler)

    level: str = level if level is not None else LOG_LEVEL
    log.setLevel(level)

    return log
