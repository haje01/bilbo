"""각종 유틸리티 함수."""
import logging


LOG_FMT = logging.Formatter('%(levelname)s [%(filename)s:%(lineno)d]'
                            ' %(message)s')


def log_level_from_verbosity(verbosity):
    if verbosity == 0:
        return 40
    elif verbosity == 1:
        return 30
    elif verbosity == 2:
        return 20
    elif verbosity >= 3:
        return 10


def query_stream_log_handler(logger):
    if len(logger.handlers):
        ch = logger.handlers[0]
    else:
        ch = logging.StreamHandler()
        logger.addHandler(ch)
    return ch


def set_log_verbosity(verbosity):
    level = log_level_from_verbosity(verbosity)
    logger = logging.getLogger()
    logger.setLevel(level)
    handler = query_stream_log_handler(logger)
    handler.setLevel(level)
    handler.setFormatter(LOG_FMT)


def debug(msg):
    logging.getLogger().debug(msg)


def info(msg):
    logging.getLogger().info(msg)


def warning(msg):
    logging.getLogger().warning(msg)


def error(msg):
    logging.getLogger().error(msg)


def critical(msg):
    logging.getLogger().critical(msg)

