import logging


def get_logger():
    """
    Return the logger
    """
    logger_name = "hmpps-person-match-logger"
    return logging.getLogger(logger_name)
