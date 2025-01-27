import logging

from hmpps_person_match.dependencies.logging.log_formatter import LogFormatter

LOGGER_NAME = "hmpps-person-match-logger"

logging.Formatter = LogFormatter
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

def get_logger():
    """
    Return the logger
    """
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.INFO)
    return logger
