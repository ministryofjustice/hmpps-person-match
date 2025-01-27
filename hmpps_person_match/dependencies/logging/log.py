import logging

from hmpps_person_match.dependencies.logging.log_formatter import LogFormatter

LOGGER_NAME = "hmpps-person-match-logger"

logging.Formatter = LogFormatter
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

def get_logger():
    """
    Return the logger
    """
    return logging.getLogger(LOGGER_NAME)
