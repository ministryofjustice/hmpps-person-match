import logging
from datetime import datetime

import pytz


class LogFormatter(logging.Formatter):
    """
    Override logging.Formatter to use an aware datetime object
    """

    TIMEZONE = pytz.timezone("Europe/London")

    def converter(self, timestamp):
        dt = datetime.fromtimestamp(timestamp, self.TIMEZONE)
        return dt

    def formatTime(self, record, datefmt=None):
        dt = self.converter(record.created)
        if datefmt:
            s = dt.strftime(datefmt)
        else:
            try:
                s = dt.isoformat(timespec="milliseconds")
            except TypeError:
                s = dt.isoformat()
        return s
