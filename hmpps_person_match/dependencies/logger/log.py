import logging

from opentelemetry import trace
from opentelemetry.util import types

from hmpps_person_match.domain.telemetry_events import TelemetryEvents

LOGGER_NAME = "hmpps-person-match-logger"

tracer = trace.get_tracer(__name__)

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class AppInsightsLogger(logging.Logger):
    """
    Custom logger to log app insights events
    """

    def __init__(self, name, level=logging.INFO):
        super().__init__(name, level)

    def log_event(self, event: TelemetryEvents, attributes: dict[str, types.AttributeValue]):
        """
        Track a telemetry event
        Export to appinsight
        """
        with tracer.start_as_current_span(event) as span:
            span.set_attributes(attributes)


def get_logger():
    """
    Return the logger
    """
    return AppInsightsLogger(LOGGER_NAME)
