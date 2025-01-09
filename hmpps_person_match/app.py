import logging
import os
import platform
import sys

# Must be imported before flask
from azure.monitor.opentelemetry import configure_azure_monitor

from hmpps_person_match.log_formatter import LogFormatter

# required to be able to log result code to appinsights
if os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING"):
    os.environ["OTEL_SERVICE_NAME"] = "hmpps-person-match"
    configure_azure_monitor(logger_name="hmpps-person-match-logger")

import flask

from hmpps_person_match.views.health_view import HealthView
from hmpps_person_match.views.info_view import InfoView
from hmpps_person_match.views.match_view import MatchView
from hmpps_person_match.views.person_match_view import PersonMatchView


class PersonMatchFlaskApplication:
    """
    Person Match Flask Application
    """

    APPLICATION_NAME = "hmpps-person-match"
    LOGGER_NAME = "hmpps-person-match-logger"

    def __init__(self) -> None:
        self.app = flask.Flask(self.APPLICATION_NAME)
        self.wsgi_app = self.app.wsgi_app
        self.initialise()

    def initialise(self):
        """
        Initialise application
        """
        self.initialise_logger()
        self.log_version()
        self.initialise_request_handlers()

    def log_version(self):
        """
        Log application version
        """
        version = " ".join(sys.version.split(" ")[:1])
        log_message = f"Starting hmpps-person-match using Python {version} on {platform.platform()}"
        self.logger.info(log_message)

    def initialise_request_handlers(self):
        """
        Set up request handlers, passes logger to each view
        Each request handler can define ROUTE const as url rule
        """
        for request_handler in [HealthView, MatchView, InfoView, PersonMatchView]:
            self.app.add_url_rule(
                request_handler.ROUTE,
                view_func=request_handler.as_view(request_handler.__name__, self.logger),
            )

    def initialise_logger(self):
        """
        Set up application logger
        """
        # this suppresses app insights logs from stdout
        logging.Formatter = LogFormatter
        logging.basicConfig(
            level=logging.WARNING,
            format="%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        self.logger = logging.getLogger(self.LOGGER_NAME)
        self.logger.setLevel(logging.INFO)

    def run(self):
        """
        Run the Application
        """
        self.app.run()


if __name__ == "__main__":
    PersonMatchFlaskApplication().run()
