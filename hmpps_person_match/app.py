import logging
import os
import platform
import sys

# Must be imported before flask
from azure.monitor.opentelemetry import configure_azure_monitor
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from hmpps_person_match.domain.constants.error_messages import ErrorMessages
from hmpps_person_match.log_formatter import LogFormatter
from hmpps_person_match.models.error_response import ErrorResponse

# required to be able to log result code to appinsights
if os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING"):
    os.environ["OTEL_SERVICE_NAME"] = "hmpps-person-match"
    configure_azure_monitor(logger_name="hmpps-person-match-logger")

from fastapi import FastAPI, HTTPException, Request

from hmpps_person_match.domain.constants.openapi.config import OpenAPIConfig
from hmpps_person_match.views.health_view import router as health_router
from hmpps_person_match.views.info_view import router as info_router
from hmpps_person_match.views.person_match_view import router as person_match_router


class PersonMatchApplication:
    """
    Person Match Fast API Application
    """

    LOGGER_NAME = "hmpps-person-match-logger"

    app: FastAPI = FastAPI(
        title=OpenAPIConfig.APPLICATION_TITLE,
        summary=OpenAPIConfig.APPLICATION_SUMMARY,
        version=OpenAPIConfig.APPLICATION_VERSION,
        docs_url=OpenAPIConfig.DOCS_URL,
        responses=OpenAPIConfig.DEFAULT_RESPONSES,
    )

    def __init__(self) -> None:
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
        Set up request handlers
        """
        self.app.include_router(person_match_router)
        self.app.include_router(health_router)
        self.app.include_router(info_router)

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

    @staticmethod
    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        """
        Custom exception handler for validation errors
        """
        return JSONResponse(
            status_code=400,
            content=ErrorResponse(
                detail=ErrorMessages.INVALID_REQUEST,
                errors=exc.errors(),
            ).model_dump(),
        )

    @staticmethod
    @app.exception_handler(HTTPException)
    async def custom_http_exception_handler(request: Request, exc: HTTPException):
        """
        Custom exception handler for http errors
        """
        return JSONResponse(
            status_code=exc.status_code,
            content=ErrorResponse(
                detail=exc.detail,
            ).model_dump(),
        )
