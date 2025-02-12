import logging
import os
import platform
import sys

from azure.monitor.opentelemetry import configure_azure_monitor

from hmpps_person_match.dependencies.logger.log import LOGGER_NAME, get_logger

if os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING"):
    os.environ["OTEL_SERVICE_NAME"] = "hmpps-person-match"
    configure_azure_monitor(logger_name=LOGGER_NAME)

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from hmpps_person_match.domain.constants.error_messages import ErrorMessages
from hmpps_person_match.domain.constants.openapi.config import OpenAPIConfig
from hmpps_person_match.models.error_response import ErrorResponse
from hmpps_person_match.routes.health import router as health_router
from hmpps_person_match.routes.info import router as info_router
from hmpps_person_match.routes.person import router as person_router


class PersonMatchApplication:
    """
    Person Match Fast API Application
    """

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
        self.app.include_router(person_router)
        self.app.include_router(health_router)
        self.app.include_router(info_router)

    def initialise_logger(self):
        """
        Set up application logger
        """
        # Stops console logs from requests, still sends telemetry
        logging.getLogger("uvicorn.access").disabled = True
        self.logger = get_logger()

    @staticmethod
    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        """
        Custom exception handler for validation errors
        """
        get_logger().exception(exc.errors)
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
