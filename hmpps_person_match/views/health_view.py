from logging import Logger
from typing import Annotated

from fastapi import APIRouter, Depends, status
from fastapi.responses import JSONResponse
from sqlalchemy import Connection, select
from sqlalchemy.exc import SQLAlchemyError

from hmpps_person_match.db import get_db_connection
from hmpps_person_match.dependencies.logging.log import get_logger
from hmpps_person_match.models.health import Health, Status

ROUTE = "/health"

router = APIRouter()


@router.get(ROUTE)
async def get_health(
    connection: Annotated[Connection, Depends(get_db_connection)],
    logger: Annotated[Logger, Depends(get_logger)],
) -> Health:
    """
    GET request handler
    """
    try:
        await connection.execute(select(1))
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=Health(status=Status.UP).model_dump(),
        )
    except SQLAlchemyError as e:
        logger.error("Error executing health check query: %s", e)
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content=Health(status=Status.DOWN).model_dump(),
        )
