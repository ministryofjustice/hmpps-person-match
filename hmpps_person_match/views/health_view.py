from logging import Logger
from typing import Annotated

from fastapi import APIRouter, Depends, status
from fastapi.responses import JSONResponse
from sqlalchemy.exc import OperationalError
from sqlmodel import Session, select

from hmpps_person_match.db import get_db_session
from hmpps_person_match.dependencies.logging import get_logger

ROUTE = "/health"

router = APIRouter()


@router.get(ROUTE)
def get_health(session: Annotated[Session,  Depends(get_db_session)],
               logger: Annotated[Logger, Depends(get_logger)]):
    """
    GET request handler
    """
    try:
        session.exec(select(1))
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "status": "UP",
            },
        )
    except OperationalError as e:
        logger.error("Error executing health check query: %s", e)
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "DOWN",
            },
        )
