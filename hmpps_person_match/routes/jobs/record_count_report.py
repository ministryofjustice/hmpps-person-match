from logging import Logger
from typing import Annotated

from fastapi import APIRouter, Depends, Response, status
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_person_match.db import get_db_session
from hmpps_person_match.dependencies.auth.jwt_bearer import JWTBearer
from hmpps_person_match.dependencies.logger.log import get_logger
from hmpps_person_match.domain.roles import Roles

ROUTE = "/jobs/recordcountreport"

DESCRIPTION = f"""
    **Authorization Required:**
    - Bearer Token must be provided.
    - Role: **'{Roles.ROLE_PERSON_MATCH}'**
"""

router = APIRouter(
    dependencies=[Depends(JWTBearer(required_roles=[Roles.ROLE_PERSON_MATCH]))],
)


@router.get(ROUTE, description=DESCRIPTION)
async def get_record_count_report(
    session: Annotated[AsyncSession, Depends(get_db_session)],
    logger: Annotated[Logger, Depends(get_logger)],
) -> Response:
    """
    Record count report GET request handler
    Returns the count of person record
    """
    return JSONResponse(content={}, status_code=status.HTTP_200_OK)
