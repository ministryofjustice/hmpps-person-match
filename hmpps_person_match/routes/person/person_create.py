from logging import Logger
from typing import Annotated

from fastapi import APIRouter, Depends, Response, status
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_cpr_splink.cpr_splink.interface import clean
from hmpps_person_match.db import get_db_session
from hmpps_person_match.dependencies.auth.jwt_bearer import JWTBearer
from hmpps_person_match.dependencies.logger.log import get_logger
from hmpps_person_match.domain.roles import Roles
from hmpps_person_match.domain.telemetry_events import TelemetryEvents
from hmpps_person_match.models.person.person import Person
from hmpps_person_match.models.person.person_batch import PersonBatch

ROUTE = "/person"

DESCRIPTION = f"""
    **Authorization Required:**
    - Bearer Token must be provided.
    - Role: **'{Roles.ROLE_PERSON_MATCH}'**
"""

router = APIRouter(
    dependencies=[Depends(JWTBearer(required_roles=[Roles.ROLE_PERSON_MATCH]))],
)


@router.post(ROUTE, description=DESCRIPTION)
async def post_person(
    person: Person,
    connection: Annotated[AsyncSession, Depends(get_db_session)],
    logger: Annotated[Logger, Depends(get_logger)],
) -> Response:
    """
    Person POST request handler
    """
    logger.info(TelemetryEvents.PERSON_UPDATED_OR_CREATED, extra=dict(matchId=person.match_id))
    await clean.clean_and_insert(PersonBatch(records=[person]), connection)
    return JSONResponse(content={}, status_code=status.HTTP_200_OK)
