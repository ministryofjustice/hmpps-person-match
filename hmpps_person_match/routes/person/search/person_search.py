from logging import Logger
from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_cpr_splink.cpr_splink.interface.search import search_candidates
from hmpps_person_match.db import get_db_session, url
from hmpps_person_match.dependencies.auth.jwt_bearer import JWTBearer
from hmpps_person_match.dependencies.logger.log import get_logger
from hmpps_person_match.domain.roles import Roles
from hmpps_person_match.domain.telemetry_events import TelemetryEvents
from hmpps_person_match.models.person.person import Person
from hmpps_person_match.models.person.person_score import PersonScore

ROUTE = "/person/search"

DESCRIPTION = f"""
    **Authorization Required:**
    - Bearer Token must be provided.
    - Role: **'{Roles.ROLE_PERSON_MATCH}'**
"""

router = APIRouter(
    dependencies=[Depends(JWTBearer(required_roles=[Roles.ROLE_PERSON_MATCH]))],
)


@router.post(ROUTE, description=DESCRIPTION)
async def search_person(
    person: Person,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    logger: Annotated[Logger, Depends(get_logger)],
) -> list[PersonScore]:
    """
    Search for candidates matching the provided person record.

    - matchId in the payload (if present) is ignored/overridden.
    - Uses a temp table; nothing is written permanently to personmatch.person.
    """
    scores = await search_candidates(
        person=person,
        session=session,
        pg_db_url=url.pg_database_url,
    )
    logger.info(TelemetryEvents.PERSON_SEARCH, extra=dict(candidate_size=len(scores)))
    return scores
