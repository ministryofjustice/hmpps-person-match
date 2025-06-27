from logging import Logger
from typing import Annotated

from fastapi import APIRouter, Depends, status
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_cpr_splink.cpr_splink.interface import score
from hmpps_person_match.db import get_db_session, url
from hmpps_person_match.dependencies.auth.jwt_bearer import JWTBearer
from hmpps_person_match.dependencies.logger.log import get_logger
from hmpps_person_match.domain.roles import Roles
from hmpps_person_match.domain.telemetry_events import TelemetryEvents
from hmpps_person_match.models.person.person_score import PersonScore

ROUTE = "/person/score/{match_id}"

DESCRIPTION = f"""
    **Authorization Required:**
    - Bearer Token must be provided.
    - Role: **'{Roles.ROLE_PERSON_MATCH}'**
"""

router = APIRouter(
    dependencies=[Depends(JWTBearer(required_roles=[Roles.ROLE_PERSON_MATCH]))],
)


@router.get(ROUTE, description=DESCRIPTION)
async def get_person_score(
    match_id: str,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    logger: Annotated[Logger, Depends(get_logger)],
) -> list[PersonScore]:
    """
    Person score GET request handler
    Returns a list of scored candidates against the provided record match identifier
    """
    if await score.match_record_exists(match_id, session):
        scored_candidates: list[PersonScore] = await score.get_scored_candidates(
            match_id,
            url.pg_database_url,
            session,
        )
        logger.info(TelemetryEvents.PERSON_SCORE, extra=dict(matchId=match_id, candidate_size=len(scored_candidates)))
        return scored_candidates
    else:
        return JSONResponse(content={}, status_code=status.HTTP_404_NOT_FOUND)
