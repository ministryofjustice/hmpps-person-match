from logging import Logger
from typing import Annotated

from fastapi import APIRouter, Depends, Response, status
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncConnection

from hmpps_cpr_splink.cpr_splink.interface import score
from hmpps_person_match.db import get_db_connection
from hmpps_person_match.dependencies.auth.jwt_bearer import JWTBearer
from hmpps_person_match.dependencies.logger.log import get_logger
from hmpps_person_match.domain.roles import Roles
from hmpps_person_match.domain.telemetry_events import TelemetryEvents

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
    connection: Annotated[AsyncConnection, Depends(get_db_connection)],
    logger: Annotated[Logger, Depends(get_logger)],
) -> Response:
    """
    Person score GET request handler
    Returns a list of scored candidates against the provided record match identifier
    """
    scored_candidates: list[score.ScoredCandidate] = await score.get_scored_candidates(match_id, connection)
    logger.info(TelemetryEvents.PERSON_SCORE, extra=dict(matchId=match_id, candidate_size=len(scored_candidates)))
    return JSONResponse(content=scored_candidates, status_code=status.HTTP_200_OK)
