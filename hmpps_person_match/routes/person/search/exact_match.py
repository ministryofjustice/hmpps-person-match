from logging import Logger
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import JSONResponse

from hmpps_cpr_splink.cpr_splink.interface import exact_match
from hmpps_person_match.dependencies.auth.jwt_bearer import JWTBearer
from hmpps_person_match.dependencies.database import TransactionalSession
from hmpps_person_match.dependencies.logger.log import get_logger
from hmpps_person_match.domain.constants.error_messages import ErrorMessages
from hmpps_person_match.domain.roles import Roles
from hmpps_person_match.domain.telemetry_events import TelemetryEvents
from hmpps_person_match.models.person.exact_match_request import ExactMatchRequest
from hmpps_person_match.models.person.exact_match_response import ExactMatchResponse

ROUTE = "/person/search/exact-match"

DESCRIPTION = f"""
    **Authorization Required:**
    - Bearer Token must be provided.
    - Role: **'{Roles.ROLE_PERSON_MATCH}'**
"""

router = APIRouter(
    dependencies=[Depends(JWTBearer(required_roles=[Roles.ROLE_PERSON_MATCH]))],
)


@router.post(ROUTE, description=DESCRIPTION)
async def post_exact_match(
    request: ExactMatchRequest,
    session: TransactionalSession,
    logger: Annotated[Logger, Depends(get_logger)],
) -> ExactMatchResponse:
    try:
        match_ids = await exact_match.exact_match(request, session)
    except exact_match.InvalidExactMatchInputError as error:
        raise HTTPException(status_code=400, detail=ErrorMessages.INVALID_REQUEST) from error
    logger.info(
        TelemetryEvents.EXACT_MATCH_COMPLETED,
        extra={"candidate_size": len(match_ids)},
    )
    if len(match_ids) == 0:
        return JSONResponse(
            content={"matchIds":[]},
            status_code=status.HTTP_404_NOT_FOUND,
        ) # type: ignore
    return ExactMatchResponse(matchIds=match_ids)
