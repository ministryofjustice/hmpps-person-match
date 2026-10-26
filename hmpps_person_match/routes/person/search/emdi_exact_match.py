from logging import Logger
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from hmpps_cpr_splink.cpr_splink.interface import emdi_exact_match
from hmpps_person_match.dependencies.auth.jwt_bearer import JWTBearer
from hmpps_person_match.dependencies.database import TransactionalSession
from hmpps_person_match.dependencies.logger.log import get_logger
from hmpps_person_match.domain.constants.error_messages import ErrorMessages
from hmpps_person_match.domain.roles import Roles
from hmpps_person_match.domain.telemetry_events import TelemetryEvents
from hmpps_person_match.models.person.emdi_exact_match_request import EmdiExactMatchRequest
from hmpps_person_match.models.person.emdi_exact_match_response import EmdiExactMatchResponse

ROUTE = "/person/search/emdi-exact-match"

DESCRIPTION = f"""
    **Authorization Required:**
    - Bearer Token must be provided.
    - Role: **'{Roles.ROLE_PERSON_MATCH}'**
"""

router = APIRouter(
    dependencies=[Depends(JWTBearer(required_roles=[Roles.ROLE_PERSON_MATCH]))],
)


@router.post(ROUTE, description=DESCRIPTION)
async def post_emdi_exact_match(
    request: EmdiExactMatchRequest,
    session: TransactionalSession,
    logger: Annotated[Logger, Depends(get_logger)],
) -> EmdiExactMatchResponse:
    try:
        match_ids = await emdi_exact_match.emdi_exact_match(request, session)
    except emdi_exact_match.InvalidEmdiExactMatchInputError as error:
        raise HTTPException(status_code=400, detail=ErrorMessages.INVALID_REQUEST) from error
    logger.info(
        TelemetryEvents.EMDI_EXACT_MATCH_COMPLETED,
        extra={"candidate_size": len(match_ids)},
    )
    return EmdiExactMatchResponse(matchIds=match_ids)
