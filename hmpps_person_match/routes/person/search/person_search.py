from logging import Logger
from fastapi import APIRouter, Depends
from typing import Annotated
from hmpps_person_match.dependencies.auth.jwt_bearer import JWTBearer
from hmpps_person_match.domain.roles import Roles
from hmpps_person_match.models.person.person import Person
from hmpps_person_match.models.person.person_score import PersonScore
from hmpps_person_match.domain.telemetry_events import TelemetryEvents
from hmpps_person_match.dependencies.logger.log import get_logger

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
    logger: Annotated[Logger, Depends(get_logger)],
) -> list[PersonScore]:
    """
    Temporary shell endpoint.

    Later this will search for candidate matches without persisting the
    input record. For now it just proves the route exists and can be called.
    """
    logger.info(TelemetryEvents.PERSON_SEARCH, extra={"candidate_size": 0})
    return []
