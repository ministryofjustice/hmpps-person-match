import uuid
from logging import Logger
from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_cpr_splink.cpr_splink.interface.search import search_candidates
from hmpps_person_match.db import get_db_session
from hmpps_person_match.dependencies.auth.jwt_bearer import JWTBearer
from hmpps_person_match.dependencies.logger.log import get_logger
from hmpps_person_match.domain.roles import Roles
from hmpps_person_match.models.person.person import Person
from hmpps_person_match.models.person.person_score import PersonScore

ROUTE = "/person/search"

DESCRIPTION = f"""
Search for matching candidates against a user-provided record.
The search record is NOT persisted - it exists only for the duration of the request.

**Note:** The `matchId` field in the request body is ignored. A unique search ID is
generated for each request to ensure the search record does not collide with any
existing records.

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
    Uses a temporary table - no data is persisted.

    Note: The matchId in the request body is overridden with a generated UUID
    to ensure uniqueness and avoid collisions with existing records.
    """
    search_match_id = str(uuid.uuid4())
    person_with_search_id = person.model_copy(update={"match_id": search_match_id})

    scores = await search_candidates(
        person=person_with_search_id,
        session=session,
        logger=logger,
    )

    return scores
