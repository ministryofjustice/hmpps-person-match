from typing import Annotated

from fastapi import APIRouter, Depends, status
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_cpr_splink.cpr_splink.interface import score
from hmpps_person_match.db import get_db_session, url
from hmpps_person_match.dependencies.auth.jwt_bearer import JWTBearer
from hmpps_person_match.domain.roles import Roles
from hmpps_person_match.models.person.person_best_match import PersonBestMatch

ROUTE = "/person/best-match/{source_system}/{match_id}"

DESCRIPTION = f"""
    **Authorization Required:**
    - Bearer Token must be provided.
    - Role: **'{Roles.ROLE_PERSON_MATCH}'**
"""

router = APIRouter(
    dependencies=[Depends(JWTBearer(required_roles=[Roles.ROLE_PERSON_MATCH]))],
)

@router.get(ROUTE, description=DESCRIPTION)
async def get_person_best_match(
    match_id: str,
    source_system: str,
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> PersonBestMatch:
    """
    Person best match GET request handler
    Takes a matchId and a source system
    Returns MATCH,NO_MATCH or POSSIBLE_MATCH if there is a matching record
    in the suppleid source system
    """
    if await score.match_record_exists(match_id, session):
        match: PersonBestMatch = await score.get_best_match(
            match_id,
            source_system,
            url.pg_database_url,
            session,
        )

        return match
    else:
        return JSONResponse(content={}, status_code=status.HTTP_404_NOT_FOUND)  # type: ignore
