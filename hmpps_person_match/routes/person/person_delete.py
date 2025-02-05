from logging import Logger
from typing import Annotated

from fastapi import APIRouter, Depends, Response, status
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from hmpps_person_match.db import get_db_connection
from hmpps_person_match.dependencies.auth.jwt_bearer import JWTBearer
from hmpps_person_match.dependencies.logging.log import get_logger
from hmpps_person_match.domain.roles import Roles
from hmpps_person_match.models.person.person_identifier import PersonIdentifier

ROUTE = "/person"

DESCRIPTION = f"""
    **Authorization Required:**
    - Bearer Token must be provided.
    - Role: **'{Roles.ROLE_PERSON_MATCH}'**
"""

router = APIRouter(
    dependencies=[Depends(JWTBearer(required_roles=[Roles.ROLE_PERSON_MATCH]))],
)

@router.delete(ROUTE, description=DESCRIPTION)
async def delete_person(
    person_identifier: PersonIdentifier,
    connection: Annotated[AsyncConnection, Depends(get_db_connection)],
    logger: Annotated[Logger, Depends(get_logger)],
) -> Response:
    """
    Person DELETE request handler
    """
    logger.info("Deleting person record", extra={"custom_dimensions": {"id": person_identifier.match_id}})
    query = text("DELETE FROM personmatch.person WHERE match_id = :match_id")
    result = await connection.execute(query, {"match_id": person_identifier.match_id})
    if result.rowcount == 0:
        return JSONResponse(content={}, status_code=status.HTTP_404_NOT_FOUND)
    return JSONResponse(content={}, status_code=status.HTTP_200_OK)
