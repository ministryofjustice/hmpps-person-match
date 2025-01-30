from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncConnection

from hmpps_cpr_splink.cpr_splink.interface import clean
from hmpps_person_match.db import get_db_connection

# from hmpps_person_match.dependencies.auth.jwt_bearer import JWTBearer
from hmpps_person_match.domain.constants.openapi.tags import OpenAPITags
from hmpps_person_match.domain.roles import Roles
from hmpps_person_match.models.person import Person

ROUTE = "/person"

router = APIRouter(
    tags=[OpenAPITags.MATCH],
)


@router.post(
    ROUTE,
    # dependencies=[Depends(JWTBearer(required_roles=[Roles.ROLE_PERSON_MATCH]))],
    description=f"""
    **Authorization Required:**
    - Bearer Token must be provided.
    - Role: **'{Roles.ROLE_PERSON_MATCH}'**
    """,
)
async def post_person_match(
        person: Person,
        connection: Annotated[AsyncConnection, Depends(get_db_connection)],
    ):
    """
    Person Match POST request handler
    """
    await clean.clean_and_insert(person, connection)
    return {}
