from fastapi import APIRouter, Depends

from hmpps_person_match.dependencies.auth.jwt_bearer import JWTBearer
from hmpps_person_match.domain.roles import Roles
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
async def search_person(person: Person) -> list[PersonScore]:
    """
    Temporary shell endpoint.

    Later this will search for candidate matches without persisting the
    input record. For now it just proves the route exists and can be called.
    """
    return []
