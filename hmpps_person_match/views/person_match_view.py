from fastapi import APIRouter, Depends

from hmpps_person_match.dependencies.auth.jwt_bearer import JWTBearer
from hmpps_person_match.domain.constants.openapi.tags import OpenAPITags
from hmpps_person_match.domain.roles import Roles
from hmpps_person_match.models.person import Person

ROUTE = "/person/match"

router = APIRouter(
    tags=[OpenAPITags.MATCH],
)


@router.post(ROUTE, dependencies=[Depends(JWTBearer(required_roles=[Roles.ROLE_PERSON_MATCH]))])
def post_person_match(person: Person):
    """
    Person Match POST request handler
    """
    return {}
