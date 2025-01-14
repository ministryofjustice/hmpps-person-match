from fastapi import APIRouter

from hmpps_person_match.domain.constants.openapi.tags import OpenAPITags

ROUTE = "/person/match"

router = APIRouter(tags=[OpenAPITags.MATCH])


@router.post(ROUTE)
def post_person_match():
    """
    Person Match POST request handler
    """
    return {}
