from fastapi import APIRouter

ROUTE = "/person/match"

router = APIRouter()


@router.post(ROUTE, tags=["person"])
def post_person_match():
    """
    Person Match POST request handler
    """
    return {}
