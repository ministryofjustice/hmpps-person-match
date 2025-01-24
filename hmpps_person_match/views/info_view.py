import os

from fastapi import APIRouter

from hmpps_person_match.models.info import Info

ROUTE = "/info"

router = APIRouter()


@router.get(ROUTE)
def info() -> Info:
    """
    GET request handler
    """
    return Info(
        version=os.environ.get("APP_BUILD_NUMBER", "unknown"),
        commit_id=os.environ.get("APP_GIT_REF", "unknown"),
        branch=os.environ.get("APP_GIT_BRANCH", "unknown"),
    )
