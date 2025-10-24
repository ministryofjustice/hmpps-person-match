import os

from fastapi import APIRouter

from hmpps_person_match.models.info import Build, Commit, Git, Info

ROUTE = "/info"

router = APIRouter()


@router.get(ROUTE)
def info() -> Info:
    """
    GET request handler
    """
    return Info(
        build=Build(version=os.environ.get("APP_BUILD_NUMBER", "unknown")),
        git=Git(
            branch=os.environ.get("APP_GIT_BRANCH", "unknown"),
            commit=Commit(id=os.environ.get("APP_GIT_REF", "unknown")),
        ),
    )
