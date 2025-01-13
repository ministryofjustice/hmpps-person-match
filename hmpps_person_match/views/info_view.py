import os

from fastapi import APIRouter

ROUTE = "/info"

router = APIRouter()

@router.get(ROUTE)
def info():
    """
    GET request handler
    """
    version = os.environ.get("APP_BUILD_NUMBER", "unknown")
    commit_id = os.environ.get("APP_GIT_REF", "unknown")
    branch = os.environ.get("APP_GIT_BRANCH", "unknown")
    return dict(
        version=version,
        commit_id=commit_id,
        branch=branch,
    )
