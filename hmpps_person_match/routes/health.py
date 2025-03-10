from fastapi import APIRouter, status
from fastapi.responses import JSONResponse

from hmpps_person_match.models.health import Health, Status

ROUTE = "/health"

router = APIRouter()


@router.get(ROUTE)
async def get_health() -> Health:
    """
    GET request handler
    """
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=Health(status=Status.UP).model_dump(),
    )
