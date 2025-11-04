from fastapi import APIRouter

from hmpps_person_match.models.health import Health, Status

ROUTE = "/health"
PING_ROUTE = "/health/ping"

router = APIRouter()


@router.get(ROUTE)
@router.get(PING_ROUTE)
async def get_health() -> Health:
    """
    GET request handler
    """
    return Health(status=Status.UP)
