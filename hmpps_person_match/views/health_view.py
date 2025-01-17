from fastapi import APIRouter

ROUTE = "/health"

router = APIRouter()


@router.get(ROUTE)
def get_health():
    """
    GET request handler
    """
    return {"status": "UP"}
