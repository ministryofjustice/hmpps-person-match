from fastapi import APIRouter

from hmpps_person_match.routes.person.search.person_search import (
    router as person_search_router,
)

__all__ = ["router"]

router = APIRouter()

router.include_router(person_search_router)
