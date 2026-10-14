from fastapi import APIRouter

from hmpps_person_match.routes.person.search.emdi_exact_match import router as emdi_exact_match_router
from hmpps_person_match.routes.person.search.person_search import router as person_search_router
from hmpps_person_match.routes.person.search.person_search_by_match_id import (
    router as person_search_by_match_id_router,
)

__all__ = ["router"]

router = APIRouter()
router.include_router(emdi_exact_match_router)
router.include_router(person_search_router)
router.include_router(person_search_by_match_id_router)
