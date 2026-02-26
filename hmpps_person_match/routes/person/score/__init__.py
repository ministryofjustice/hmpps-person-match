from fastapi import APIRouter

from hmpps_person_match.routes.person.score.person_probation_match import router as person_probation_match_router
from hmpps_person_match.routes.person.score.person_score import router as person_score_router

__all__ = ["router"]

router = APIRouter()

router.include_router(person_score_router)
router.include_router(person_probation_match_router)
