from fastapi import APIRouter

from hmpps_person_match.routes.person.score.person_score import router as person_score_router

__all__ = ["router"]

router = APIRouter()

router.include_router(person_score_router)
