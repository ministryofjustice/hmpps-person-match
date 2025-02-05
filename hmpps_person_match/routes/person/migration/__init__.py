from fastapi import APIRouter

from hmpps_person_match.routes.person.migration.person_migrate import router as person_migrate_router

__all__ = ["router"]

router = APIRouter()

router.include_router(person_migrate_router)
