from fastapi import APIRouter

from hmpps_person_match.domain.constants.openapi.tags import OpenAPITags
from hmpps_person_match.routes.person.migration import router as person_migration_router
from hmpps_person_match.routes.person.person_create import router as person_create_router
from hmpps_person_match.routes.person.person_delete import router as person_delete_router
from hmpps_person_match.routes.person.score import router as person_score_router
from hmpps_person_match.routes.person.search import router as person_search_router

__all__ = ["router"]

router = APIRouter(tags=[OpenAPITags.PERSON])

# Person
router.include_router(person_create_router)
router.include_router(person_delete_router)

# Migration
router.include_router(person_migration_router)

# Score
router.include_router(person_score_router)

# Search
router.include_router(person_search_router)
