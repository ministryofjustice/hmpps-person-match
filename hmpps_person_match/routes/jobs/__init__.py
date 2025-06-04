from fastapi import APIRouter

from hmpps_person_match.domain.constants.openapi.tags import OpenAPITags
from hmpps_person_match.routes.jobs.record_count_report import router as record_count_report_router
from hmpps_person_match.routes.jobs.term_frequencies import router as term_frequency_router

__all__ = ["router"]

router = APIRouter(tags=[OpenAPITags.JOBS])

# Term Frequencies
router.include_router(term_frequency_router)
router.include_router(record_count_report_router)
