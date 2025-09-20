from fastapi import APIRouter

from hmpps_person_match.domain.constants.openapi.tags import OpenAPITags
from hmpps_person_match.routes.cluster.is_cluster_valid import router as is_cluster_valid_router
from hmpps_person_match.routes.cluster.visualise_cluster import router as visualise_cluster_router

__all__ = ["router"]

router = APIRouter(tags=[OpenAPITags.CLUSTER])

router.include_router(is_cluster_valid_router)
router.include_router(visualise_cluster_router)
