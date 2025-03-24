from logging import Logger
from typing import Annotated, TypedDict

from fastapi import APIRouter, Depends, Query, status
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_cpr_splink.cpr_splink.interface import score
from hmpps_person_match.db import get_db_session, url
from hmpps_person_match.dependencies.auth.jwt_bearer import JWTBearer
from hmpps_person_match.dependencies.logger.log import get_logger
from hmpps_person_match.domain.roles import Roles
from hmpps_person_match.domain.telemetry_events import TelemetryEvents

ROUTE = "/is-cluster-valid"

DESCRIPTION = f"""
    **Authorization Required:**
    - Bearer Token must be provided.
    - Role: **'{Roles.ROLE_PERSON_MATCH}'**
"""

router = APIRouter(
    dependencies=[Depends(JWTBearer(required_roles=[Roles.ROLE_PERSON_MATCH]))],
)

class ClusterReturn(TypedDict):
    isClusterValid: bool
    clusters: list[list[str]]


@router.get(ROUTE, description=DESCRIPTION)
async def get_cluster_validity(
    match_ids: Annotated[list[str], Query()],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    logger: Annotated[Logger, Depends(get_logger)],
) -> ClusterReturn:
    """
    Is cluster valid GET request handler
    Returns an indication of whether all supplied match_ids belong to the same cluster,
    as well as the cluster groupings for casese where they don't
    """
    missing_ids = await score.get_missing_record_ids(match_ids, session)
    if not missing_ids:
        clusters = await score.get_clusters(match_ids, url.pg_database_url, session)

        return_data = {
            "isClusterValid": clusters.is_single_cluster,
            "clusters": clusters.clusters_groupings,
        }

        logger.info(TelemetryEvents.IS_CLUSTER_VALID, extra=dict(clusters=clusters.clusters_groupings))
        return return_data
    else:
        return JSONResponse(content={"unknown_ids": missing_ids}, status_code=status.HTTP_404_NOT_FOUND)
