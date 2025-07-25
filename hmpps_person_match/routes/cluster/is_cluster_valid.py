from logging import Logger
from typing import Annotated

from fastapi import APIRouter, Body, Depends, status
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_cpr_splink.cpr_splink.interface import score
from hmpps_person_match.db import get_db_session, url
from hmpps_person_match.dependencies.auth.jwt_bearer import JWTBearer
from hmpps_person_match.dependencies.logger.log import get_logger
from hmpps_person_match.domain.roles import Roles
from hmpps_person_match.domain.telemetry_events import TelemetryEvents
from hmpps_person_match.models.cluster.is_cluster_valid import IsClusterValid, MissingRecordIds

ROUTE = "/is-cluster-valid"

DESCRIPTION = f"""
    **Authorization Required:**
    - Bearer Token must be provided.
    - Role: **'{Roles.ROLE_PERSON_MATCH}'**

    Given a list of matchIds, this will return whether they logically belong to the same cluster
    according purely to their model scores.
    It will not identify any further matchIds that may also belong to the same cluster.

    Additionally, the groupings of the matchIds into clusters is returned.
"""

router = APIRouter(
    dependencies=[Depends(JWTBearer(required_roles=[Roles.ROLE_PERSON_MATCH]))],
)


@router.post(ROUTE, description=DESCRIPTION)
async def get_cluster_validity(
    match_ids: Annotated[list[str], Body(example=["ea59b57f-f3b6-4f77-88dd-64f86d37dffd"])],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    logger: Annotated[Logger, Depends(get_logger)],
) -> IsClusterValid:
    """
    Is cluster valid GET request handler
    Returns an indication of whether all supplied match_ids belong to the same cluster,
    as well as the cluster groupings for casese where they don't
    """
    missing_ids = await score.get_missing_record_ids(match_ids, session)
    if not missing_ids:
        clusters_info = await score.get_clusters(match_ids, url.pg_database_url, session)
        logger.info(
            TelemetryEvents.IS_CLUSTER_VALID,
            extra=dict(isClusterValid=clusters_info.is_single_cluster, clusters=str(clusters_info.clusters_groupings)),
        )
        return IsClusterValid(
            isClusterValid=clusters_info.is_single_cluster,
            clusters=clusters_info.clusters_groupings,
        )
    else:
        return JSONResponse(
            content=MissingRecordIds(unknownIds=missing_ids).model_dump(by_alias=True),
            status_code=status.HTTP_404_NOT_FOUND,
        )
