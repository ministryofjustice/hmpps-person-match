from logging import Logger
from typing import Annotated

from fastapi import APIRouter, Body, Depends, status
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_cpr_splink.cpr_splink.interface import score
from hmpps_cpr_splink.cpr_splink.interface.visualise import get_nodes_edges
from hmpps_cpr_splink.cpr_splink.visualisation import load_base_spec
from hmpps_person_match.db import get_db_session, url
from hmpps_person_match.dependencies.auth.jwt_bearer import JWTBearer
from hmpps_person_match.dependencies.logger.log import get_logger
from hmpps_person_match.domain.roles import Roles
from hmpps_person_match.models.cluster.is_cluster_valid import MissingRecordIds
from hmpps_cpr_splink.cpr_splink.visualisation.munge_nodes_edges import build_spec

ROUTE = "/visualise-cluster"

DESCRIPTION = f"""
    **Authorization Required:**
    - Bearer Token must be provided.
    - Role: **'{Roles.ROLE_PERSON_MATCH}'**

    Given a list of matchIds, this will:
    - compute match scores between these matchIds; and
    - return a vega spec to visualise the clusters formed by those matchIds.
"""

router = APIRouter(
    dependencies=[Depends(JWTBearer(required_roles=[Roles.ROLE_PERSON_MATCH]))],
)


@router.post(ROUTE, description=DESCRIPTION)
async def get_cluster_vis(
    match_ids: Annotated[list[str], Body(example=["ea59b57f-f3b6-4f77-88dd-64f86d37dffd"])],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    logger: Annotated[Logger, Depends(get_logger)],
) -> dict:
    """
    visualise cluster GET request handler
    Returns a vega spec to visualise the clusters formed by the supplied match_ids
    """
    missing_ids = await score.get_missing_record_ids(match_ids, session)

    if not missing_ids:
        nodes, edges = await get_nodes_edges(match_ids, url.pg_database_url, session)
        spec = build_spec(nodes, edges)
        return JSONResponse(content={"spec": spec}, status_code=status.HTTP_200_OK)
    else:
        return JSONResponse(
            content=MissingRecordIds(unknownIds=missing_ids).model_dump(by_alias=True),
            status_code=status.HTTP_404_NOT_FOUND,
        )
