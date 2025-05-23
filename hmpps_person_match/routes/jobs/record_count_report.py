from logging import Logger
from typing import Annotated

from fastapi import APIRouter, Depends, status
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_person_match.db import get_db_session
from hmpps_person_match.dependencies.logger.log import get_logger
from hmpps_person_match.domain.telemetry_events import TelemetryEvents

ROUTE = "/jobs/recordcountreport"

router = APIRouter()


@router.get(ROUTE, include_in_schema=False)
async def get_record_count_report(
    session: Annotated[AsyncSession, Depends(get_db_session)],
    logger: Annotated[Logger, Depends(get_logger)],
) -> JSONResponse:
    """
    Record count report GET request handler
    Returns the count of person record
    """
    result = await session.execute(
        text("select p.source_system, count(p.source_system) from personmatch.person p group by p.source_system"),
    )
    result_mapping_dict = {row["source_system"]: row["count"] for row in result.mappings().fetchall()}
    logger.info(TelemetryEvents.PERSON_MATCH_RECORD_COUNT_REPORT, extra=result_mapping_dict)

    return JSONResponse(content={}, status_code=status.HTTP_200_OK)
