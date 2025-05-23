from logging import Logger
from typing import Annotated

from fastapi import APIRouter, BackgroundTasks, Depends, Response, status
from fastapi.responses import JSONResponse
from sqlalchemy import text

from hmpps_person_match.db import AsyncSessionLocal
from hmpps_person_match.dependencies.logger.log import get_logger
from hmpps_person_match.domain.telemetry_events import TelemetryEvents

ROUTE = "/jobs/termfrequencies"

router = APIRouter()


TERM_FREQUENCY_TABLES = [
    "term_frequencies_cro_single",
    "term_frequencies_date_of_birth",
    "term_frequencies_first_and_last_name_std",
    "term_frequencies_last_name_std",
    "term_frequencies_name_1_std",
    "term_frequencies_name_2_std",
    "term_frequencies_pnc_single",
    "term_frequencies_postcode",
]


@router.post(ROUTE, include_in_schema=False)
async def post_term_frequency(
    background_tasks: BackgroundTasks,
    logger: Annotated[Logger, Depends(get_logger)],
) -> Response:
    """
    Term Frequency Job POST request handler
    """
    logger.info(TelemetryEvents.JOBS_TERM_FREQUENCY_REFRESH)
    background_tasks.add_task(trigger_term_frequency_refresh)
    return JSONResponse(content={}, status_code=status.HTTP_200_OK)


async def trigger_term_frequency_refresh():
    """
    Refresh the term frequency materialized views
    """
    async with AsyncSessionLocal() as session, session.begin():
        for tf_table in TERM_FREQUENCY_TABLES:
            await session.execute(text(f"REFRESH MATERIALIZED VIEW CONCURRENTLY personmatch.{tf_table};"))
