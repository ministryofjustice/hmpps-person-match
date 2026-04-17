from sqlalchemy import URL, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from hmpps_person_match.models.person.person import Person
from hmpps_person_match.models.person.person_score import PersonScore


async def _run_postgres_search_phase(
    pg_conn: AsyncConnection,
) -> None:
    """
    Run the temp-table / PostgreSQL blocking phase on one explicit connection.

    This function assumes the caller already owns the surrounding transaction.
    Nothing in this phase should commit.
    """
    await pg_conn.execute(
        text(
            "CREATE TEMP TABLE person_search_input_temp (LIKE personmatch.person INCLUDING CONSTRAINTS) ON COMMIT DROP"
        ),
    )


async def search_candidates(
    person: Person,
    pg_engine: AsyncEngine,
    pg_db_url: URL,
) -> list[PersonScore]:
    async with pg_engine.connect() as pg_conn:
        tx = await pg_conn.begin()
        try:
            await _run_postgres_search_phase(pg_conn=pg_conn)
        finally:
            if tx.is_active:
                await tx.rollback()

    return []
