import uuid

from sqlalchemy import URL, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from hmpps_cpr_splink.cpr_splink.interface.block import candidate_search
from hmpps_cpr_splink.cpr_splink.interface.clean import clean_and_insert_in_transaction
from hmpps_person_match.models.person.person import Person
from hmpps_person_match.models.person.person_batch import PersonBatch
from hmpps_person_match.models.person.person_score import PersonScore


async def _run_postgres_search_phase(
    person: Person,
    pg_conn: AsyncConnection,
) -> None:
    """
    Run the temp-table / PostgreSQL blocking phase on one explicit connection.

    This function assumes the caller already owns the surrounding transaction.
    Nothing in this phase should commit.
    """
    search_match_id = str(uuid.uuid4())
    person_with_search_id = person.model_copy(update={"match_id": search_match_id})

    # Can't use CREATE TEMP TABLE person_search_input_temp (LIKE personmatch.person) because
    # it'll copy over the non-nullability of id, and here it's fine for id to be null
    await pg_conn.execute(
        text(
            "CREATE TEMP TABLE person_search_input_temp ON COMMIT DROP AS "
            "SELECT * FROM personmatch.person WITH NO DATA",
        ),
    )

    await clean_and_insert_in_transaction(
        records=PersonBatch(records=[person_with_search_id]),
        connection_pg=pg_conn,
        target_table_name="person_search_input_temp",
    )
    candidates = await candidate_search(
        primary_record_id=search_match_id,
        connection_pg=pg_conn,
        primary_table_name="person_search_input_temp",
        candidates_table_name="personmatch.person",
    )

    return [dict(row) for row in candidates]


async def search_candidates(
    person: Person,
    pg_engine: AsyncEngine,
    pg_db_url: URL,
) -> list[PersonScore]:
    async with pg_engine.connect() as pg_conn:
        tx = await pg_conn.begin()
        try:
            rows = await _run_postgres_search_phase(
                person=person,
                pg_conn=pg_conn,
            )
        finally:
            if tx.is_active:
                await tx.rollback()

    return rows
