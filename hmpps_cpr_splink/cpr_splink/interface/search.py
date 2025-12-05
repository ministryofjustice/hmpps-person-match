import uuid

import duckdb
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_cpr_splink.cpr_splink.interface.block import (
    candidate_search_from_table,
    get_record_with_term_frequencies,
)
from hmpps_cpr_splink.cpr_splink.interface.clean import clean_record_to_duckdb
from hmpps_cpr_splink.cpr_splink.interface.db import insert_duckdb_table_into_postgres_table
from hmpps_cpr_splink.cpr_splink.interface.score import score_records_to_person_scores
from hmpps_person_match.models.person.person import Person
from hmpps_person_match.models.person.person_score import PersonScore


async def search_candidates(
    person: Person,
    session: AsyncSession,
) -> list[PersonScore]:
    """
    Search for candidates matching the provided person record.

    Flow:
    1. Create temp table in PostgreSQL
    2. Clean record and insert into temp table (parameterized - safe)
    3. Run blocking query (AsyncSession can see temp table)
    4. Pass candidates to DuckDB for Splink scoring
    5. Return scores
    6. Drop temp table
    """
    search_match_id = person.match_id
    temp_table_name = f"search_{uuid.uuid4().hex[:8]}"

    try:
        # Step 1: Create temp table
        # Note: ON COMMIT DROP is defensive - it only kicks in if the transaction commits.
        # In this code path we never call session.commit(), so the table lives for the
        # connection lifetime and is removed either by our explicit DROP or when the
        # connection returns to the pool.
        await session.execute(
            text(f"CREATE TEMP TABLE {temp_table_name} (LIKE personmatch.person INCLUDING ALL) ON COMMIT DROP"),
        )

        # Step 2: Clean and insert the search record
        await _clean_and_insert_to_temp_table(
            person=person,
            session=session,
            temp_table_name=temp_table_name,
        )

        # Step 3: Run blocking query (uses same session - can see temp table)
        candidates_data = await candidate_search_from_table(
            primary_record_id=search_match_id,
            primary_table=temp_table_name,
            candidates_table="personmatch.person",
            connection_pg=session,
        )

        if not candidates_data:
            return []

        # Step 4: Fetch search record with term frequencies (same format as candidates)
        search_record_with_tf = await get_record_with_term_frequencies(
            record_id=search_match_id,
            table_name=temp_table_name,
            connection_pg=session,
        )

        if not search_record_with_tf:
            return []

        # Step 5: Pass to DuckDB for Splink scoring
        # Combine search record + candidates and use shared scoring helper
        full_records = [*search_record_with_tf, *candidates_data]

        with duckdb.connect(":memory:") as duckdb_conn:
            scores = score_records_to_person_scores(
                duckdb_conn,
                search_match_id,
                full_records,
                table_name="all_records",
            )

        return scores

    finally:
        # Step 6: Cleanup (defensive - temp table would auto-drop anyway)
        try:
            await session.execute(text(f"DROP TABLE IF EXISTS {temp_table_name}"))
        except SQLAlchemyError:
            pass


async def _clean_and_insert_to_temp_table(
    person: Person,
    session: AsyncSession,
    temp_table_name: str,
) -> None:
    """
    Clean the person record and insert into the temp table.
    Uses parameterized query - safe from SQL injection.
    """
    # Use existing DuckDB cleaning logic (context manager ensures cleanup)
    with duckdb.connect(":memory:") as duckdb_conn:
        cleaned_table = clean_record_to_duckdb(duckdb_conn, person)

        # Insert from DuckDB to Postgres temp table (parameterized, no upsert/commit)
        await insert_duckdb_table_into_postgres_table(
            ddb_tab=duckdb_conn.table(cleaned_table),
            pg_table_name=temp_table_name,
            connection_pg=session,
            upsert=False,
            commit=False,
        )
