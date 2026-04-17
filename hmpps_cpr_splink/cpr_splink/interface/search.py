import uuid
from collections.abc import Mapping, Sequence
from typing import Any

from splink.internals.pipeline import CTEPipeline
from sqlalchemy import URL, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from hmpps_cpr_splink.cpr_splink.interface.block import (
    candidate_search,
    enqueue_join_term_frequency_tables,
)
from hmpps_cpr_splink.cpr_splink.interface.clean import clean_and_insert
from hmpps_cpr_splink.cpr_splink.interface.db import duckdb_connected_to_postgres
from hmpps_cpr_splink.cpr_splink.interface.score import score_records_to_person_scores
from hmpps_person_match.models.person.person import Person
from hmpps_person_match.models.person.person_batch import PersonBatch
from hmpps_person_match.models.person.person_score import PersonScore


def _materialise_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows]


def _new_temp_table_name() -> str:
    # Application-generated identifier only; do not replace with user input.
    return f"search_req_{uuid.uuid4().hex[:8]}"


async def _run_postgres_search_phase(
    person: Person,
    pg_conn: AsyncConnection,
) -> tuple[str, list[dict[str, Any]]]:
    """
    Run the temp-table / PostgreSQL blocking phase on one explicit connection.

    This function assumes the caller already owns the surrounding transaction.
    Nothing in this phase should commit.
    """
    search_match_id = str(uuid.uuid4())
    person_with_search_id = person.model_copy(update={"match_id": search_match_id})
    temp_table_name = _new_temp_table_name()

    await pg_conn.execute(
        text(
            f"CREATE TEMP TABLE {temp_table_name} (LIKE personmatch.person INCLUDING CONSTRAINTS) ON COMMIT DROP",  # noqa: S608
        ),
    )

    await clean_and_insert(
        records=PersonBatch(records=[person_with_search_id]),
        connection_pg=pg_conn,
        target_table_name=temp_table_name,
        upsert=False,
        commit=False,
    )

    candidates_data = await candidate_search(
        primary_record_id=search_match_id,
        connection_pg=pg_conn,
        primary_table_name=temp_table_name,
        candidates_table_name="personmatch.person",
    )

    if not candidates_data:
        return search_match_id, []

    pipeline = CTEPipeline()
    pipeline.enqueue_sql(
        sql=f"SELECT * FROM {temp_table_name} WHERE match_id = :mid",  # noqa: S608
        output_table_name="single_record",
    )

    enqueue_join_term_frequency_tables(
        pipeline,
        table_to_join_to="single_record",
        output_table_name="record_with_tfs",
    )

    sql = pipeline.generate_cte_pipeline_sql()
    res = await pg_conn.execute(text(sql), {"mid": search_match_id})
    search_record_with_tf = res.mappings().fetchall()

    full_records = [
        *_materialise_rows(search_record_with_tf),
        *_materialise_rows(candidates_data),
    ]
    return search_match_id, full_records


async def search_candidates(
    person: Person,
    pg_engine: AsyncEngine,
    pg_db_url: URL,
) -> list[PersonScore]:
    """
    Search for scored person-match candidates without persisting the search row.

    PostgreSQL work is done on one explicit connection and one explicit
    rollback-only transaction. DuckDB scoring happens only after all required
    PostgreSQL rows have been materialised into Python memory.
    """
    async with pg_engine.connect() as pg_conn:
        tx = await pg_conn.begin()
        try:
            search_match_id, full_records = await _run_postgres_search_phase(
                person=person,
                pg_conn=pg_conn,
            )
        finally:
            if tx.is_active:
                await tx.rollback()

    if not full_records:
        return []

    with duckdb_connected_to_postgres(pg_db_url) as duckdb_conn:
        return score_records_to_person_scores(
            duckdb_conn,
            primary_record_id=search_match_id,
            records_with_tf=full_records,
            table_name="all_records",
        )
