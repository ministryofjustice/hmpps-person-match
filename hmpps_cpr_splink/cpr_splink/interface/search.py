import uuid
from collections.abc import Mapping, Sequence
from typing import Any

import duckdb
from splink.internals.pipeline import CTEPipeline
from sqlalchemy import URL, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from hmpps_cpr_splink.cpr_splink.interface.block import (
    candidate_search,
    enqueue_join_term_frequency_tables,
)
from hmpps_cpr_splink.cpr_splink.interface.clean import clean_and_insert_in_transaction
from hmpps_cpr_splink.cpr_splink.interface.score import score_candidates
from hmpps_person_match.models.person.person import Person
from hmpps_person_match.models.person.person_batch import PersonBatch
from hmpps_person_match.models.person.person_score import PersonScore


def _materialise_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows]


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

    pipeline = CTEPipeline()
    pipeline.enqueue_sql(
        sql="SELECT * FROM person_search_input_temp",
        output_table_name="single_record",
    )

    enqueue_join_term_frequency_tables(
        pipeline,
        table_to_join_to="single_record",
        output_table_name="record_with_tfs",
    )

    sql = pipeline.generate_cte_pipeline_sql()
    result = await pg_conn.execute(text(sql))
    search_record_with_tf = result.mappings().all()

    full_records = [
        *_materialise_rows(search_record_with_tf),
        *_materialise_rows(candidates),
    ]

    return search_match_id, full_records


async def search_candidates(
    person: Person,
    pg_engine: AsyncEngine,
    pg_db_url: URL,
) -> list[PersonScore]:
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

    with duckdb.connect(":memory:") as duckdb_conn:
        return score_candidates(
            duckdb_conn,
            search_match_id,
            full_records,
            table_name="all_records",
        )
