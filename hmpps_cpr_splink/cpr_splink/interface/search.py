import uuid

from splink.internals.pipeline import CTEPipeline
from sqlalchemy import URL, text
from sqlalchemy.ext.asyncio import AsyncSession

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


async def search_candidates(
    person: Person,
    session: AsyncSession,
    pg_db_url: URL,
) -> list[PersonScore]:
    search_match_id = str(uuid.uuid4())
    person_with_search_id = person.model_copy(update={"match_id": search_match_id})

    temp_table_name = f"search_req_{uuid.uuid4().hex[:8]}"

    try:
        await session.execute(
            text(
                f"CREATE TEMP TABLE {temp_table_name} (LIKE personmatch.person INCLUDING ALL) ON COMMIT DROP",  # noqa: S608
            ),
        )

        batch = PersonBatch(records=[person_with_search_id])

        await clean_and_insert(
            records=batch,
            connection_pg=session,
            target_table_name=temp_table_name,
            upsert=False,
            commit=False,
        )

        candidates_data = await candidate_search(
            primary_record_id=search_match_id,
            connection_pg=session,
            primary_table_name=temp_table_name,
            candidates_table_name="personmatch.person",
        )

        if not candidates_data:
            return []

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
        res = await session.execute(text(sql), {"mid": search_match_id})
        search_record_with_tf = res.mappings().fetchall()

        full_records = [*search_record_with_tf, *candidates_data]

        with duckdb_connected_to_postgres(pg_db_url) as duckdb_conn:
            scores = score_records_to_person_scores(
                duckdb_conn,
                primary_record_id=search_match_id,
                records_with_tf=full_records,
                table_name="all_records",
            )

        return scores

    finally:
        await session.execute(text(f"DROP TABLE IF EXISTS {temp_table_name}"))  # noqa: S608
