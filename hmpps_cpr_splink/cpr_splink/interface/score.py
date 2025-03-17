import time
from logging import Logger
from typing import TypedDict

from sqlalchemy import URL, text
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_cpr_splink.cpr_splink.interface.block import candidate_search
from hmpps_cpr_splink.cpr_splink.interface.db import duckdb_connected_to_postgres
from hmpps_cpr_splink.cpr_splink.model.score import score
from hmpps_cpr_splink.cpr_splink.model_cleaning import CLEANED_TABLE_SCHEMA
from hmpps_cpr_splink.cpr_splink.utils import create_table_from_records


class ScoredCandidate(TypedDict):
    candidate_match_id: str
    candidate_match_probability: float
    candidate_match_weight: float


async def get_scored_candidates(
    primary_record_id: str,
    pg_db_url: URL,
    connection_pg: AsyncSession,
    logger: Logger,
) -> list[ScoredCandidate]:
    """
    Takes a primary record, generates candidates, scores
    """
    # TODO: allow a threshold cutoff? (depending on blocking rules)
    with duckdb_connected_to_postgres(pg_db_url) as connection_duckdb:
        pg_start_time = time.perf_counter()
        candidates_data = await candidate_search(primary_record_id, connection_pg)
        pg_end_time = time.perf_counter()
        logger.info("CandidateSearchPostgresQuery", extra=dict(
            matchId=primary_record_id,
            query_time=pg_end_time - pg_start_time,
        ))

        dk_db_start_time = time.perf_counter()
        if not candidates_data:
            return []
        candidates_table_name = "candidates"

        create_table_from_records(connection_duckdb, candidates_data, candidates_table_name, CLEANED_TABLE_SCHEMA)

        res = score(connection_duckdb, primary_record_id, candidates_table_name, return_scores_only=True)

        dk_db_end_time = time.perf_counter()
        logger.info("ScoreDuckDbQuery", extra=dict(
            matchId=primary_record_id,
            query_time=dk_db_end_time - dk_db_start_time,
        ))

        data = [dict(zip(res.columns, row, strict=True)) for row in res.fetchall()]
        return [
            {
                "candidate_match_id": row["match_id_r"],  # match_id_l is primary record
                "candidate_match_probability": row["match_probability"],
                "candidate_match_weight": row["match_weight"],
            }
            for row in data
        ]


async def match_record_exists(match_id: str, connection_pg: AsyncSession) -> bool:
    """
    Check if a record exists in the person table
    """
    result = await connection_pg.execute(
        text("SELECT EXISTS (SELECT 1 FROM personmatch.person WHERE match_id = :match_id)"),
        {"match_id": match_id},
    )
    return result.scalar()
