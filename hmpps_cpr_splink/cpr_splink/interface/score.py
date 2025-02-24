from typing import TypedDict

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from ..model.score import score
from .block import candidate_search
from .db import duckdb_connected_to_postgres


class ScoredCandidate(TypedDict):
    candidate_match_id: str
    candidate_match_probability: float
    candidate_match_weight: float


async def get_scored_candidates(primary_record_id: str, connection_pg: AsyncSession) -> list[ScoredCandidate]:
    """
    Takes a primary record, generates candidates, scores
    """
    # TODO: allow a threshold cutoff? (depending on blocking rules)
    connection_duckdb = duckdb_connected_to_postgres(connection_pg)

    candidates_table_name = await candidate_search(primary_record_id, connection_duckdb)

    res = score(connection_duckdb, primary_record_id, candidates_table_name, return_scores_only=True)

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
