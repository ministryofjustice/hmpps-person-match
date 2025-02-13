from typing import TypedDict

from sqlalchemy.ext.asyncio import AsyncConnection

from ..model.score import score
from .block import candidate_search
from .db import duckdb_connected_to_postgres


class ScoredCandidate(TypedDict):
    candidate_match_id: str
    candidate_match_probability: float
    candidate_match_weight: float


async def get_scored_candidates(primary_record_id: str, connection_pg: AsyncConnection) -> list[ScoredCandidate]:
    """
    Takes a primary record, generates candidates, scores
    """
    # TODO: allow a threshold cutoff? (depending on blocking rules)
    connection_duckdb = duckdb_connected_to_postgres(connection_pg)

    candidates_table_name = await candidate_search(primary_record_id, connection_duckdb)

    res = score(connection_duckdb, primary_record_id, candidates_table_name, return_scores_only=True)
    return [
        {
            "candidate_match_id": row[1],
            "candidate_match_probability": row[2],
            "candidate_match_weight": row[3],
        }
        for row in res.fetchall()
    ]
