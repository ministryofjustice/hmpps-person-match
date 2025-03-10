from typing import TypedDict

import pandas as pd
from sqlalchemy import URL, text
from sqlalchemy.ext.asyncio import AsyncSession

from ..model.score import score
from .block import candidate_search
from .db import duckdb_connected_to_postgres


class ScoredCandidate(TypedDict):
    candidate_match_id: str
    candidate_match_probability: float
    candidate_match_weight: float


async def get_scored_candidates(
        primary_record_id: str,
        pg_db_url: URL,
        connection_pg: AsyncSession,
    ) -> list[ScoredCandidate]:
    """
    Takes a primary record, generates candidates, scores
    """
    # TODO: allow a threshold cutoff? (depending on blocking rules)
    connection_duckdb = duckdb_connected_to_postgres(pg_db_url)

    candidates_data = await candidate_search(primary_record_id, connection_pg)
    if not candidates_data:
        return []
    candidates_table_name = "candidates"

    # duckdb only recognises data in certain formats. For now use pandas as go-between.
    df = pd.DataFrame(candidates_data)
    connection_duckdb.register(candidates_table_name, df)

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
