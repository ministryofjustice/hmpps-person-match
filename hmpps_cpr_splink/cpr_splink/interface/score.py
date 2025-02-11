from typing import TypedDict

import duckdb

from ..model.score import score
from .block import candidate_search
from .db import duckdb_connected_to_postgres


class ScoredCandidate(TypedDict):
    match_id: str
    match_score: float


# probably returning list[ScoredCandidate] makes sense, but let's think
def get_scored_candidates(primary_record_id: str) -> duckdb.DuckDBPyRelation:
    """
    Takes a primary record, generates candidates, scores
    """
    # TODO: allow a threshold cutoff? (depending on blocking rules)
    con = duckdb_connected_to_postgres()

    df_candidates = candidate_search(primary_record_id, con)
    full_candidates_tn = f"pg_db.{df_candidates}"

    res = score(con, primary_record_id, full_candidates_tn, return_scores_only=False)
    return res
