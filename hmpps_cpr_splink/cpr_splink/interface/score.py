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
) -> list[ScoredCandidate]:
    """
    Takes a primary record, generates candidates, scores
    """
    # TODO: allow a threshold cutoff? (depending on blocking rules)
    with duckdb_connected_to_postgres(pg_db_url) as connection_duckdb:
        candidates_data = await candidate_search(primary_record_id, connection_pg)

        if not candidates_data:
            return []
        candidates_table_name = "candidates"

        tf_columns = [
            "name_1_std",
            "name_2_std",
            "last_name_std",
            "first_and_last_name_std",
            "date_of_birth",
            "cro_single",
            "pnc_single",
        ]
        postcode_tf_schema = [("postcode_arr_repacked", "VARCHAR[]"), ("postcode_freq_arr", "FLOAT[]")]
        tf_schema = [(f"tf_{tf_col}", "FLOAT") for tf_col in tf_columns] + postcode_tf_schema
        create_table_from_records(
            connection_duckdb, candidates_data, candidates_table_name, CLEANED_TABLE_SCHEMA + tf_schema,
        )
        candidates_with_postcode_tf = "candidates_with_postcode_tfs"
        sql = f"""
            CREATE TABLE {candidates_with_postcode_tf} AS
            SELECT *, 
            list_transform(
                list_zip(postcode_arr_repacked, postcode_freq_arr),
                pc -> struct_pack(value := pc[1], rel_freq := pc[2])
            ) AS postcode_arr_with_freq
            FROM {candidates_table_name}
        """  # noqa: S608
        connection_duckdb.execute(sql)
        res = score(connection_duckdb, primary_record_id, candidates_with_postcode_tf, return_scores_only=True)


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
