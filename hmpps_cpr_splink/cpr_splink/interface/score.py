from typing import TypedDict

import duckdb
from splink import DuckDBAPI
from splink.internals.clustering import cluster_pairwise_predictions_at_threshold
from splink.internals.pipeline import CTEPipeline
from splink.internals.realtime import compare_records
from sqlalchemy import URL, text
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_cpr_splink.cpr_splink.interface.block import candidate_search, enqueue_join_term_frequency_tables
from hmpps_cpr_splink.cpr_splink.interface.clusters import Clusters
from hmpps_cpr_splink.cpr_splink.interface.db import duckdb_connected_to_postgres
from hmpps_cpr_splink.cpr_splink.model.model import (
    FRACTURE_MATCH_WEIGHT_THRESHOLD,
    IS_CLUSTER_VALID_MATCH_WEIGHT_THRESHOLD,
    JOINING_MATCH_WEIGHT_THRESHOLD,
    MODEL_PATH,
)
from hmpps_cpr_splink.cpr_splink.model.score import score
from hmpps_cpr_splink.cpr_splink.model_cleaning import CLEANED_TABLE_SCHEMA
from hmpps_cpr_splink.cpr_splink.utils import create_table_from_records


class ScoredCandidate(TypedDict):
    candidate_match_id: str
    candidate_match_probability: float
    candidate_match_weight: float
    candidate_should_join: bool
    candidate_should_fracture: bool


def insert_data_into_duckdb(connection_duckdb: duckdb.DuckDBPyConnection, data_to_insert: list, base_table_name: str):
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
        connection_duckdb,
        data_to_insert,
        base_table_name,
        CLEANED_TABLE_SCHEMA + tf_schema,
    )
    table_with_postcode_tf_tablename = f"{base_table_name}_with_postcode_tfs"
    sql = f"""
        CREATE TABLE {table_with_postcode_tf_tablename} AS
        SELECT *,
        list_transform(
            list_zip(postcode_arr_repacked, postcode_freq_arr),
            pc -> struct_pack(value := pc[1], rel_freq := pc[2])
        ) AS postcode_arr_with_freq
        FROM {base_table_name}
    """  # noqa: S608
    connection_duckdb.execute(sql)
    return table_with_postcode_tf_tablename


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

        candidates_with_postcode_tf = insert_data_into_duckdb(connection_duckdb, candidates_data, "candidates")

        res = score(connection_duckdb, primary_record_id, candidates_with_postcode_tf, return_scores_only=True)

        data = [dict(zip(res.columns, row, strict=True)) for row in res.fetchall()]
        return [
            ScoredCandidate(
                candidate_match_id=row["match_id_r"],  # match_id_l is primary record
                candidate_match_probability=row["match_probability"],
                candidate_match_weight=row["match_weight"],
                candidate_should_join=row["match_weight"] >= JOINING_MATCH_WEIGHT_THRESHOLD,
                candidate_should_fracture=row["match_weight"] < FRACTURE_MATCH_WEIGHT_THRESHOLD,
            )
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


async def get_missing_record_ids(match_ids: list[str], connection_pg: AsyncSession) -> list[str]:
    """
    Given a list of match_ids, return a list of any that do not have records in the person table
    """
    result = await connection_pg.execute(
        text(
            "SELECT match_id_list.id AS missing_match_id FROM "
            "unnest(CAST(:match_ids AS VARCHAR[])) AS match_id_list(id) "
            "LEFT JOIN personmatch.person p "
            "ON match_id_list.id = p.match_id "
            "WHERE p.match_id IS NULL;",
        ),
        {"match_ids": tuple(match_ids)},
    )
    return [r[0] for r in result.fetchall()]


async def get_clusters(match_ids: list[str], pg_db_url: URL, connection_pg: AsyncSession) -> Clusters:
    with duckdb_connected_to_postgres(pg_db_url) as connection_duckdb:
        records_l_tablename = "records_l"
        records_r_tablename = "records_r"

        tf_enhanced_table_names = []
        for tablename in (records_l_tablename, records_r_tablename):
            pipeline = CTEPipeline()
            pipeline.enqueue_sql(
                sql="SELECT * FROM personmatch.person WHERE match_id = ANY(:match_ids)",
                output_table_name="people_to_check_cluster_status",
            )
            enqueue_join_term_frequency_tables(
                pipeline,
                table_to_join_to=pipeline.output_table_name,
                output_table_name=tablename,
            )

            sql = pipeline.generate_cte_pipeline_sql()
            res = await connection_pg.execute(text(sql), {"match_ids": match_ids})

            tf_enhanced_table_name = insert_data_into_duckdb(connection_duckdb, res.mappings().fetchall(), tablename)
            tf_enhanced_table_names.append(tf_enhanced_table_name)

        db_api = DuckDBAPI(connection_duckdb)
        scores = compare_records(  # noqa: F841
            *tf_enhanced_table_names,
            settings=MODEL_PATH,
            db_api=db_api,
            use_sql_from_cache=True,
        )

        df_clusters = cluster_pairwise_predictions_at_threshold(
            nodes=records_l_tablename,
            edges=scores.physical_name,
            db_api=db_api,
            node_id_column_name="match_id",
            threshold_match_weight=IS_CLUSTER_VALID_MATCH_WEIGHT_THRESHOLD,
        )
        clusters = connection_duckdb.execute(
            f"SELECT match_id, cluster_id FROM {df_clusters.physical_name} "  # noqa: S608
            "GROUP BY match_id, cluster_id",
        ).fetchall()
        cluster_assignments = {}
        for match_id, cluster_id in clusters:
            if cluster_id not in cluster_assignments:
                cluster_assignments[cluster_id] = []
            cluster_assignments[cluster_id].append(match_id)

    return Clusters(list(cluster_assignments.values()))
