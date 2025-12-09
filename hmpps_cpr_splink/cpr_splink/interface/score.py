from collections.abc import Sequence
from typing import Any, cast

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
from hmpps_person_match.models.person.person_score import PersonScore


def insert_data_into_duckdb(
    connection_duckdb: duckdb.DuckDBPyConnection,
    data_to_insert: Sequence,
    base_table_name: str,
) -> str:
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
) -> list[PersonScore]:
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
            PersonScore(
                candidate_match_id=row["match_id_r"],  # match_id_l is primary record
                candidate_match_probability=row["match_probability"],
                candidate_match_weight=row["match_weight"],
                candidate_should_join=row["match_weight"] >= JOINING_MATCH_WEIGHT_THRESHOLD,
                candidate_should_fracture=row["match_weight"] < FRACTURE_MATCH_WEIGHT_THRESHOLD,
                candidate_is_possible_twin=row["possible_twins"],
                unadjusted_match_weight=row["unaltered_match_weight"],
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
    return cast(bool, result.scalar())


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


def get_mutually_excluded_records(
    connection_duckdb: duckdb.DuckDBPyConnection, duckdb_table_name: str,
) -> list[tuple[Any, ...]]:
    # check if we have any rows in our data that share a scope, but have distinct markers
    # if we have any such rows, our cluster is invalid
    pipeline = CTEPipeline()
    sql = f"""
    SELECT
        override_marker,
        UNNEST(override_scopes) AS override_scope,
    FROM
        {duckdb_table_name}
    """  # noqa: S608
    pipeline.enqueue_sql(sql=sql, output_table_name="exploded_scopes")
    sql = """
    SELECT
        l.override_marker AS override_marker_l,
        r.override_marker AS override_marker_r,
    FROM
        exploded_scopes l
    JOIN
        exploded_scopes r
    ON
        l.override_scope = r.override_scope
    WHERE
        override_marker_l <> override_marker_r
    """
    pipeline.enqueue_sql(sql=sql, output_table_name="excluded_overrides")

    sql = pipeline.generate_cte_pipeline_sql()
    return connection_duckdb.execute(sql).fetchall()


async def get_clusters(match_ids: list[str], pg_db_url: URL, connection_pg: AsyncSession) -> Clusters:
    with duckdb_connected_to_postgres(pg_db_url) as connection_duckdb:
        tablename = "records_to_check"

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

        excluded_overrides = get_mutually_excluded_records(connection_duckdb, tf_enhanced_table_name)
        if excluded_overrides:
            # if we have excluded overrides amongst our records,
            # we can't determine in general what the composition 'should be'
            # so don't attempt to guess
            return Clusters([])

        db_api = DuckDBAPI(connection_duckdb)
        scores = compare_records(
            tf_enhanced_table_name,
            tf_enhanced_table_name,
            settings=MODEL_PATH,
            db_api=db_api,
            sql_cache_key="get_clusters_compare_sql",
            join_condition="l.id < r.id",
        )

        df_clusters = cluster_pairwise_predictions_at_threshold(
            nodes=tablename,
            edges=scores.physical_name,
            db_api=db_api,
            node_id_column_name="match_id",
            threshold_match_weight=IS_CLUSTER_VALID_MATCH_WEIGHT_THRESHOLD,
        )
        clusters = connection_duckdb.execute(
            f"SELECT match_id, cluster_id FROM {df_clusters.physical_name} "  # noqa: S608
            "GROUP BY match_id, cluster_id",
        ).fetchall()
        cluster_assignments: dict[str, list[str]] = {}
        for match_id, cluster_id in clusters:
            if cluster_id not in cluster_assignments:
                cluster_assignments[cluster_id] = []
            cluster_assignments[cluster_id].append(match_id)

        return Clusters(list(cluster_assignments.values()))
