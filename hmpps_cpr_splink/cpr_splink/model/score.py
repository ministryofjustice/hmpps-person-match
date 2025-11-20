import logging
import time

import duckdb
from splink import DuckDBAPI
from splink.internals.pipeline import CTEPipeline
from splink.internals.realtime import compare_records

from .model import (
    MODEL_PATH,
    POSSIBLE_TWINS_ASSIGNED_MATCH_WEIGHT,
    POSSIBLE_TWINS_SIMILARITY_FLAG_THRESHOLD,
)

logger = logging.getLogger(__name__)


def filter_twins_sql(table_name: str) -> str:
    pipeline = CTEPipeline()
    # higher threshold = more names look different = more 'twins' flagged
    # means even slightly distinct names 'look' totally different
    jw_thresh = 0.9

    # a long boolean condition (combination with AND) - if these are all TRUE,
    # then we consider the pair of records to possibly belong to twins
    twins_condition = f"""
        -- first name not a match
            name_1_std_l <> name_1_std_r
        -- every pair of aliases is dissimilar
        AND
            list_aggregate(
                list_transform(
                    list_transform(
                        forename_std_arr_l,
                        alias_l -> list_transform(
                            forename_std_arr_r,
                            alias_r -> jaro_winkler_similarity(alias_l, alias_r) < {jw_thresh}
                        )
                    ),
                    is_dissimilar_arr -> list_aggregate(
                        is_dissimilar_arr,
                        'bool_and'
                    )
                ),
                'bool_and'
            )
        -- dob matchess
        AND
            date_of_birth_l = date_of_birth_r
        -- surname matches
        AND
            last_name_std_l = last_name_std_r
        -- either sentenced on same date, or postcode in common
        AND (
            array_length(array_intersect(sentence_date_arr_r, sentence_date_arr_l)) > 0
            OR
            array_length(array_intersect(postcode_arr_r, postcode_arr_l)) > 0
        )
        -- no matching ID of either type (but could be null)
        -- this will flag more records then explicit mismatches
        AND
            ifnull(cro_single_l, 'cro_l') <> ifnull(cro_single_r, 'cro_r')
        AND
            ifnull(pnc_single_l, 'pnc_l') <> ifnull(pnc_single_r, 'pnc_r')
        -- look sufficiently similar
        -- needn't flag if they would not be linked anyhow
        AND
            match_weight > {POSSIBLE_TWINS_SIMILARITY_FLAG_THRESHOLD}
    """
    # TODO: no override marker of defendant id matches!

    sql_filter_dob = {
        "sql": f"""
            SELECT
                * RENAME (match_weight AS unaltered_match_weight),
                (
                    {twins_condition}
                ) AS possible_twins,
                CASE
                    WHEN possible_twins THEN {POSSIBLE_TWINS_ASSIGNED_MATCH_WEIGHT}
                    ELSE unaltered_match_weight
                END AS match_weight
            FROM
                {table_name}
        """,  # noqa: S608
        "output_table_name": "scored_same_birthdate_no_id_match",
    }
    pipeline.enqueue_list_of_sqls([sql_filter_dob])

    return pipeline.generate_cte_pipeline_sql()


def score(
    connection_duckdb: duckdb.DuckDBPyConnection,
    primary_record_id: str,
    full_candidates_tn: str,
    return_scores_only: bool = True,
) -> duckdb.DuckDBPyRelation:
    start_time = time.perf_counter()
    # Compare records
    db_api = DuckDBAPI(connection_duckdb)

    # Splink has a limitation around caching SQL - this choice of names is a workaround until we update
    # need this so that we can keep cached SQL
    source_name = "records_l_with_postcode_tfs"
    candidates_name = "records_r_with_postcode_tfs"
    # cannot create views with prepared statements: https://github.com/duckdb/duckdb/issues/13069
    source_sql = f"CREATE TABLE {source_name} AS SELECT * FROM {full_candidates_tn} WHERE match_id = $primary_record_id"  # noqa: S608
    candidates_sql = (
        f"CREATE TABLE {candidates_name} AS SELECT * FROM {full_candidates_tn} WHERE match_id != $primary_record_id"  # noqa: S608
    )
    connection_duckdb.execute(source_sql, parameters={"primary_record_id": primary_record_id})
    connection_duckdb.execute(candidates_sql, parameters={"primary_record_id": primary_record_id})

    scores_df = compare_records(
        source_name,
        candidates_name,
        settings=MODEL_PATH,
        db_api=db_api,
        sql_cache_key="score_records_sql",
    )

    sql = f"CREATE TABLE scores_with_twins AS {filter_twins_sql(scores_df.physical_name)}"
    connection_duckdb.execute(sql)

    end_time = time.perf_counter()
    logger.info("Time taken: %.2f seconds", end_time - start_time)

    connection_duckdb.table("scores_with_twins").to_csv("twins.csv")
    if return_scores_only:
        return connection_duckdb.sql(
            "SELECT match_id_l, match_id_r, match_probability, match_weight, possible_twins, unaltered_match_weight "
            "FROM scores_with_twins",
        )
    else:
        return connection_duckdb.sql("SELECT * FROM scores_with_twins")
