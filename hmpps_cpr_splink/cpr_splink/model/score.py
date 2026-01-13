import logging
import time

import duckdb
from splink import DuckDBAPI
from splink.internals.pipeline import CTEPipeline
from splink.internals.realtime import compare_records

from .model import (
    MODEL_PATH,
    POSSIBLE_TWINS_ASSIGNED_MATCH_PROBABILITY,
    POSSIBLE_TWINS_ASSIGNED_MATCH_WEIGHT,
    POSSIBLE_TWINS_SIMILARITY_FLAG_THRESHOLD,
)

logger = logging.getLogger(__name__)

# Higher threshold = more names look different = more 'twins' flagged
# Means even slightly distinct names 'look' totally different
_JARO_WINKLER_THRESHOLD = 0.9


def _no_override_match() -> str:
    """No matching override marker between records."""
    return "ifnull(override_marker_l, 'override_l') <> ifnull(override_marker_r, 'override_r')"


def _no_master_defendant_id_match() -> str:
    """No matching master_defendant_id between records."""
    return "ifnull(master_defendant_id_l, 'm_d_id_l') <> ifnull(master_defendant_id_r, 'm_d_id_r')"


def _first_names_differ() -> str:
    """First names mismatch and no cross-match on first two names."""
    return """
        coalesce(name_1_std_l <> name_1_std_r, FALSE)
        AND coalesce(name_1_std_l <> name_2_std_r, TRUE)
        AND coalesce(name_2_std_l <> name_1_std_r, TRUE)
    """


def _all_aliases_satisfy_condition(boolean_condition: str) -> str:
    """Every pair of aliases satisfies some given boolean condition)."""
    return f"""
        list_aggregate(
            list_transform(
                list_transform(
                    forename_std_arr_l,
                    alias_l -> list_transform(
                        forename_std_arr_r,
                        alias_r -> {boolean_condition}
                    )
                ),
                is_dissimilar_arr -> list_aggregate(is_dissimilar_arr, 'bool_and')
            ),
            'bool_and'
        )
    """


def _all_aliases_dissimilar_exact() -> str:
    """Every pair of aliases is completely dissimilar (exact mismatch)."""
    return _all_aliases_satisfy_condition("alias_l <> alias_r")


def _all_aliases_dissimilar_fuzzy(jw_threshold: float) -> str:
    """Every pair of aliases is dissimilar (using Jaro-Winkler similarity)."""
    return _all_aliases_satisfy_condition(f"jaro_winkler_similarity(alias_l, alias_r) < {jw_threshold}")


def _one_or_more_explicit_id_mismatch() -> str:
    """Explicit mismatch on one of CRO and PNC IDs (both must be non-null and different),
    and lack of match on the other ID (allowing dual nulls).
    """
    return """
        (
            coalesce(cro_single_l <> cro_single_r, FALSE)
            AND
            ifnull(pnc_single_l, 'pnc_l') <> ifnull(pnc_single_r, 'pnc_r')
        )
        OR (
            coalesce(pnc_single_l <> pnc_single_r, FALSE)
            AND
            ifnull(cro_single_l, 'cro_l') <> ifnull(cro_single_r, 'cro_r')
        )
    """


def _no_id_match() -> str:
    """No matching ID of either type (but one or other could be null)."""
    return """
        ifnull(cro_single_l, 'cro_l') <> ifnull(cro_single_r, 'cro_r')
        AND ifnull(pnc_single_l, 'pnc_l') <> ifnull(pnc_single_r, 'pnc_r')
    """


def _matching_dob_in_arrays() -> str:
    """Matching date of birth in the alias arrays."""
    return """
        coalesce(array_length(array_intersect(date_of_birth_arr_l, date_of_birth_arr_r)) > 0, FALSE)
    """


def _surname_match() -> str:
    """Surname must match."""
    return """
        coalesce(last_name_std_l = last_name_std_r, FALSE)
    """


def _shared_sentence_date_or_postcode() -> str:
    """Either sentenced on the same date, or have a postcode in common."""
    return """
        coalesce(array_length(array_intersect(sentence_date_arr_r, sentence_date_arr_l)) > 0, FALSE)
        OR coalesce(array_length(array_intersect(postcode_arr_r, postcode_arr_l)) > 0, FALSE)
    """


def _twins_condition() -> str:
    """
    Build the full twins detection condition.

    A pair of records is flagged as possible twins when ALL of the following are true:
    - No matching override marker
    - No matching master_defendant_id
    - First names differ (including cross-matches on first two names)
    - All aliases are dissimilar (with explicit ID mismatch we disbar exact matches only,
        if at least one of CRO or PNC doesn't explicitly mismatch then we tolerate fuzzy alias match)
    - Matching date of birth in arrays
    - Same surname
    - Share either a sentence date or postcode
    - Match weight exceeds the similarity threshold
    """
    # Two paths for alias dissimilarity:
    # 1. Explicit ID mismatch on both CRO and PNC means we flag any records that have no exact alias matches
    # 2. No matching IDs (allowing nulls) means we only flag records that have no fuzzy alias matches
    # this is done so that we can avoid flagging twins with similar names if we ensure that source
    # data includes explicitly mismatched IDs
    alias_and_id_condition = f"""
        (
            ({_all_aliases_dissimilar_exact()})
            AND ({_one_or_more_explicit_id_mismatch()})
        )
        OR
        (
            ({_all_aliases_dissimilar_fuzzy(_JARO_WINKLER_THRESHOLD)})
            AND ({_no_id_match()})
        )
    """

    return f"""
        ({_no_override_match()})
        AND ({_no_master_defendant_id_match()})
        AND ({_first_names_differ()})
        AND ({alias_and_id_condition})
        AND ({_surname_match()})
        AND ({_matching_dob_in_arrays()})
        AND ({_shared_sentence_date_or_postcode()})
        AND match_weight > {POSSIBLE_TWINS_SIMILARITY_FLAG_THRESHOLD}
    """


def filter_twins_sql(table_name: str) -> str:
    """Generate SQL to identify and flag possible twin records."""
    pipeline = CTEPipeline()

    sql_filter_twins = {
        "sql": f"""
            SELECT
                * RENAME (match_weight AS unaltered_match_weight, match_probability AS unaltered_match_probability),
                ({_twins_condition()}) AS possible_twins,
                CASE
                    WHEN possible_twins THEN {POSSIBLE_TWINS_ASSIGNED_MATCH_WEIGHT}
                    ELSE unaltered_match_weight
                END AS match_weight,
                CASE
                    WHEN possible_twins THEN {POSSIBLE_TWINS_ASSIGNED_MATCH_PROBABILITY}
                    ELSE unaltered_match_probability
                END AS match_probability,
            FROM
                {table_name}
        """,  # noqa: S608
        "output_table_name": "scored_with_twins_flag",
    }
    pipeline.enqueue_list_of_sqls([sql_filter_twins])

    return pipeline.generate_cte_pipeline_sql()


def enhance_scores_with_twins(
    connection_duckdb: duckdb.DuckDBPyConnection,
    scores_table_name: str,
) -> str:
    """Enhance an existing scores table with possible twins flag and adjusted match weight."""
    enhanced_table_name = f"{scores_table_name}_with_twins"
    sql = f"""
        CREATE TABLE {enhanced_table_name} AS
        {filter_twins_sql(scores_table_name)}
    """
    connection_duckdb.execute(sql)
    return enhanced_table_name


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

    scores_with_twins_table = enhance_scores_with_twins(connection_duckdb, scores_df.physical_name)

    end_time = time.perf_counter()
    logger.info("Time taken: %.2f seconds", end_time - start_time)

    if return_scores_only:
        return connection_duckdb.sql(
            f"SELECT match_id_l, match_id_r, "  # noqa: S608
            f"match_probability, match_weight, possible_twins, unaltered_match_weight "
            f"FROM {scores_with_twins_table}",
        )
    else:
        return connection_duckdb.sql(f"SELECT * FROM {scores_with_twins_table}")  #noqa: S608
