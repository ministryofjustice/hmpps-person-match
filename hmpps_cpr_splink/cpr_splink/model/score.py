import logging
import time

import duckdb
from splink import DuckDBAPI
from splink.internals.realtime import compare_records

from .model import MODEL_PATH

logger = logging.getLogger(__name__)


def _mock_lookup_many_tf(value_col_pairs):
    unique_pairs = {(d["value"], d["column_name"]) for d in value_col_pairs}
    results = []
    for val, col in unique_pairs:
        if col in ["postcode", "postcode_arr"]:
            tf = 0.1
        elif col == "name_1_std":
            tf = 0.2
        elif col == "last_name_std":
            tf = 0.3
        else:
            tf = 0.5
        results.append({"value": val, "column_name": col, "rel_freq": tf})
    return results


def populate_with_tfs(con: duckdb.DuckDBPyConnection, records_table: str) -> str:
    tf_columns = [
        "name_1_std",
        "name_2_std",
        "last_name_std",
        "first_and_last_name_std",
        "date_of_birth",
        "cro_single",
        "pnc_single",
    ]
    join_clauses = []
    select_clauses = ["f.*"]
    for col in tf_columns:
        tf_colname = f"tf_{col}"
        tf_table_name = f"term_frequencies_{col}"
        alias_table_name = tf_colname
        tf_lookup_table_name = f"pg_db.{tf_table_name}"
        join_clauses.append(
            f"LEFT JOIN {tf_lookup_table_name} AS {alias_table_name} ON f.{col} = {alias_table_name}.{col}",
        )
        select_clauses.append(f"{alias_table_name}.{tf_colname} AS {tf_colname}")

    # postcodes are in arrays so logic is more complex to join
    with_clause = f"""
    WITH exploded_postcodes AS (
        SELECT
            match_id,
            UNNEST(postcode_arr) AS postcode
        FROM
            {records_table}
    ),
    exploded_postcodes_with_term_frequencies AS (
        SELECT
            exploded_postcodes.match_id AS match_id,
            exploded_postcodes.postcode AS value,
            COALESCE(pc_tf.tf_postcode, 1) AS rel_freq
        FROM
            exploded_postcodes
        LEFT JOIN pg_db.personmatch.term_frequencies_postcode AS pc_tf
        ON exploded_postcodes.postcode = pc_tf.postcode
    ),
    postcodes_repacked_with_term_frequencies AS (
        SELECT
            match_id,
            array_agg(
                struct_pack(value := value, rel_freq := rel_freq)
            ) AS postcode_arr_with_freq,
        FROM
            exploded_postcodes_with_term_frequencies
        GROUP BY
            match_id
    )
    """  # noqa: S608
    alias_table_name = "tf_postcode"
    select_clauses.append(
        f"{alias_table_name}.postcode_arr_with_freq AS postcode_arr_with_freq",
    )
    join_clauses.append(
        f"LEFT JOIN postcodes_repacked_with_term_frequencies AS {alias_table_name} "
        f"ON f.match_id = {alias_table_name}.match_id",
    )

    joined_views_name = "final_table_with_tf"
    sql_join = " ".join(join_clauses)
    sql_select = ", ".join(select_clauses)
    con.execute(
        f"""
        CREATE TABLE {joined_views_name} AS
        {with_clause}
        SELECT {sql_select}
        FROM {records_table} AS f
        {sql_join}
        """,  # noqa: S608
    )
    return joined_views_name


def score(
    connection_duckdb: duckdb.DuckDBPyConnection,
    primary_record_id: str,
    full_candidates_tn: str,
    return_scores_only: bool = True,
):
    start_time = time.perf_counter()
    # Compare records
    db_api = DuckDBAPI(connection_duckdb)

    full_table_name = populate_with_tfs(connection_duckdb, full_candidates_tn)

    source_name = "primary_record"
    candidates_name = "candidate_record"
    # cannot create views with prepared statements: https://github.com/duckdb/duckdb/issues/13069
    source_sql = f"CREATE TABLE {source_name} AS SELECT * FROM {full_table_name} WHERE match_id = $primary_record_id"  # noqa: S608
    candidates_sql = (
        f"CREATE TABLE {candidates_name} AS SELECT * FROM {full_table_name} WHERE match_id != $primary_record_id"  # noqa: S608
    )
    connection_duckdb.execute(source_sql, parameters={"primary_record_id": primary_record_id})
    connection_duckdb.execute(candidates_sql, parameters={"primary_record_id": primary_record_id})

    scores = compare_records(  # noqa: F841
        source_name,
        candidates_name,
        settings=MODEL_PATH,
        db_api=db_api,
        use_sql_from_cache=True,
    ).as_duckdbpyrelation()

    end_time = time.perf_counter()
    logger.info("Time taken: %.2f seconds", end_time - start_time)

    if return_scores_only:
        return connection_duckdb.sql("SELECT match_id_l, match_id_r, match_probability, match_weight FROM scores")
    else:
        return connection_duckdb.sql("SELECT * FROM scores")
