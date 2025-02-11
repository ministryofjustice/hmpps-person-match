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


def populate_with_tfs(con: duckdb.DuckDBPyConnection, records_table: str):
    # TODO: postcodes
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
        alias_table_name = tf_colname
        tf_lookup_table_name = f"pg_db.public.{tf_colname}"
        join_clauses.append(
            f"LEFT JOIN {tf_lookup_table_name} AS {alias_table_name} ON f.{col} = {alias_table_name}.{col}",
        )
        select_clauses.append(f"{alias_table_name}.{tf_colname} AS {tf_colname}")

    joined_views_name = "final_table_with_tf"
    sql_join = " ".join(join_clauses)
    sql_select = ", ".join(select_clauses)
    con.execute(
        f"""
        CREATE TABLE {joined_views_name} AS
        SELECT {sql_select}
        FROM {records_table} f
        {sql_join}
        """,  # noqa: S608
    )
    return joined_views_name


def score(
    con: duckdb.DuckDBPyConnection,
    primary_record_id: str,
    full_candidates_tn: str,
    return_scores_only: bool = True,
):
    start_time = time.perf_counter()
    # Compare records
    db_api = DuckDBAPI(con)
    # db_api.debug_mode = True

    # TODO:
    # join tf tables to candidates
    # split

    full_table_name = populate_with_tfs(con, full_candidates_tn)

    source_name = "primary_record"
    candidates_name = "candidate_record"
    # cannot create views with prepared statements: https://github.com/duckdb/duckdb/issues/13069
    source_sql = f"CREATE TABLE {source_name} AS SELECT * FROM {full_table_name} WHERE match_id = $primary_record_id"  # noqa: S608
    candidates_sql = (
        f"CREATE TABLE {candidates_name} AS SELECT * FROM {full_table_name} WHERE match_id != $primary_record_id"  # noqa: S608
    )
    con.execute(source_sql, parameters={"primary_record_id": primary_record_id})
    con.execute(candidates_sql, parameters={"primary_record_id": primary_record_id})

    con.sql("SHOW ALL TABLES").show()

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
        return con.sql("SELECT match_id_l, match_id_r, match_probability, match_weight FROM scores")
    else:
        return con.sql("SELECT * FROM scores")
