import logging
import time

import duckdb
from splink import DuckDBAPI
from splink.internals.realtime import compare_records

from hmpps_cpr_splink.cpr_splink.model_cleaning.create_table_sql import create_table_from_records
from hmpps_cpr_splink.cpr_splink.model_cleaning.tables import (
    clean_and_explode_distinct_postcode_arr,
    clean_whole_joined_table,
)
from hmpps_cpr_splink.cpr_splink.schemas.joined import DUCKDB_COLUMNS_WITH_TYPES, JoinedRecord

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
            f"LEFT JOIN {tf_lookup_table_name} AS {alias_table_name}"
            f" ON f.{col} = {alias_table_name}.{col}"
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
        """
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

    source_name = "primary_record_view"
    candidates_name = "candidate_record_view"
    con.sql(
        f"CREATE VIEW {source_name} AS "
        f"SELECT * FROM {full_table_name} WHERE id = '{primary_record_id}'"
    )
    con.sql(
        f"CREATE VIEW {candidates_name} AS "
        f"SELECT * FROM {full_table_name} WHERE id != '{primary_record_id}'"
    )

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
        return con.sql("SELECT id_l, id_r, match_probability FROM scores")
    else:
        return con.sql("SELECT * FROM scores")


def _score(
    primary_record: JoinedRecord,
    candidates_to_score: list[JoinedRecord],
    return_scores_only: bool = True,
):
    start_time = time.perf_counter()
    con = duckdb.connect(":memory:")
    all_records = [primary_record, *candidates_to_score]

    # Create table for all records
    table_name = create_table_from_records(
        con, all_records, "all_records", DUCKDB_COLUMNS_WITH_TYPES
    )

    # Clean and explode postcodes
    pc_clean_sql = clean_and_explode_distinct_postcode_arr(table_name)
    pc_clean_table = con.sql(pc_clean_sql.select_statement_with_lineage)

    # Collect distinct postcodes for lookup
    postcodes_to_lookup = [
        {"value": row[0], "column_name": "postcode"}
        for row in pc_clean_table.fetchall()
    ]

    postcode_lookup_results = _mock_lookup_many_tf(postcodes_to_lookup)

    # Create postcode lookup table
    postcode_lookup_table_name = "postcode_lookup"
    create_table_from_records(
        con,
        postcode_lookup_results,
        postcode_lookup_table_name,
        [("value", "VARCHAR"), ("column_name", "VARCHAR"), ("rel_freq", "FLOAT")],
    )

    # Clean whole table with postcode lookup
    clean_table = clean_whole_joined_table(table_name, postcode_lookup_table_name)
    con.execute(clean_table.create_table_sql)

    # Create final table
    final_table_name = "final_table"
    con.execute(
        f"CREATE TABLE {final_table_name} AS (SELECT * FROM {clean_table.name})"
    )

    # Collect distinct (value, column) pairs for other TF columns
    tf_columns = [
        "name_1_std",
        "name_2_std",
        "last_name_std",
        "first_and_last_name_std",
        "date_of_birth",
        "cro_single",
        "pnc_single",
    ]

    pairs = []
    for col in tf_columns:
        rows = con.sql(
            f"SELECT DISTINCT {col} FROM {final_table_name} WHERE {col} IS NOT NULL"
        ).fetchall()
        for row in rows:
            val = row[0]
            if isinstance(val, list):
                for v in val:
                    pairs.append({"value": v, "column_name": col})
            else:
                pairs.append({"value": val, "column_name": col})

    tf_results = _mock_lookup_many_tf(pairs)

    # Create table for TF lookup results
    tf_lookup_name = "tf_lookup"
    create_table_from_records(
        con,
        tf_results,
        tf_lookup_name,
        [("value", "VARCHAR"), ("column_name", "VARCHAR"), ("rel_freq", "FLOAT")],
    )

    # Build up LEFT JOINs for TF columns
    join_clauses = []
    select_clauses = ["f.*"]
    for col in tf_columns + ["postcode_arr"]:  # Include postcodes in final table join
        alias = f"tf_{col}"
        join_clauses.append(
            f"LEFT JOIN {tf_lookup_name} AS {alias}"
            f" ON (f.{col} = {alias}.value OR (f.{col}::varchar = {alias}.value))"
            f" AND {alias}.column_name = '{col}'"
        )
        select_clauses.append(f"{alias}.rel_freq AS tf_{col}")

    joined_views_name = "final_table_with_tf"
    sql_join = " ".join(join_clauses)
    sql_select = ", ".join(select_clauses)
    con.execute(
        f"""
        CREATE TABLE {joined_views_name} AS
        SELECT {sql_select}
        FROM {final_table_name} f
        {sql_join}
        """
    )

    # Create views for source and candidate records
    source_name = "source_record"
    con.execute(
        f"""
        CREATE VIEW {source_name} AS
        SELECT *
        FROM {joined_views_name}
        WHERE id = {primary_record["id"]}
        """
    )

    candidates_name = "candidates_record"
    con.execute(
        f"""
        CREATE VIEW {candidates_name} AS
        SELECT *
        FROM {joined_views_name}
        WHERE id != {primary_record["id"]}
        """
    )

    # Compare records
    db_api = DuckDBAPI(con)
    scores = compare_records(  # noqa: F841
        source_name,
        candidates_name,
        settings="model_2024_12_06_1e08.json",
        db_api=db_api,
        use_sql_from_cache=True,
    ).as_duckdbpyrelation()

    end_time = time.perf_counter()
    logger.info("Time taken: %.2f seconds", end_time - start_time)

    if return_scores_only:
        return con.sql("SELECT id_l, id_r, match_probability FROM scores")
    else:
        return con.sql("SELECT * FROM scores")
