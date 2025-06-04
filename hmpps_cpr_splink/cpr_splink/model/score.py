import logging
import time

import duckdb
from splink import DuckDBAPI
from splink.internals.realtime import compare_records

from .model import MODEL_PATH

logger = logging.getLogger(__name__)


def score(
    connection_duckdb: duckdb.DuckDBPyConnection,
    primary_record_id: str,
    full_candidates_tn: str,
    return_scores_only: bool = True,
):
    start_time = time.perf_counter()
    # Compare records
    db_api = DuckDBAPI(connection_duckdb)

    full_table_name = full_candidates_tn

    # Splink has a limitation around caching SQL - this choice of names is a workaround until we update
    # need this so that we can keep cached SQL
    source_name = "records_l_with_postcode_tfs"
    candidates_name = "records_r_with_postcode_tfs"
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
