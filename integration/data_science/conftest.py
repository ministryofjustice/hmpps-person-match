import pytest
from duckdb import DuckDBPyConnection

from hmpps_cpr_splink.cpr_splink.interface.db import duckdb_connected_to_postgres


@pytest.fixture()
def duckdb_con_with_pg(pg_db_url) -> DuckDBPyConnection:
    return duckdb_connected_to_postgres(pg_db_url)
