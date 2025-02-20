import pytest
from duckdb import DuckDBPyConnection

from hmpps_cpr_splink.cpr_splink.interface.db import duckdb_connected_to_postgres


@pytest.fixture()
def duckdb_con_with_pg(db_connection) -> DuckDBPyConnection:
    return duckdb_connected_to_postgres(db_connection)
