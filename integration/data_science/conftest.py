import pytest
from duckdb import DuckDBPyConnection
from sqlalchemy import URL

from hmpps_cpr_splink.cpr_splink.interface.db import duckdb_connected_to_postgres


@pytest.fixture()
def duckdb_con_with_pg(pg_db_url: URL) -> DuckDBPyConnection:
    return duckdb_connected_to_postgres(pg_db_url)
