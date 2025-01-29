import os

import adbc_driver_postgresql.dbapi
from duckdb import DuckDBPyRelation
from psycopg import Connection, connect

host = os.environ.get("CPR_PG_HOST", "localhost")
port = os.environ.get("CPR_PG_PORT", "5432")
user = os.environ.get("CPR_PG_USER", "splinkognito")
password = os.environ.get("CPR_PG_PASSWORD", "splink123!")
database = os.environ.get("CPR_PG_DATABASE", "splink_db")
# TODO: where?

pg_conn_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"


def postgres_db_connector() -> Connection:
    return connect(pg_conn_string)

def postgres_arrow_connector() -> Connection:
    return adbc_driver_postgresql.dbapi.connect(pg_conn_string)

def insert_duckdb_table_into_postgres_table(ddb_tab: DuckDBPyRelation, pg_table_name: str):
    rec_arrow = ddb_tab.arrow()
    arrow_conn = postgres_arrow_connector()
    with arrow_conn.cursor() as cur:
        cur.adbc_ingest(pg_table_name, rec_arrow, mode="append")

    arrow_conn.commit()
