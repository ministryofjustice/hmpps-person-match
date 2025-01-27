import os

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
