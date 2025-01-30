import os

from duckdb import DuckDBPyRelation
from psycopg import Connection, connect
from sqlalchemy import create_engine, text

host = os.environ.get("CPR_PG_HOST", "localhost")
port = os.environ.get("CPR_PG_PORT", "5432")
user = os.environ.get("CPR_PG_USER", "splinkognito")
password = os.environ.get("CPR_PG_PASSWORD", "splink123!")
database = os.environ.get("CPR_PG_DATABASE", "splink_db")
# TODO: where?

pg_conn_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"


def postgres_db_connector() -> Connection:
    return connect(pg_conn_string)

postgres_engine = create_engine(pg_conn_string)

def insert_duckdb_table_into_postgres_table(ddb_tab: DuckDBPyRelation, pg_table_name: str):

    values = ddb_tab.fetchall()
    columns = [desc[0] for desc in ddb_tab.description]
    # assuming a single row to insert for now
    data = dict(zip(columns, values[0], strict=True))

    with postgres_engine.connect() as conn:
        placeholders = ", ".join([f":{col}" for col in columns])
        query = text(f"INSERT INTO person({', '.join(columns)}) VALUES ({placeholders})")

        conn.execute(query, data)
        conn.commit()
