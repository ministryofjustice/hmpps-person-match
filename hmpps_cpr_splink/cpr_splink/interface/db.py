import os

from duckdb import DuckDBPyRelation
from psycopg import Connection as ConnectionPsycopg
from psycopg import connect
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

host = os.environ.get("CPR_PG_HOST", "localhost")
port = os.environ.get("CPR_PG_PORT", "5432")
user = os.environ.get("CPR_PG_USER", "splinkognito")
password = os.environ.get("CPR_PG_PASSWORD", "splink123!")
database = os.environ.get("CPR_PG_DATABASE", "splink_db")
# TODO: where?

pg_conn_string = f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}"


def postgres_db_connector() -> ConnectionPsycopg:
    return connect(pg_conn_string)


async def insert_duckdb_table_into_postgres_table(ddb_tab: DuckDBPyRelation, pg_table_name: str, conn: AsyncConnection):
    values = ddb_tab.fetchall()
    columns = [desc[0] for desc in ddb_tab.description]
    # assuming a single row to insert for now
    data = dict(zip(columns, values[0], strict=True))

    placeholders = ", ".join([f":{col}" for col in columns])
    query = text(f"INSERT INTO personmatch.person({', '.join(columns)}) VALUES ({placeholders})")

    await conn.execute(query, data)
    await conn.commit()
