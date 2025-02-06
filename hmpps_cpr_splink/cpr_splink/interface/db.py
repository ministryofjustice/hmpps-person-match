import os

import duckdb
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

host = os.environ.get("CPR_PG_HOST", "localhost")
port = os.environ.get("CPR_PG_PORT", "5432")
user = os.environ.get("CPR_PG_USER", "root")
password = os.environ.get("CPR_PG_PASSWORD", "dev")
database = os.environ.get("CPR_PG_DATABASE", "postgres")

def duckdb_connected_to_postgres() -> duckdb.DuckDBPyConnection:
    # TODO: a more coördinated way to get this:
    pg_conn_string_sync = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    con = duckdb.connect(":memory:")
    con.sql(f"ATTACH '{pg_conn_string_sync}' AS pg_db (TYPE POSTGRES);")
    return con


async def insert_duckdb_table_into_postgres_table(
    ddb_tab: duckdb.DuckDBPyRelation, pg_table_name: str, conn: AsyncConnection,
):
    values = ddb_tab.fetchall()
    columns = [desc[0] for desc in ddb_tab.description]

    data = [dict(zip(columns, row, strict=True)) for row in values]

    placeholders = ", ".join([f":{col}" for col in columns])
    update_columns = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns])
    query = text(
        f"INSERT INTO {pg_table_name}({', '.join(columns)}) VALUES ({placeholders}) "  # noqa: S608
        f"ON CONFLICT (match_id) DO UPDATE SET {update_columns}",
    )
    await conn.execute(query, data)

    await conn.commit()
