from collections.abc import Generator
from contextlib import contextmanager

import duckdb
from sqlalchemy import URL, text
from sqlalchemy.ext.asyncio import AsyncSession


@contextmanager
def duckdb_connected_to_postgres(pg_db_url: URL) -> Generator[duckdb.DuckDBPyConnection]:
    pg_conn_string_sync = pg_db_url.render_as_string(hide_password=False)
    with duckdb.connect(":memory:") as connection_duckdb:
        connection_duckdb.sql(f"ATTACH '{pg_conn_string_sync}' AS pg_db (TYPE POSTGRES);")
        yield connection_duckdb
        connection_duckdb.close()


async def insert_duckdb_table_into_postgres_table(
    ddb_tab: duckdb.DuckDBPyRelation,
    pg_table_name: str,
    connection_pg: AsyncSession,
):
    values = ddb_tab.fetchall()
    columns = [desc[0] for desc in ddb_tab.description]

    data = [dict(zip(columns, row, strict=True)) for row in values]

    placeholders = ", ".join([f":{col}" for col in columns])
    update_columns = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns])
    query = text(
        f"INSERT INTO {pg_table_name}({', '.join(columns)}) VALUES ({placeholders}) "  # noqa: S608
        f"ON CONFLICT (source_system_id, source_system) DO UPDATE SET {update_columns}",
    )
    await connection_pg.execute(query, data)
    await connection_pg.commit()
