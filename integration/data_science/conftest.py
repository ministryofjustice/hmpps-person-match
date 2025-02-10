from collections.abc import AsyncGenerator

import pytest
from duckdb import DuckDBPyConnection
from sqlalchemy import URL
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from hmpps_cpr_splink.cpr_splink.interface import clean
from hmpps_cpr_splink.cpr_splink.interface.db import duckdb_connected_to_postgres
from hmpps_person_match.models.person.person import Person
from hmpps_person_match.models.person.person_batch import PersonBatch


@pytest.fixture()
async def sqlalchemy_db_connection() -> AsyncGenerator[AsyncConnection]:
    database_url = URL.create(
        drivername="postgresql+asyncpg",
        username="root",
        password="dev",  # noqa: S106
        host="localhost",
        port="5432",
        database="postgres",
    )

    engine: AsyncEngine = create_async_engine(
        database_url,
    )
    async with engine.connect() as conn:
        yield conn

    await engine.dispose()


@pytest.fixture()
def duckdb_con_with_pg(sqlalchemy_db_connection) -> DuckDBPyConnection:
    return duckdb_connected_to_postgres(sqlalchemy_db_connection)


@pytest.fixture()
async def create_person_record(sqlalchemy_db_connection):
    async def _create_person(person: Person):
        await clean.clean_and_insert(PersonBatch(records=[person]), sqlalchemy_db_connection)

    return _create_person
