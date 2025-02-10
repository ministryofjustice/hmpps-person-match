from collections.abc import AsyncGenerator

import pytest
from sqlalchemy import URL
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from hmpps_cpr_splink.cpr_splink.interface import clean
from hmpps_person_match.models.person.person import Person
from hmpps_person_match.models.person.person_batch import PersonBatch


@pytest.fixture()
async def get_db_connection() -> AsyncGenerator[AsyncConnection]:
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
async def create_person_record(get_db_connection):
    async def _create_person(person: Person):
        await clean.clean_and_insert(PersonBatch(records=[person]), get_db_connection)
    return _create_person
