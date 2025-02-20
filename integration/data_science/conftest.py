import pytest
from duckdb import DuckDBPyConnection
from sqlalchemy import text

from hmpps_cpr_splink.cpr_splink.interface.db import (
    duckdb_connected_to_postgres,
)

from .data import CleanedPerson


@pytest.fixture()
def duckdb_con_with_pg(db_connection) -> DuckDBPyConnection:
    return duckdb_connected_to_postgres(db_connection)


@pytest.fixture()
async def create_cleaned_person(db_connection):
    async def _create_cleaned_person(cleaned_person: CleanedPerson):
        person_dict = cleaned_person.as_dict()
        columns, data = zip(*person_dict.items(), strict=True)
        placeholders = ", ".join([f":{col}" for col in columns])
        query = text(
            f"INSERT INTO personmatch.person({', '.join(columns)}) VALUES ({placeholders}) ",  # noqa: S608
        )
        await db_connection.execute(query, person_dict)
        await db_connection.commit()
    return _create_cleaned_person
