from typing import Any

import duckdb
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_cpr_splink.cpr_splink.interface.db import insert_duckdb_table_into_postgres_table
from hmpps_cpr_splink.cpr_splink.model_cleaning import simple_clean_whole_joined_table
from hmpps_cpr_splink.cpr_splink.schemas import DUCKDB_COLUMNS_WITH_TYPES
from hmpps_cpr_splink.cpr_splink.utils import create_table_from_records
from hmpps_person_match.models.person.person import Person
from hmpps_person_match.models.person.person_batch import PersonBatch


def _clean_records(records: PersonBatch, connection_duckdb: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    record_table_name = "record_table"
    person_records = records.model_dump()["records"]
    create_table_from_records(connection_duckdb, person_records, record_table_name, DUCKDB_COLUMNS_WITH_TYPES)

    cleaned_table = simple_clean_whole_joined_table(record_table_name)
    connection_duckdb.sql(cleaned_table.create_table_sql)
    return connection_duckdb.table(cleaned_table.name)


def clean_person(person: Person, internal_match_id: str) -> dict[str, Any]:
    search_person = person.model_copy(update={"match_id": internal_match_id})
    with duckdb.connect(":memory:") as connection_duckdb:
        cleaned = _clean_records(PersonBatch(records=[search_person]), connection_duckdb)
        row = cleaned.fetchone()
        columns = [description[0] for description in cleaned.description]

    if row is None:
        raise ValueError("Person cleaning produced no records")
    return dict(zip(columns, row, strict=True))


async def clean_and_insert(records: PersonBatch, connection_pg: AsyncSession) -> None:
    """
    Takes in a single record in joined format.

    Cleans the record and inserts into postgres table
    """
    with duckdb.connect(":memory:") as connection_duckdb:
        cleaned = _clean_records(records, connection_duckdb)
        await insert_duckdb_table_into_postgres_table(
            cleaned,
            "personmatch.person",
            connection_pg,
        )
