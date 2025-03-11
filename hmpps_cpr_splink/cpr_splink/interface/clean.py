import duckdb
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_cpr_splink.cpr_splink.interface.db import insert_duckdb_table_into_postgres_table
from hmpps_cpr_splink.cpr_splink.model_cleaning import simple_clean_whole_joined_table
from hmpps_cpr_splink.cpr_splink.schemas import DUCKDB_COLUMNS_WITH_TYPES
from hmpps_cpr_splink.cpr_splink.utils import create_table_from_records
from hmpps_person_match.models.person.person_batch import PersonBatch


async def clean_and_insert(records: PersonBatch, connection_pg: AsyncSession) -> None:
    """
    Takes in a single record in joined format.

    Cleans the record and inserts into postgres table
    """
    connection_duckdb = duckdb.connect(":memory:")
    record_table_name = "record_table"
    person_records = records.model_dump()["records"]
    create_table_from_records(connection_duckdb, person_records, record_table_name, DUCKDB_COLUMNS_WITH_TYPES)

    t_cleaned = simple_clean_whole_joined_table(record_table_name)
    sql = t_cleaned.create_table_sql
    connection_duckdb.sql(sql)
    await insert_duckdb_table_into_postgres_table(
        connection_duckdb.table(t_cleaned.name),
        "personmatch.person",
        connection_pg,
    )
