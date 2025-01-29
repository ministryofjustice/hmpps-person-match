import duckdb
from sqlalchemy.ext.asyncio import AsyncConnection

from hmpps_person_match.models.person import Person

from ..model_cleaning import create_table_from_records, simple_clean_whole_joined_table
from ..schemas import DUCKDB_COLUMNS_WITH_TYPES
from .db import insert_duckdb_table_into_postgres_table


def clean_and_insert(record: Person, connection: AsyncConnection) -> None:
    """
    Takes in a single record in joined format.

    Cleans the record and inserts into postgres table
    """
    con = duckdb.connect(":memory:")
    record_table_name = "record_table"
    create_table_from_records(con, [record.model_dump()], record_table_name, DUCKDB_COLUMNS_WITH_TYPES)

    t_cleaned = simple_clean_whole_joined_table(record_table_name)
    sql = t_cleaned.create_table_sql
    con.sql(sql)
    insert_duckdb_table_into_postgres_table(
        con.table(t_cleaned.name),
        "person",
        connection,
    )
