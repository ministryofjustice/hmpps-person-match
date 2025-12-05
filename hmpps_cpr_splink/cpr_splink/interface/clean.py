import duckdb
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_cpr_splink.cpr_splink.interface.db import insert_duckdb_table_into_postgres_table
from hmpps_cpr_splink.cpr_splink.model_cleaning import simple_clean_whole_joined_table
from hmpps_cpr_splink.cpr_splink.schemas import DUCKDB_COLUMNS_WITH_TYPES
from hmpps_cpr_splink.cpr_splink.utils import create_table_from_records
from hmpps_person_match.models.person.person import Person
from hmpps_person_match.models.person.person_batch import PersonBatch


def _load_and_clean_records_into_duckdb(
    conn: duckdb.DuckDBPyConnection,
    records: list[dict],
    base_table_name: str = "record_table",
) -> str:
    """
    Load records into DuckDB and apply cleaning transformations.

    This is the shared helper for both batch inserts and single record cleaning.

    Args:
        conn: DuckDB connection
        records: List of record dictionaries (from PersonBatch.model_dump()["records"])
        base_table_name: Name for the intermediate table before cleaning

    Returns:
        The name of the cleaned table in DuckDB
    """
    create_table_from_records(conn, records, base_table_name, DUCKDB_COLUMNS_WITH_TYPES)
    t_cleaned = simple_clean_whole_joined_table(base_table_name)
    conn.execute(t_cleaned.create_table_sql)
    return t_cleaned.name


async def clean_and_insert(records: PersonBatch, connection_pg: AsyncSession) -> None:
    """
    Takes in records in joined format.

    Cleans the records and inserts into postgres table.
    """
    with duckdb.connect(":memory:") as conn:
        table_name = _load_and_clean_records_into_duckdb(
            conn,
            records.model_dump()["records"],
        )
        await insert_duckdb_table_into_postgres_table(
            conn.table(table_name),
            "personmatch.person",
            connection_pg,
        )


def clean_record_to_duckdb(
    connection_duckdb: duckdb.DuckDBPyConnection,
    person: Person,
) -> str:
    """
    Clean a single person record and return it as a DuckDB table.

    Returns:
        The name of the cleaned table in DuckDB
    """
    return _load_and_clean_records_into_duckdb(
        connection_duckdb,
        PersonBatch(records=[person]).model_dump()["records"],
        base_table_name="record_to_clean",
    )
