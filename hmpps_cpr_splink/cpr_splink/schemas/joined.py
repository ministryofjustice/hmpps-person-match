from datetime import date
from typing import TypedDict


class JoinedRecord(TypedDict):
    """
    Represents a 'raw' (uncleaned) record retrieved from the Core Person Record (CPR)
    system.

    This class defines the schema for records exchanged between the CPR system and the
    `cpr_splink` library.  Records are expected to conform to this schema,
    as specified by a Pydantic model.
    """

    id: str
    source_system: str
    first_name: str | None
    middle_names: str | None
    last_name: str | None
    date_of_birth: date | None
    first_name_aliases: list[str] | None
    last_name_aliases: list[str] | None
    date_of_birth_aliases: list[date] | None
    postcodes: list[str] | None
    cros: list[str] | None
    pncs: list[str] | None
    sentence_dates: list[date] | None


PYDANTIC_TO_DUCKDB_TYPE_MAPPING = {
    int: "INTEGER",
    str: "VARCHAR",
    str | None: "VARCHAR",
    int | None: "INTEGER",
    date | None: "DATE",
    list[str] | None: "TEXT[]",
    list[date] | None: "DATE[]",
}


def generate_duckdb_columns_with_types(typed_dict):
    columns = []
    for key, value in typed_dict.__annotations__.items():
        sql_type = PYDANTIC_TO_DUCKDB_TYPE_MAPPING.get(value)
        columns.append((key, sql_type))
    return columns


DUCKDB_COLUMNS_WITH_TYPES = generate_duckdb_columns_with_types(JoinedRecord)
