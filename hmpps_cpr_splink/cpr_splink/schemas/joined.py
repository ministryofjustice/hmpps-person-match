from datetime import date
from typing import Optional, TypedDict


class JoinedRecord(TypedDict):
    """
    Represents a 'raw' (uncleaned) record retrieved from the Core Person Record (CPR)
    system.

    This class defines the schema for records exchanged between the CPR system and the
    `cpr_splink` library.  Records are expected to conform to this schema,
    as specified by a Pydantic model.
    """

    id: int
    source_system: str
    first_name: str | None
    middle_names: str | None
    last_name: str | None
    date_of_birth: date | None
    first_name_alias_arr: list[str] | None
    last_name_aliases: list[str] | None
    date_of_birth_alias_arr: list[date] | None
    postcode_arr: list[str] | None
    cro_arr: list[str] | None
    pnc_arr: list[str] | None
    sentence_date_arr: list[date] | None


PYDANTIC_TO_DUCKDB_TYPE_MAPPING = {
    int: "INTEGER",
    str: "VARCHAR",
    Optional[str]: "VARCHAR",
    Optional[int]: "INTEGER",
    Optional[date]: "DATE",
    Optional[list[str]]: "TEXT[]",
    Optional[list[date]]: "DATE[]",
}


def generate_duckdb_columns_with_types(typed_dict):
    columns = []
    for key, value in typed_dict.__annotations__.items():
        sql_type = PYDANTIC_TO_DUCKDB_TYPE_MAPPING.get(value)
        columns.append((key, sql_type))
    return columns


DUCKDB_COLUMNS_WITH_TYPES = generate_duckdb_columns_with_types(JoinedRecord)
