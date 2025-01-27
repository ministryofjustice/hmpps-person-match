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
    first_name: Optional[str]
    middle_names: Optional[str]
    last_name: Optional[str]
    crn: Optional[str]
    prison_number: Optional[str]
    date_of_birth: Optional[date]
    sex: Optional[str]
    ethnicity: Optional[str]
    first_name_alias_arr: Optional[list[str]]
    last_name_alias_arr: Optional[list[str]]
    date_of_birth_alias_arr: Optional[list[date]]
    postcode_arr: Optional[list[str]]
    cro_arr: Optional[list[str]]
    pnc_arr: Optional[list[str]]
    sentence_date_arr: Optional[list[date]]


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
