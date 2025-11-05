from datetime import datetime
from typing import TypedDict


class ArrayWithTF(TypedDict):
    value: str
    rel_freq: float


class CleanedRecord(TypedDict):
    id: int
    # for name comparison
    name_1_std: str | None
    tf_name_1_std: float | None
    last_name_std: str | None
    tf_last_name_std: float | None
    first_and_last_name_std: str | None
    tf_first_and_last_name_std: float | None
    forename_std_arr: list[str] | None
    forename_first: str | None
    forename_last: str | None
    last_name_std_arr: list[str] | None
    last_name_last: str | None
    # name 2
    name_2_std: str | None
    tf_name_2_std: float | None
    # sentence date
    sentence_date_arr: list[datetime] | None
    sentence_date_first: datetime | None
    sentence_date_last: datetime | None
    # dob
    date_of_birth: datetime | None
    tf_date_of_birth: float | None
    date_of_birth_arr: list[datetime] | None
    date_of_birth_last: datetime | None
    # pc
    postcode_arr: list[str] | None
    postcode_arr_with_freq: list[ArrayWithTF] | None
    postcode_first: str | None
    postcode_second: str | None
    postcode_last: str | None
    postcode_outcode_arr: list[str] | None
    postcode_outcode_first: str | None
    postcode_outcode_last: str | None
    # ids
    cro_single: str | None
    tf_cro_single: float | None
    pnc_single: str | None
    tf_pnc_single: float | None
    # where from
    source_system: str
