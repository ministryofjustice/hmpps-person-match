from datetime import datetime
from typing import TypedDict


class ArrayWithTF(TypedDict):
    value: str
    rel_freq: float


class CleanedRecord(TypedDict):
    # TODO: they are probably almost all Optional
    id: int
    # for name comparison
    name_1_std: str | None
    tf_name_1_std: float | None
    last_name_std: str | None
    tf_last_name_std: float | None
    first_and_last_name_std: str | None
    tf_first_and_last_name_std: float | None
    forename_std_arr: list[str] | None
    last_name_std_arr: list[str] | None
    # name 2
    name_2_std: str | None
    tf_name_2_std: float | None
    # sentence date
    sentence_date_arr: list[datetime] | None
    # dob
    date_of_birth: datetime | None
    tf_date_of_birth: float | None
    date_of_birth_arr: datetime | None
    # pc
    postcode_arr: list[str] | None
    postcode_arr_with_freq: list[ArrayWithTF] | None
    postcode_outcode_arr: list[str] | None
    # ids
    cro_single: str | None
    tf_cro_single: float | None
    pnc_single: str | None
    tf_pnc_single: float | None
    # where from
    source_system: str
