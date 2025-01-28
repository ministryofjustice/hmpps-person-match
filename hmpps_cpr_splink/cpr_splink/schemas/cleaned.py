from datetime import datetime
from typing import Optional, TypedDict


class ArrayWithTF(TypedDict):
    value: str
    rel_freq: float


class CleanedRecord(TypedDict):
    # TODO: they are probably almost all Optional
    id: int
    # for name comparison
    name_1_std: Optional[str]
    tf_name_1_std: Optional[float]
    last_name_std: Optional[str]
    tf_last_name_std: Optional[float]
    first_and_last_name_std: Optional[str]
    tf_first_and_last_name_std: Optional[float]
    forename_std_arr: Optional[list[str]]
    last_name_std_arr: Optional[list[str]]
    # name 2
    name_2_std: Optional[str]
    tf_name_2_std: Optional[float]
    # sentence date
    sentence_date_arr: Optional[list[datetime]]
    # dob
    date_of_birth: Optional[datetime]
    tf_date_of_birth: Optional[float]
    date_of_birth_arr: Optional[datetime]
    # pc
    postcode_arr: Optional[list[str]]
    postcode_arr_with_freq: Optional[list[ArrayWithTF]]
    postcode_outcode_arr: Optional[list[str]]
    # ids
    cro_single: Optional[str]
    tf_cro_single: Optional[float]
    pnc_single: Optional[str]
    tf_pnc_single: Optional[float]
    # where from
    source_system: str
