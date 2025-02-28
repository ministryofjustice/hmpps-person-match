import uuid

from dataclasses import dataclass,field
from integration import random_test_data


@dataclass
class PersonData:
    match_id: str = str(uuid.uuid4()),
    first_name: str = random_test_data.random_name()
    middle_names: str =  random_test_data.random_name()
    last_name: str = random_test_data.random_name()
    crn: str = random_test_data.random_crn()
    date_of_birth: str = random_test_data.random_date()
    first_name_aliases:  list[str] = field(default_factory=lambda: [random_test_data.random_name()])
    last_name_aliases:  list[str] = field(default_factory=lambda: [random_test_data.random_name()])
    date_of_birth_aliases:  list[str] = field(default_factory=lambda: [random_test_data.random_date()])
    postcodes:  list[str] = field(default_factory=lambda: [random_test_data.random_postcode()])
    cros:  list[str] = field(default_factory=lambda: [random_test_data.random_cro()])
    pncs:  list[str] = field(default_factory=lambda: [random_test_data.random_pnc()])
    sentence_dates: list[str] = field(default_factory=lambda: [random_test_data.random_date()])
