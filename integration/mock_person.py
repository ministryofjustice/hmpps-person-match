from pydantic import Field

from hmpps_person_match.models.person.person import Person
from integration import random_test_data


class MockPerson(Person):
    """
    Inherits main person model
    Sets random test data values
    """

    match_id: str = Field(alias="matchId")
    first_name: str = Field(alias="firstName", default_factory=lambda: random_test_data.random_name())
    middle_names: str = Field(alias="middleNames", default_factory=lambda: random_test_data.random_name())
    last_name: str = Field(alias="lastName", default_factory=lambda: random_test_data.random_name())
    crn: str = random_test_data.random_crn()
    prison_number: str = Field(alias="prisonNumber", default_factory=lambda: random_test_data.random_prison_number())
    first_name_aliases: list[str] = Field(
        alias="firstNameAliases",
        default_factory=lambda: [random_test_data.random_name()],
    )
    last_name_aliases: list[str] = Field(
        alias="lastNameAliases",
        default_factory=lambda: [random_test_data.random_name()],
    )
    date_of_birth: str = Field(
        alias="dateOfBirth",
        default_factory=lambda: random_test_data.random_date(),
    )
    date_of_birth_aliases: list[str] = Field(
        alias="dateOfBirthAliases",
        default_factory=lambda: [random_test_data.random_date()],
    )
    postcodes: list[str] = Field(default_factory=lambda: [random_test_data.random_postcode()])
    cros: list[str] = Field(default_factory=lambda: [random_test_data.random_cro()])
    pncs: list[str] = Field(default_factory=lambda: [random_test_data.random_pnc()])
    sentence_dates: list[str] = Field(
        alias="sentenceDates",
        default_factory=lambda: [random_test_data.random_date()],
    )
    source_system: str = Field(
        alias="sourceSystem",
        default_factory=lambda: random_test_data.random_source_system(),
    )
