import datetime

from pydantic import BaseModel, Field


class Person(BaseModel):
    """
    Pydantic Person Model
    """

    match_id: str = Field(alias="matchId")
    source_system: str = Field(alias="sourceSystem")
    first_name: str | None = Field(alias="firstName", default=None)
    middle_names: str | None = Field(alias="middleNames", default=None)
    last_name: str | None = Field(alias="lastName", default=None)
    date_of_birth: datetime.date | None = Field(alias="dateOfBirth", format="date", default=None)  # Ensures YYYY-MM-DD
    first_name_aliases: list[str] = Field(alias="firstNameAliases", default=[])
    last_name_aliases: list[str] = Field(alias="lastNameAliases", default=[])
    date_of_birth_aliases: list[datetime.date] = Field(
        alias="dateOfBirthAliases", format="date", default=[],
    )  # Ensures YYYY-MM-DD
    postcodes: list[str] = Field(alias="postcodes", default=[])
    cros: list[str] = []
    pncs: list[str] = []
    sentence_dates: list[datetime.date] = Field(alias="sentenceDates", format="date", default=[])  # Ensures YYYY-MM-DD
