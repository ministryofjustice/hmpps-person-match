import datetime

from pydantic import BaseModel, Field, field_validator


class Person(BaseModel):
    """
    Pydantic Person Model
    """

    match_id: str = Field(alias="matchId")
    source_system: str = Field(alias="sourceSystem")
    first_name: str = Field(alias="firstName")
    middle_names: str = Field(alias="middleNames")
    last_name: str = Field(alias="lastName")
    date_of_birth: datetime.date | None = Field(alias="dateOfBirth", format="date", default=None)  # Ensures YYYY-MM-DD
    first_name_aliases: list[str] = Field(alias="firstNameAliases")
    last_name_aliases: list[str] = Field(alias="lastNameAliases")
    date_of_birth_aliases: list[datetime.date] = Field(alias="dateOfBirthAliases", format="date")  # Ensures YYYY-MM-DD
    postcodes: list[str] = Field(alias="postcodes")
    cros: list[str]
    pncs: list[str]
    crn: str
    prison_number: str = Field(alias="prisonNumber", format="date")
    sentence_dates: list[datetime.date] = Field(alias="sentenceDates", format="date")  # Ensures YYYY-MM-DD

    @field_validator("date_of_birth", mode="before")
    @classmethod
    def validate_date(cls, v):
        if v == "":
            return None
        return v
