import datetime

from pydantic import BaseModel, Field, field_validator


class Person(BaseModel):
    """
    Pydantic Person Model
    """

    match_id: str = Field(alias="matchId", examples=["ec30e2d2-b4c2-4c42-9e14-514aa58edff5"])
    source_system: str = Field(alias="sourceSystem", examples=["COMMON_PLATFORM"])
    source_system_id: str = Field(alias="sourceSystemId", examples=["479bdd8e-f22a-42c5-8f7e-91e690426464"])
    first_name: str = Field(alias="firstName", examples=["Jane"])
    middle_names: str = Field(alias="middleNames", examples=["Grace"])
    last_name: str = Field(alias="lastName", examples=["Doe"])
    date_of_birth: datetime.date | None = Field(
        alias="dateOfBirth",
        format="date",
        default=None,
        examples=["1970-01-01"],
    )  # Ensures YYYY-MM-DD
    first_name_aliases: list[str] = Field(alias="firstNameAliases", examples=[["Jayne"]])
    last_name_aliases: list[str] = Field(alias="lastNameAliases", examples=[["Smith"]])
    date_of_birth_aliases: list[datetime.date] = Field(
        alias="dateOfBirthAliases",
        format="date",
        examples=[["1970-01-01"]],
    )  # Ensures YYYY-MM-DD
    postcodes: list[str] = Field(alias="postcodes", examples=[["AB1 2BC"]])
    cros: list[str] = Field(examples=[["123456/00A"]])
    pncs: list[str] = Field(examples=[["2000/1234567A"]])
    sentence_dates: list[datetime.date] = Field(
        alias="sentenceDates",
        format="date",
        examples=[["2025-01-01"]],
    )  # Ensures YYYY-MM-DD

    @field_validator("date_of_birth", mode="before")
    @classmethod
    def validate_date(cls, v):
        if v == "":
            return None
        return v
