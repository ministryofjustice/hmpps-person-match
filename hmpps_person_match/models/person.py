from pydantic import BaseModel, Field


class Person(BaseModel):
    """
    Pydantic Person Model
    """

    id: str
    source_system: str = Field(alias="sourceSystem")
    first_name: str = Field(alias="firstName")
    middle_names: str = Field(alias="middleNames")
    last_name: str = Field(alias="lastName")
    date_of_birth: str = Field(alias="dateOfBirth")
    first_name_aliases: list[str] = Field(alias="firstNameAliases")
    last_name_aliases: list[str] = Field(alias="lastNameAliases")
    date_of_birth_aliases: list[str] = Field(alias="dateOfBirthAliases")
    postcodes: list[str] = Field(alias="postcodes")
    cros: list[str]
    pncs: list[str]
    sentence_dates: list[str] = Field(alias="sentenceDates")
