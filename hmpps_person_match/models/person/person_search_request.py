import datetime

from pydantic import BaseModel, ConfigDict, Field

from hmpps_person_match.models.person.person import Person


class PersonSearchRequest(BaseModel):
    """Information supplied to an ad hoc person search."""

    model_config = ConfigDict(extra="forbid")

    full_name: str | None = Field(alias="fullName", default=None, examples=["John Paul Smith"], min_length=1)
    date_of_birth: datetime.date | None = Field(
        alias="dateOfBirth",
        default=None,
        json_schema_extra={"format": "date"},
        examples=["1989-04-12"],
    )
    first_name_aliases: list[str] = Field(
        alias="firstNameAliases",
        default_factory=list,
        examples=[["Johnny"]],
    )
    last_name_aliases: list[str] = Field(
        alias="lastNameAliases",
        default_factory=list,
        examples=[["Smithson"]],
    )
    date_of_birth_aliases: list[datetime.date] = Field(
        alias="dateOfBirthAliases",
        default_factory=list,
        json_schema_extra={"format": "date"},
        examples=[["1989-04-13"]],
    )
    postcodes: list[str] = Field(default_factory=list, examples=[["AB1 2CD"]])


def person_search_request_to_person(
    search_request: PersonSearchRequest,
    internal_match_id: str,
) -> Person:
    """Map search evidence into the canonical input expected by the cleaner."""
    return Person(
        matchId=internal_match_id,
        sourceSystem="PERSON_SEARCH",
        sourceSystemId=internal_match_id,
        masterDefendantId=None,
        firstName=search_request.full_name,
        middleNames=None,
        lastName=None,
        dateOfBirth=search_request.date_of_birth,
        firstNameAliases=search_request.first_name_aliases,
        lastNameAliases=search_request.last_name_aliases,
        dateOfBirthAliases=search_request.date_of_birth_aliases,
        postcodes=search_request.postcodes,
        cros=[],
        pncs=[],
        sentenceDates=[],
        overrideMarker=None,
        overrideScopes=None,
    )
