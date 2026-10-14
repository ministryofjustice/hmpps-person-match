import datetime

from pydantic import BaseModel, ConfigDict, Field

from hmpps_person_match.models.person.person import Person


class EmdiExactMatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    first_name: str = Field(alias="firstName", examples=["Henry"], min_length=1, pattern=r"\S")
    last_name: str = Field(alias="lastName", examples=["Junaed"], min_length=1, pattern=r"\S")
    date_of_birth: datetime.date = Field(
        alias="dateOfBirth",
        json_schema_extra={"format": "date"},
        examples=["1992-03-02"],
    )
    pnc: str = Field(alias="pnc", examples=["2000/1234567A"], min_length=1, pattern=r"\S")


def emdi_exact_match_request_to_person(
    request: EmdiExactMatchRequest,
    internal_match_id: str,
) -> Person:
    return Person(
        matchId=internal_match_id,
        sourceSystem="PERSON_SEARCH",
        sourceSystemId=internal_match_id,
        masterDefendantId=None,
        firstName=request.first_name,
        middleNames=None,
        lastName=request.last_name,
        dateOfBirth=request.date_of_birth,
        firstNameAliases=[],
        lastNameAliases=[],
        dateOfBirthAliases=[],
        postcodes=[],
        cros=[],
        pncs=[request.pnc],
        sentenceDates=[],
        overrideMarker=None,
        overrideScopes=None,
    )
