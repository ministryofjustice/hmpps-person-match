from hmpps_cpr_splink.cpr_splink.interface.clean import clean_person_for_search
from hmpps_person_match.models.person.person_search_request import (
    PersonSearchRequest,
    person_search_request_to_person,
)
from integration.mock_person import MockPerson


def test_clean_person_uses_internal_identity_without_persisting() -> None:
    person = MockPerson(
        matchId="caller-supplied-id",
        firstName="",
        middleNames="",
        firstNameAliases=[],
        lastNameAliases=[],
        postcodes=[],
        cros=[],
        pncs=[],
    )

    cleaned = clean_person_for_search(person, "request-scoped-id")

    assert cleaned["match_id"] == "request-scoped-id"
    assert cleaned["name_1_std"] is None
    assert cleaned["postcode_arr"] is None
    assert cleaned["cro_single"] is None
    assert cleaned["pnc_single"] is None
    assert person.match_id == "caller-supplied-id"


def test_search_request_full_name_is_mapped_and_cleaned() -> None:
    search_request = PersonSearchRequest(
        fullName="John Paul Smith",
        dateOfBirth="1989-04-12",
        firstNameAliases=["Johnny"],
        lastNameAliases=["Smithson"],
        dateOfBirthAliases=["1989-04-13"],
        postcodes=["AB1 2CD"],
    )

    person = person_search_request_to_person(search_request, "request-scoped-id")
    cleaned = clean_person_for_search(person, "request-scoped-id")

    assert person.match_id == "request-scoped-id"
    assert person.source_system == "PERSON_SEARCH"
    assert person.cros == []
    assert person.pncs == []
    assert person.sentence_dates == []
    assert cleaned["name_1_std"] == "JOHN"
    assert cleaned["name_2_std"] == "PAUL"
    assert cleaned["name_3_std"] is None
    assert cleaned["last_name_std"] == "SMITH"
    assert cleaned["first_and_last_name_std"] == "JOHN SMITH"
    assert cleaned["forename_std_arr"] == ["JOHN", "JOHNNY"]
    assert cleaned["last_name_std_arr"] == ["SMITH", "SMITHSON"]
    assert [str(value) for value in cleaned["date_of_birth_arr"]] == ["1989-04-12", "1989-04-13"]
    assert cleaned["postcode_arr"] == ["AB12CD"]
