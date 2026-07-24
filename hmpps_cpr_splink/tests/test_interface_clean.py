from hmpps_cpr_splink.cpr_splink.interface.clean import clean_person
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

    cleaned = clean_person(person, "request-scoped-id")

    assert cleaned["match_id"] == "request-scoped-id"
    assert cleaned["name_1_std"] is None
    assert cleaned["postcode_arr"] is None
    assert cleaned["cro_single"] is None
    assert cleaned["pnc_single"] is None
    assert person.match_id == "caller-supplied-id"
