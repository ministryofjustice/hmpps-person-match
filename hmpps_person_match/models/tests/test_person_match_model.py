import pytest
from pydantic import ValidationError

from hmpps_person_match.models.person_match_model import MatchingFromPerson, MatchingToPerson, PersonMatching


class TestPersonMatchModel:
    """
    Test person matching model
    """

    def test_model_populates_source_dataset(self):
        """
        Test model populates the source dataset property accordingly
        """
        person_match_model = PersonMatching(
            matching_from=MatchingFromPerson(unique_id="1", pnc="1234567890"),
            matching_to=[MatchingToPerson(unique_id="2", pnc="1234567890")],
        )
        assert person_match_model.matching_from.source_dataset == "matching_from"
        assert person_match_model.matching_to[0].source_dataset == "matching_to"

    def test_model_allows_mulitiple_records(self):
        """
        Test model allows multiple records
        """
        person_match_model = PersonMatching(
            matching_from=MatchingFromPerson(unique_id="1", pnc="1234567890"),
            matching_to=[MatchingToPerson(unique_id="2", pnc="1234567890")] * 20,
        )
        assert len(person_match_model.matching_to) == 20

    def test_model_doesnt_allow_more_than_limit_to_match(self):
        """
        Test model doesnt allow more than 100 records
        """
        with pytest.raises(ValidationError) as err:
            PersonMatching(
                matching_from=MatchingFromPerson(unique_id="1", pnc="1234567890"),
                matching_to=[MatchingToPerson(unique_id="2", pnc="1234567890")] * 101,
            )
        assert err.value.error_count() == 1
        assert err.value.errors()[0].get("loc") == ("matching_to",)
        assert err.value.errors()[0].get("msg") == "List should have at most 100 items after validation, not 101"

    def test_model_requires_unique_id(self):
        """
        Test model requires unique_id
        """
        with pytest.raises(ValidationError) as err:
            PersonMatching(
                matching_from=MatchingFromPerson(pnc="1234567890"),
                matching_to=[MatchingToPerson(pnc="1234567890")] * 51,
            )
        assert err.value.error_count() == 1
        assert err.value.errors()[0].get("loc") == ("unique_id",)
        assert err.value.errors()[0].get("msg") == "Field required"
