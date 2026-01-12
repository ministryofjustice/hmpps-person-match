import pytest
from sqlalchemy import URL
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_cpr_splink.cpr_splink.interface.score import get_clusters, get_scored_candidates
from hmpps_cpr_splink.cpr_splink.model.model import POSSIBLE_TWINS_SIMILARITY_FLAG_THRESHOLD
from integration.mock_person import MockPerson
from integration.person_factory import PersonFactory
from integration.test_base import IntegrationTestBase


def make_semi_identical_people(differing_data: list[dict[str, dict]]) -> list[MockPerson]:
    """
    Create two people who have identical data except for specified fields
    """
    person_1 = MockPerson()
    person_2 = person_1.model_copy()
    for field_name, (value_person_1, value_person_2) in differing_data.items():
        setattr(person_1, field_name, value_person_1)
        setattr(person_2, field_name, value_person_2)

    return [person_1, person_2]


_TWIN_PARAMETERS = [
    pytest.param(
        {},
        False,
        id="Identical data; not twins",
    ),
    pytest.param(
        {
            "first_name": ["name", "completelydifferentname"],
            "first_name_aliases": [[], []],
            "pncs": [["00/0000000A"], ["99/9999999Z"]],
            "cros": [["00/000000A"], ["99/999999Z"]],
            "master_defendant_id": [None, None],
        },
        True,
        id="Simple twins",
    ),
    pytest.param(
        {
            "first_name": ["brian", "rian"],
            "first_name_aliases": [[], []],
            "pncs": [["00/0000000A"], ["99/9999999Z"]],
            "cros": [["00/000000A"], ["99/999999Z"]],
            "master_defendant_id": [None, None],
        },
        True,
        id="Explicitly mismatched IDs, and non-matching (but similar names); twins",
    ),
    pytest.param(
        {
            # first two letters of name so we can match on blocking rule
            "first_name": ["name", "nacompletelydifferentname"],
            "first_name_aliases": [[], []],
            "date_of_birth": ["1990-01-01", "1990-05-05"],
            "date_of_birth_aliases": [["1990-05-05"], []],
            "pncs": [["00/0000000A"], []],
            "cros": [[], ["99/999999Z"]],
            "master_defendant_id": [None, None],
        },
        True,
        id="DOB matches only via alias; twins",
    ),
    pytest.param(
        {
            "first_name": ["brian", "rian"],
            "first_name_aliases": [[], []],
            "pncs": [["00/0000000A"], ["99/9999999Z"]],
            "cros": [["00/000000A"], []],
            "master_defendant_id": [None, None],
        },
        True,
        id="One explicitly mismatched ID, and non-matching (but similar names); twins",
    ),
    pytest.param(
        {
            "first_name": ["brian", "rian"],
            "first_name_aliases": [[], []],
            "pncs": [["00/0000000A"], []],
            "cros": [["00/000000A"], []],
            "master_defendant_id": [None, None],
        },
        False,
        id="Not fully explicitly mismatched IDs, and non-matching (but similar names); not twins",
    ),
    pytest.param(
        {
            "first_name": ["brian", "rian"],
            "first_name_aliases": [[], ["brian"]],
            "pncs": [["00/0000000A"], ["99/9999999Z"]],
            "cros": [["00/000000A"], ["99/999999Z"]],
            "master_defendant_id": [None, None],
        },
        False,
        id="Explicitly mismatched IDs, and matching names; not twins",
    ),
    pytest.param(
        {
            "first_name": ["name", "completelydifferentname"],
            "first_name_aliases": [[], []],
            "pncs": [[], []],
            "cros": [[], []],
            "master_defendant_id": [None, None],
        },
        True,
        id="Twins, IDs not explicitly mismatched",
    ),
    pytest.param(
        {
            "first_name_aliases": [[], []],
            "pncs": [["00/0000000A"], ["99/9999999Z"]],
            "cros": [["00/000000A"], ["99/999999Z"]],
            "master_defendant_id": [None, None],
        },
        False,
        id="Matching first name; not twins",
    ),
    pytest.param(
        {
            "first_name": ["name", "completelydifferentname"],
            "first_name_aliases": [["alias"], ["aliasa"]],
            "pncs": [["00/0000000A"], []],
            "cros": [[], ["00/000000A"]],
            "master_defendant_id": [None, None],
        },
        False,
        id="Similar alias; not twins",
    ),
    pytest.param(
        {
            "first_name": ["name", "completelydifferentname"],
            "first_name_aliases": [[], ["name"]],
            "pncs": [["00/0000000A"], ["99/9999999Z"]],
            "cros": [["00/000000A"], ["99/999999Z"]],
            "master_defendant_id": [None, None],
        },
        False,
        id="Name matches alias; not twins",
    ),
    pytest.param(
        {
            "first_name": ["name", "completelydifferentname"],
            "first_name_aliases": [[], []],
            "pncs": [["00/0000000A"], ["00/0000000A"]],
            "cros": [["00/000000A"], ["99/999999Z"]],
            "master_defendant_id": [None, None],
        },
        False,
        id="Matching PNC; not twins",
    ),
    pytest.param(
        {
            "first_name": ["name", "completelydifferentname"],
            "first_name_aliases": [[], []],
            "pncs": [["00/0000000A"], ["99/9999999Z"]],
            "cros": [["00/000000A"], ["00/000000A"]],
            "master_defendant_id": [None, None],
        },
        False,
        id="Matching CRO; not twins",
    ),
    pytest.param(
        {
            "first_name": ["name", "completelydifferentname"],
            "middle_names": ["name", "name"],
            "first_name_aliases": [[], []],
            "pncs": [[], []],
            "cros": [[], []],
            "master_defendant_id": [None, None],
        },
        False,
        id="Second name matches first name; not twins",
    ),
    pytest.param(
        {
            "first_name": ["name", "completelydifferentname"],
            "first_name_aliases": [[], []],
            "pncs": [["00/0000000A"], ["99/9999999Z"]],
            "cros": [["00/000000A"], ["99/999999Z"]],
        },
        False,
        id="Look like twins, but have master_defendant_id override",
    ),
    pytest.param(
        {
            "first_name": ["name", "completelydifferentname"],
            "first_name_aliases": [[], []],
            "pncs": [["00/0000000A"], ["99/9999999Z"]],
            "cros": [["00/000000A"], ["99/999999Z"]],
            "master_defendant_id": [None, None],
            "override_marker": ["A", "A"],
            "override_scopes": [["B"], ["B"]],
        },
        False,
        id="Look like twins, but have include override marker",
    ),
]


class TestTwinDetection(IntegrationTestBase):
    """
    Test direct twin detection
    """

    @pytest.fixture(autouse=True, scope="function")
    async def clean_db(self, db_connection: AsyncSession) -> None:
        """
        Before Each
        Delete all records from the database
        """
        await self.truncate_person_data(db_connection)

    @pytest.mark.parametrize(
        ["differing_fields", "expected_flagged_as_twins"],
        _TWIN_PARAMETERS,
    )
    async def test_twin_scenarios_score(
        self,
        differing_fields: list[dict[str, dict]],
        expected_flagged_as_twins: bool,
        pg_db_url: URL,
        db_connection: AsyncSession,
        person_factory: PersonFactory,
    ) -> None:
        mock_people = make_semi_identical_people(differing_fields)
        for mock_person in mock_people:
            person = await person_factory.create_from(mock_person)
        scored_candidates = await get_scored_candidates(person.match_id, pg_db_url, db_connection)
        assert len(scored_candidates) == 1
        assert scored_candidates[0].candidate_is_possible_twin == expected_flagged_as_twins
        assert scored_candidates[0].unadjusted_match_weight > POSSIBLE_TWINS_SIMILARITY_FLAG_THRESHOLD
        if expected_flagged_as_twins:
            assert scored_candidates[0].candidate_match_weight < scored_candidates[0].unadjusted_match_weight
        else:
            assert scored_candidates[0].candidate_match_weight == scored_candidates[0].unadjusted_match_weight


    @pytest.mark.parametrize(
        ["differing_fields", "expected_flagged_as_twins"],
        _TWIN_PARAMETERS,
    )
    async def test_twins_scenarios_is_cluster_valid(
        self,
        differing_fields: list[dict[str, dict]],
        expected_flagged_as_twins: bool,
        pg_db_url: URL,
        db_connection: AsyncSession,
        person_factory: PersonFactory,
    ) -> None:
        mock_people = make_semi_identical_people(differing_fields)
        person_ids = []
        for mock_person in mock_people:
            person = await person_factory.create_from(mock_person)
            person_ids.append(person.match_id)
        await get_scored_candidates(person.match_id, pg_db_url, db_connection)
        # use a high postcode default tf so that we match more highly - effectively making
        # the random postcodes we use 'rare'
        clusters = await get_clusters(person_ids, pg_db_url, db_connection, default_postcode_tf=0.000001)
        assert clusters.is_single_cluster == (not expected_flagged_as_twins)
