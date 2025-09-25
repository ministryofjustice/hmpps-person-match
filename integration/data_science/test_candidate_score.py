import uuid

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_cpr_splink.cpr_splink.interface.score import get_scored_candidates
from integration import random_test_data
from integration.mock_person import MockPerson
from integration.person_factory import PersonFactory
from integration.test_base import IntegrationTestBase


class TestPersonScore(IntegrationTestBase):
    """
    Test functioning of candidate search
    """

    HIGH_MATCH_WEIGHT = 20

    @pytest.fixture(autouse=True, scope="function")
    async def clean_db(self, db_connection: AsyncSession):
        """
        Before Each
        Delete all records from the database
        """
        await self.truncate_person_data(db_connection)

    async def test_get_scored_candidates(
        self,
        pg_db_url,
        db_connection,
        person_factory: PersonFactory,
    ):
        """
        Test retrieving scored candidates gives correct number
        """
        # primary record
        person = await person_factory.create_from(MockPerson())

        # candidates - all should match with high match weight
        n_candidates = 10
        for _ in range(n_candidates):
            await person_factory.create_from(person)

        res = await get_scored_candidates(person.match_id, pg_db_url, db_connection)

        # we have all candidates + original record
        assert len(res) == n_candidates
        assert (
            len([match_weight for r in res if (match_weight := r.candidate_match_weight) > self.HIGH_MATCH_WEIGHT])
            == n_candidates
        )

    @pytest.mark.parametrize(
        "person_data",
        [
            {"firstName": ""},
            {"middleNames": ""},
            {"lastName": ""},
            {"firstNameAliases": []},
            {"lastNameAliases": []},
            {"dateOfBirthAliases": []},
            {"postcodes": []},
            {"cros": []},
            {"pncs": []},
            {"sentenceDates": []},
            {"sourceSystem": ""},
            {"sourceSystemId": ""},
            {"masterDefendantId": ""},
            {"overrideMarker": ""},
            {"overrideScopes": []},
        ],
    )
    async def test_get_scored_candidates_blank_data(
        self,
        person_factory: PersonFactory,
        pg_db_url,
        db_connection,
        person_data,
    ):
        """
        Test that we can score candidates even if fields are 'empty'
        """
        primary_record = await person_factory.create_from(MockPerson(**person_data))
        # candidates - all should match with high match weight
        n_candidates = 10
        for _ in range(n_candidates):
            await person_factory.create_from(primary_record)

        res = await get_scored_candidates(primary_record.match_id, pg_db_url, db_connection)

        # we have all candidates + original record
        assert len(res) == n_candidates
        assert (
            len([match_weight for r in res if (match_weight := r.candidate_match_weight) > self.HIGH_MATCH_WEIGHT])
            == n_candidates
        )

    async def test_get_scored_candidates_none_in_db(
        self,
        person_factory: PersonFactory,
        pg_db_url,
        db_connection,
    ):
        """
        Test scoring returns nothing if the given match_id is not in db
        """
        n_candidates = 10
        for _ in range(n_candidates):
            await person_factory.create_from(MockPerson())

        res = await get_scored_candidates("bogus_match_id", pg_db_url, db_connection)

        assert len(res) == 0

    async def test_score_unaffected_when_manual_override_is_null(
        self,
        person_factory: PersonFactory,
        pg_db_url,
        db_connection,
    ):
        """
        Tests that a NULL override_marker has no effect on the score.

        It does this by creating two moderately similar records and scoring them.
        It then creates an identical pair where one record has a NULL override
        and asserts that the score is effectively the same.
        """
        #  Guarantee it's caught by candidate search by using same PNC
        shared_pnc = random_test_data.random_pnc()

        person_1 = await person_factory.create_from(
            MockPerson(
                firstName="Baseline",
                lastName="Person",
                dateOfBirth="1990-01-01",
                pncs=[shared_pnc],
            ),
        )
        person_2 = await person_factory.create_from(
            MockPerson(
                firstName="Baseline",
                lastName="Person",
                dateOfBirth="1991-02-02",
                pncs=[shared_pnc],
            ),
        )

        scored_candidates = await get_scored_candidates(person_1.match_id, pg_db_url, db_connection)

        assert scored_candidates[0].candidate_match_id == person_2.match_id
        scored_candidates_weight = scored_candidates[0].candidate_match_weight

        # If there was a manual override, we'd expect an extreme match weight
        assert -40 < scored_candidates_weight < 40

    async def test_score_is_very_high_when_manual_overrides_match(
        self,
        person_factory: PersonFactory,
        pg_db_url,
        db_connection,
    ):
        """
        Tests that if two records share the same override_marker UUID,
        they receive a very high match weight, forcing them to match.
        """
        # Arrange: Two completely different people linked by an override ID
        override_id = str(uuid.uuid4())

        #  Guarantee it's caught by candidate search by using same PNC
        shared_pnc = random_test_data.random_pnc()

        person_1 = await person_factory.create_from(
            MockPerson(
                firstName="John",
                lastName="Smith",
                pncs=[shared_pnc],
                overrideMarker=override_id,
            ),
        )
        person_2 = await person_factory.create_from(
            MockPerson(
                firstName="Jane",
                lastName="Doe",
                pncs=[shared_pnc],
                overrideMarker=override_id,
            ),
        )

        scored_candidates = await get_scored_candidates(person_1.match_id, pg_db_url, db_connection)

        assert len(scored_candidates) == 1
        assert scored_candidates[0].candidate_match_id == person_2.match_id
        assert scored_candidates[0].candidate_match_weight > 100

    async def test_score_is_very_low_when_overrides_differ_in_same_scope(
        self,
        person_factory: PersonFactory,
        pg_db_url,
        db_connection,
    ):
        """
        Tests that if two records have different override_markers but share
        an override_scope, they receive a very low match weight, forcing them apart.
        """
        # Two identical people who should be forced NOT to match
        scope_id = str(uuid.uuid4())

        #  Guarantee it's caught by candidate search by using same PNC
        shared_pnc = random_test_data.random_pnc()

        # Create a base person record that would normally be a perfect match to itself
        person_1 = await person_factory.create_from(
            MockPerson(pncs=[shared_pnc], overrideMarker=str(uuid.uuid4()), overrideScopes=[scope_id]),
        )

        person_2 = await person_factory.create_from(
            MockPerson(pncs=[shared_pnc], overrideMarker=str(uuid.uuid4()), overrideScopes=[scope_id]),
        )

        scored_candidates = await get_scored_candidates(person_1.match_id, pg_db_url, db_connection)

        assert len(scored_candidates) == 1
        assert scored_candidates[0].candidate_match_id == person_2.match_id
        assert scored_candidates[0].candidate_match_weight < -100

    async def test_score_is_unaffected_when_overrides_differ_in_different_scopes(
        self,
        person_factory: PersonFactory,
        pg_db_url,
        db_connection,
    ):
        """
        Tests that if two records have different override_markers _and_ no overlapping
        override_scopes, their match weight is unaffected.
        """
        #  Guarantee pair caught by candidate search by using same PNC
        shared_pnc = random_test_data.random_pnc()

        # create two different people, and capture score
        person_1 = await person_factory.create_from(
            MockPerson(
                firstName="Baseline",
                lastName="Person",
                dateOfBirth="1990-01-01",
                pncs=[shared_pnc],
            ),
        )
        person_2 = await person_factory.create_from(
            MockPerson(
                firstName="Baseline",
                lastName="Person",
                dateOfBirth="1991-02-02",
                pncs=[shared_pnc],
            ),
        )

        scored_candidates = await get_scored_candidates(person_1.match_id, pg_db_url, db_connection)
        scored_candidates_weight = scored_candidates[0].candidate_match_weight

        # Should be no manual override, as it's null
        assert -40 < scored_candidates_weight < 40

        # update people to have different override markers with no overlapping scope
        person_1.override_marker = uuid.uuid4()
        person_1.override_scopes = [uuid.uuid4()]
        await person_factory.update(person_1)

        person_2.override_marker = uuid.uuid4()
        person_2.override_scopes = [uuid.uuid4()]
        await person_factory.update(person_2)

        scored_candidates = await get_scored_candidates(person_1.match_id, pg_db_url, db_connection)
        new_scored_candidates_weight = scored_candidates[0].candidate_match_weight

        assert scored_candidates_weight == new_scored_candidates_weight
