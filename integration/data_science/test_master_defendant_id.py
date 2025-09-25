import uuid

import pytest

from hmpps_cpr_splink.cpr_splink.interface.score import get_scored_candidates
from integration import random_test_data
from integration.mock_person import MockPerson
from integration.person_factory import PersonFactory
from integration.test_base import IntegrationTestBase


class TestMasterDefendantId(IntegrationTestBase):
    @pytest.fixture(autouse=True)
    async def clean_db(self, db_connection):
        await self.truncate_person_data(db_connection)

    @staticmethod
    async def _score_weight_between(
        primary,
        candidate,
        pg_db_url,
        db_connection,
    ) -> float:
        scores = await get_scored_candidates(primary.match_id, pg_db_url, db_connection)
        matched = next(score for score in scores if score.candidate_match_id == candidate.match_id)
        return matched.candidate_match_weight

    async def test_weight_changes_only_for_equal_master_defendant_id(
        self,
        person_factory: PersonFactory,
        pg_db_url,
        db_connection,
    ):
        shared_pnc = random_test_data.random_pnc()
        person_1 = await person_factory.create_from(MockPerson(pncs=[shared_pnc]))
        person_2 = await person_factory.create_from(MockPerson(pncs=[shared_pnc]))

        baseline_weight = await self._score_weight_between(person_1, person_2, pg_db_url, db_connection)

        equal_id = str(uuid.uuid4())
        person_1.master_defendant_id = equal_id
        person_2.master_defendant_id = equal_id
        await person_factory.update(person_1)
        await person_factory.update(person_2)

        equal_weight = await self._score_weight_between(person_1, person_2, pg_db_url, db_connection)
        assert 19.9 <= equal_weight - baseline_weight <= 21.1

        person_1.master_defendant_id = str(uuid.uuid4())
        person_2.master_defendant_id = str(uuid.uuid4())
        await person_factory.update(person_1)
        await person_factory.update(person_2)

        mismatch_weight = await self._score_weight_between(person_1, person_2, pg_db_url, db_connection)
        assert mismatch_weight == pytest.approx(baseline_weight, abs=1e-6)

        person_1.master_defendant_id = str(uuid.uuid4())
        person_2.master_defendant_id = None
        await person_factory.update(person_1)
        await person_factory.update(person_2)

        one_null_weight = await self._score_weight_between(person_1, person_2, pg_db_url, db_connection)
        assert one_null_weight == pytest.approx(baseline_weight, abs=1e-6)
