import uuid

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_person_match.models.person.person import Person
from hmpps_person_match.routes.person.score.person_score import ROUTE
from integration.client import Client
from integration.test_base import IntegrationTestBase


class TestPersonScoreEndpoint(IntegrationTestBase):
    """
    Test person score
    """

    @pytest.fixture(autouse=True, scope="function")
    async def before_each(self, db_connection: AsyncSession, person_match_url: str):
        """
        Before Each
        """
        await self.truncate_person_data(db_connection)
        await self.refresh_term_frequencies_assert_empty(person_match_url, db_connection)

    async def test_score_no_matching(self, call_endpoint, match_id):
        """
        Test person score handles no matching match id
        """
        response = call_endpoint("get", self._build_score_url(match_id), client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 404
        assert response.json() == {}

    async def test_score_invalid_match_id(self, call_endpoint, match_id):
        """
        Test person score handles non uuid match_id
        """
        match_id = "invalid_!!id123"
        response = call_endpoint("get", self._build_score_url(match_id), client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 404
        assert response.json() == {}

    async def test_score_does_not_return_self(self, call_endpoint, match_id, create_person_data):
        """
        Test person score doesn't return its own record as part of candidates
        """
        # Create person
        data = create_person_data(match_id)
        response = call_endpoint("post", "/person", json=data, client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 200

        # Call score for person
        response = call_endpoint("get", self._build_score_url(match_id), client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 200
        assert response.json() == []

    async def test_score_returns_candidates(self, call_endpoint, match_id, create_person_data, create_person_record):
        """
        Test person cleaned and stored on person endpoint
        """
        # Create person to match and score
        await create_person_record(Person(**create_person_data(match_id)))

        # Create different person
        matching_person_id_1 = str(uuid.uuid4())
        await create_person_record(Person(**create_person_data(matching_person_id_1)))

        # Create different person
        matching_person_id_2 = str(uuid.uuid4())
        await create_person_record(Person(**create_person_data(matching_person_id_2)))

        # Call score for person
        response = call_endpoint("get", self._build_score_url(match_id), client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 200
        assert len(response.json()) == 2
        candidates_id = [candidate["candidate_match_id"] for candidate in response.json()]
        assert matching_person_id_1 in candidates_id
        assert matching_person_id_2 in candidates_id

    @staticmethod
    def _build_score_url(match_id: str):
        return ROUTE.format(match_id=match_id)
