import uuid

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_person_match.routes.person.score.person_score import ROUTE
from integration import random_test_data
from integration.client import Client
from integration.mock_person import MockPerson
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
        await self.refresh_term_frequencies(db_connection)

    async def test_score_no_matching(self, call_endpoint):
        """
        Test person score handles no matching match id
        """
        response = call_endpoint(
            "get",
            self._build_score_url(random_test_data.random_match_id()),
            client=Client.HMPPS_PERSON_MATCH,
        )
        assert response.status_code == 404
        assert response.json() == {}

    async def test_score_invalid_match_id(self, call_endpoint):
        """
        Test person score handles non uuid match_id
        """
        match_id = "invalid_!!id123"
        response = call_endpoint("get", self._build_score_url(match_id), client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 404
        assert response.json() == {}

    async def test_score_does_not_return_self(self, call_endpoint, create_person_record):
        """
        Test person score doesn't return its own record as part of candidates
        """
        # Create person
        person_data = MockPerson(matchId=random_test_data.random_match_id())
        await create_person_record(person_data)
        response = call_endpoint(
            "post",
            "/person",
            json=person_data.model_dump(by_alias=True),
            client=Client.HMPPS_PERSON_MATCH,
        )
        assert response.status_code == 200

        # Call score for person
        response = call_endpoint("get", self._build_score_url(person_data.match_id), client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 200
        assert response.json() == []

    async def test_score_returns_candidates(self, call_endpoint, create_person_record):
        """
        Test person cleaned and stored on person endpoint
        """
        # Create person to match and score
        person_data = MockPerson(matchId=random_test_data.random_match_id())
        await create_person_record(person_data)

        # Create different person
        matching_person_id_1 = random_test_data.random_match_id()
        person_data.match_id = matching_person_id_1
        person_data.source_system_id = random_test_data.random_source_system_id()
        await create_person_record(person_data)

        # Create different person
        matching_person_id_2 = random_test_data.random_match_id()
        person_data.match_id = matching_person_id_2
        person_data.source_system_id = random_test_data.random_source_system_id()
        await create_person_record(person_data)

        # Call score for person
        response = call_endpoint("get", self._build_score_url(person_data.match_id), client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 200
        assert len(response.json()) == 2
        candidates_id = [candidate["candidate_match_id"] for candidate in response.json()]
        assert matching_person_id_1 in candidates_id
        assert matching_person_id_2 in candidates_id

    async def test_returns_joining_flag_for_candidate(self, call_endpoint, create_person_record):
        """
        Test person has joining flag when it passes the threshold
        """
        # Create person to match and score
        person_data = MockPerson(matchId=random_test_data.random_match_id())
        await create_person_record(person_data)

        # Create different person with same details
        matched_person_id = random_test_data.random_match_id()
        person_data.match_id = matched_person_id
        person_data.source_system_id = random_test_data.random_source_system_id()
        await create_person_record(person_data)

        # Call score for person
        response = call_endpoint("get", self._build_score_url(person_data), client=Client.HMPPS_PERSON_MATCH)

        assert response.status_code == 200
        assert len(response.json()) == 1

        matched_candidate = response.json()[0]
        assert matched_candidate["candidate_match_id"] == matched_person_id
        assert matched_candidate["candidate_should_join"]

    async def test_returns_fracture_flag_for_candidate(self, call_endpoint, match_id, create_person_record):
        """
        Test person has fracture flag when it passes the threshold
        """
        pnc = random_test_data.random_pnc()

        # Create person to match and score
        person_data = MockPerson(matchId=match_id)
        person_data.pncs = [pnc]
        await create_person_record(person_data)

        # Create different person with different details
        matched_person_id = str(uuid.uuid4())
        matched_person_data = MockPerson(matchId=matched_person_id)
        matched_person_data.pncs = [pnc]
        await create_person_record(matched_person_data)

        # Call score for person
        response = call_endpoint("get", self._build_score_url(match_id), client=Client.HMPPS_PERSON_MATCH)

        assert response.status_code == 200
        assert len(response.json()) == 1

        matched_candidate = response.json()[0]
        assert matched_candidate["candidate_match_id"] == matched_person_id
        assert matched_candidate["candidate_should_fracture"]

    @staticmethod
    def _build_score_url(match_id: str):
        return ROUTE.format(match_id=match_id)
