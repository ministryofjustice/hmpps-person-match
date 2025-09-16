import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_person_match.routes.person.score.person_score import ROUTE
from integration import random_test_data
from integration.client import Client
from integration.mock_person import MockPerson
from integration.person_factory import PersonFactory
from integration.test_base import IntegrationTestBase


class TestPersonScoreEndpoint(IntegrationTestBase):
    """
    Test person score
    """

    @pytest.fixture(autouse=True, scope="function")
    async def before_each(self, db_connection: AsyncSession):
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

    async def test_score_does_not_return_self(self, call_endpoint, person_factory: PersonFactory):
        """
        Test person score doesn't return its own record as part of candidates
        """
        # Create person
        person = await person_factory.create_from(MockPerson())

        # Call score for person
        response = call_endpoint("get", self._build_score_url(person.match_id), client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 200
        assert response.json() == []

    async def test_score_returns_candidates(self, call_endpoint, person_factory: PersonFactory):
        """
        Test person cleaned and stored on person endpoint
        """
        # Create person to match and score
        person_1 = await person_factory.create_from(MockPerson())

        # Create different matching person
        person_2 = await person_factory.create_from(person_1)

        # Create different matching person
        person_3 = await person_factory.create_from(person_1)

        # Call score for person
        response = call_endpoint("get", self._build_score_url(person_1.match_id), client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 200
        assert len(response.json()) == 2
        candidates_id = [candidate["candidate_match_id"] for candidate in response.json()]
        assert person_2.match_id in candidates_id
        assert person_3.match_id in candidates_id

    async def test_returns_joining_flag_for_candidate(self, call_endpoint, person_factory: PersonFactory):
        """
        Test person has joining flag when it passes the threshold
        """
        # Create person to match and score
        person_1 = await person_factory.create_from(MockPerson())

        # Create different person with same details
        person_2 = await person_factory.create_from(person_1)

        # Call score for person
        response = call_endpoint("get", self._build_score_url(person_1.match_id), client=Client.HMPPS_PERSON_MATCH)

        assert response.status_code == 200
        assert len(response.json()) == 1

        matched_candidate = response.json()[0]
        assert matched_candidate["candidate_match_id"] == person_2.match_id
        assert matched_candidate["candidate_should_join"]

    async def test_returns_fracture_flag_for_candidate(self, call_endpoint, person_factory: PersonFactory):
        """
        Test person has fracture flag when it passes the threshold
        """
        pnc = random_test_data.random_pnc()

        # Create person to match and score
        person_1 = await person_factory.create_from(MockPerson(pncs=[pnc]))

        # Create different person with different details
        person_2 = await person_factory.create_from(MockPerson(pncs=[pnc]))

        # Call score for person
        response = call_endpoint("get", self._build_score_url(person_1.match_id), client=Client.HMPPS_PERSON_MATCH)

        assert response.status_code == 200
        assert len(response.json()) == 1

        matched_candidate = response.json()[0]
        assert matched_candidate["candidate_match_id"] == person_2.match_id
        assert matched_candidate["candidate_should_fracture"]

    async def test_score_return_mutually_excluded_candidate(self, call_endpoint, person_factory: PersonFactory):
        """
        Test score can handle mutually exclusive candidate
        """
        # Create person to match and score
        scope = self.new_scope()
        person_data = MockPerson()
        person_1 = await person_factory.create_from(person_data)

        # Create different person
        person_data.override_marker = self.new_override_marker()
        person_data.override_scopes = [scope]
        person_2 = await person_factory.create_from(person_data)

        # Create different matching person
        person_data.override_marker = self.new_override_marker()
        person_data.override_scopes = [scope]
        person_3 = await person_factory.create_from(person_data)

        # Call score for person
        response = call_endpoint("get", self._build_score_url(person_1.match_id), client=Client.HMPPS_PERSON_MATCH)

        assert response.status_code == 200
        assert len(response.json()) == 2

        candidates_id = [candidate["candidate_match_id"] for candidate in response.json()]
        assert person_2.match_id in candidates_id
        assert person_3.match_id in candidates_id

    @staticmethod
    def _build_score_url(match_id: str):
        return ROUTE.format(match_id=match_id)
