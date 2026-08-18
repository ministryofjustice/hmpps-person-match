from collections.abc import Callable

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_person_match.routes.person.search.person_search_by_match_id import ROUTE
from integration import random_test_data
from integration.client import Client
from integration.mock_person import MockPerson
from integration.person_factory import PersonFactory
from integration.test_base import IntegrationTestBase


class TestPersonSearchEndpoint(IntegrationTestBase):
    """
    Test person search by match id
    """

    @pytest.fixture(autouse=True, scope="function")
    async def before_each(self, db_connection: AsyncSession) -> None:
        """
        Before Each
        """
        await self.truncate_person_data(db_connection)
        await self.refresh_term_frequencies(db_connection)

    async def test_search_no_matching(self, call_endpoint: Callable) -> None:
        """
        Test person search handles no matching match id
        """
        response = call_endpoint(
            "get",
            self._build_search_url(random_test_data.random_match_id()),
            client=Client.HMPPS_PERSON_MATCH,
        )
        assert response.status_code == 404
        assert response.json() == {}

    async def test_search_invalid_match_id(self, call_endpoint: Callable) -> None:
        """
        Test person search handles non uuid match_id
        """
        match_id = "invalid_!!id123"
        response = call_endpoint("get", self._build_search_url(match_id), client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 404
        assert response.json() == {}

    async def test_search_does_not_return_self(self, call_endpoint: Callable, person_factory: PersonFactory) -> None:
        """
        Test person search doesn't return its own record as part of candidates
        """
        # Create person
        person = await person_factory.create_from(MockPerson())

        # Call score for person
        response = call_endpoint("get", self._build_search_url(person.match_id), client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 200
        assert response.json() == []

    async def test_search_returns_candidates(self, call_endpoint: Callable, person_factory: PersonFactory) -> None:
        """
        Test person search returns matching candidates
        """
        # Create person to match and score
        person_1 = await person_factory.create_from(MockPerson())

        # Create different matching person
        person_2 = await person_factory.create_from(person_1)

        # Create different matching person
        person_3 = await person_factory.create_from(person_1)

        # Call score for person
        response = call_endpoint("get", self._build_search_url(person_1.match_id), client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 200
        assert len(response.json()) == 2
        candidates_id = [candidate["candidate_match_id"] for candidate in response.json()]
        assert person_2.match_id in candidates_id
        assert person_3.match_id in candidates_id

    async def test_search_returns_joining_flag_for_candidate(
        self,
        call_endpoint: Callable,
        person_factory: PersonFactory,
    ) -> None:
        """
        Test person search returns joining flag for candidate
        """
        # Create person to match and score
        person_1 = await person_factory.create_from(MockPerson())

        # Create different person with same details
        person_2 = await person_factory.create_from(person_1)

        # Call score for person
        response = call_endpoint("get", self._build_search_url(person_1.match_id), client=Client.HMPPS_PERSON_MATCH)

        assert response.status_code == 200
        assert len(response.json()) == 1

        matched_candidate = response.json()[0]
        assert matched_candidate["candidate_match_id"] == person_2.match_id
        assert matched_candidate["candidate_should_join"]

    async def test_search_returns_fracture_flag_for_candidate(
        self,
        call_endpoint: Callable,
        person_factory: PersonFactory,
    ) -> None:
        """
        Test person search returns fracture flag for candidate
        """
        pnc = random_test_data.random_pnc()

        # Create person to match and score
        person_1 = await person_factory.create_from(MockPerson(pncs=[pnc]))

        # Create different person with different details
        person_2 = await person_factory.create_from(MockPerson(pncs=[pnc]))

        # Call score for person
        response = call_endpoint("get", self._build_search_url(person_1.match_id), client=Client.HMPPS_PERSON_MATCH)

        assert response.status_code == 200
        assert len(response.json()) == 1

        matched_candidate = response.json()[0]
        assert matched_candidate["candidate_match_id"] == person_2.match_id
        assert matched_candidate["candidate_should_fracture"]

    async def test_search_return_mutually_excluded_candidate(
        self,
        call_endpoint: Callable,
        person_factory: PersonFactory,
    ) -> None:
        """
        Test person search returns mutually exclusive candidate
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
        response = call_endpoint("get", self._build_search_url(person_1.match_id), client=Client.HMPPS_PERSON_MATCH)

        assert response.status_code == 200
        assert len(response.json()) == 2

        candidates_id = [candidate["candidate_match_id"] for candidate in response.json()]
        assert person_2.match_id in candidates_id
        assert person_3.match_id in candidates_id

    async def test_search_flags_twins(
        self,
        call_endpoint: Callable,
        person_factory: PersonFactory,
    ) -> None:
        person_data = MockPerson()
        person_data.master_defendant_id = None
        original_name = person_data.first_name
        person_1 = await person_factory.create_from(person_data)

        # Create another record of same person
        person_2 = await person_factory.create_from(person_data)

        # Create a 'twin'
        person_data.first_name = random_test_data.random_name()
        person_data.first_name_aliases = []
        person_data.pncs = []
        person_data.cros = []
        person_3 = await person_factory.create_from(person_data)
        assert person_data.first_name != original_name

        # Call score for person
        response = call_endpoint("get", self._build_search_url(person_1.match_id), client=Client.HMPPS_PERSON_MATCH)

        assert response.status_code == 200
        assert len(response.json()) == 2

        candidates_id = [candidate["candidate_match_id"] for candidate in response.json()]
        assert person_2.match_id in candidates_id
        assert person_3.match_id in candidates_id
        person_2_response_row = [
            candidate for candidate in response.json() if candidate["candidate_match_id"] == person_2.match_id
        ]
        person_3_response_row = [
            candidate for candidate in response.json() if candidate["candidate_match_id"] == person_3.match_id
        ]

        assert not person_2_response_row[0]["candidate_is_possible_twin"]
        assert person_2_response_row[0]["candidate_match_weight"] > 24
        assert person_3_response_row[0]["candidate_is_possible_twin"]
        assert person_3_response_row[0]["candidate_match_weight"] < 0

    @staticmethod
    def _build_search_url(match_id: str) -> str:
        return ROUTE.format(match_id=match_id)
