from collections.abc import Callable

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_person_match.routes.person.score.person_score import ROUTE
from integration import random_test_data
from integration.client import Client
from integration.mock_person import MockPerson
from integration.person_factory import PersonFactory
from integration.test_base import IntegrationTestBase


class TestPersonProbationMatchEndpoint(IntegrationTestBase):
    """
    Test person probation possible match
    """

    @pytest.fixture(autouse=True, scope="function")
    async def before_each(self, db_connection: AsyncSession) -> None:
        """
        Before Each
        """
        await self.truncate_person_data(db_connection)
        await self.refresh_term_frequencies(db_connection)

    async def test_probation_possible_match_no_matching(self, call_endpoint: Callable) -> None:
        """
        Test person probation possible match handles no matching match id
        """
        response = call_endpoint(
            "get",
            self.build_url(random_test_data.random_match_id()),
            client=Client.HMPPS_PERSON_MATCH,
        )
        assert response.status_code == 200
        assert response.json() == {"match_status": "NO_MATCH"}

    async def test_probation_possible_match_invalid_match_id(self, call_endpoint: Callable) -> None:
        """
        Test person probation possible match handles non uuid match_id
        """
        match_id = "invalid_!!id123"
        response = call_endpoint("get", self.build_url(match_id), client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 404
        assert response.json() == {}

    async def test_probation_possible_match_does_not_return_self(self,
        call_endpoint: Callable, person_factory: PersonFactory) -> None:
        """
        Test person probation possible match doesn't return its own record as part of candidates
        """
        # Create person
        person = await person_factory.create_from(MockPerson())

        # Call probation possible match for person
        response = call_endpoint("get", self.build_url(person.match_id), client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 200
        assert response.json() == {"match_status": "NO_MATCH"}

    async def test_probation_possible_match_returns_match_when_matching_probation_record_exists(self,
        call_endpoint: Callable, person_factory: PersonFactory) -> None:
        """
        Test person probation possible match returns match when a matching probation record exists
        """
        # Create person to match and score
        person_1 = await person_factory.create_from(MockPerson(sourceSystem="COMMON_PLATFORM"))

        # Create different matching person
        await person_factory.create_from(person_1.model_copy(update={"sourceSystem":"DELIUS"}))


        # Call probation possible match for person
        response = call_endpoint("get", self.build_url(person_1.match_id), client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 200
        assert response.json() == {"match_status": "MATCH"}

    async def test_probation_possible_match_returns_no_match_when_matching_record_exists_in_other_source_system(
        self,
        call_endpoint: Callable,
        person_factory: PersonFactory,
    ) -> None:
        """
        Test person probation possible match returns no match when a matching record exists but not in probation
        """
        # Create person to match and score
        person_1 = await person_factory.create_from(MockPerson(sourceSystem="COMMON_PLATFORM"))

        # Create different matching person
        await person_factory.create_from(person_1.model_copy(update={"sourceSystem":"NOMIS",
                                                                     "match_id": random_test_data.random_match_id()}))

        # Create different matching person
        await person_factory.create_from(person_1.model_copy(update={"sourceSystem":"LIBRA",
                                                                     "match_id": random_test_data.random_match_id()}))

        # Create different matching person
        await person_factory.create_from(person_1.model_copy(update={"sourceSystem":"COMMON_PLATFORM",
                                                                     "match_id": random_test_data.random_match_id()}))


        # Call probation possible match for person
        response = call_endpoint("get", self.build_url(person_1.match_id), client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 200
        assert response.json() == {"match_status": "NO_MATCH"}

    async def test_probation_possible_match_returns_possible_match_when_possibly_matching_record_exists_in_probation(
        self,
        call_endpoint: Callable,
        person_factory: PersonFactory,
    ) -> None:
        """
        Test person probation possible match returns possible match when a record exists
        which matches with a probability between negative 10 and positive 20
        """
        pnc = random_test_data.random_pnc()

        # Create person to match and score
        person_1 = await person_factory.create_from(MockPerson(pncs=[pnc]))

        # Create different person with different details
        await person_factory.create_from(MockPerson(pncs=[pnc]))

        # Call probation possible match for person
        response = call_endpoint("get", self.build_url(person_1.match_id), client=Client.HMPPS_PERSON_MATCH)

        assert response.status_code == 200
        assert response.json() == {"match_status": "POSSIBLE_MATCH"}

    @staticmethod
    def build_url(match_id: str) -> str:
        return ROUTE.format(match_id=match_id)
