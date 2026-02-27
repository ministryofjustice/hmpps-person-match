from collections.abc import Callable

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_person_match.routes.person.score.person_best_match import ROUTE
from integration import random_test_data
from integration.client import Client
from integration.mock_person import MockPerson
from integration.person_factory import PersonFactory
from integration.test_base import IntegrationTestBase


class TestPersonBestMatchEndpoint(IntegrationTestBase):
    """
    Test person best match
    """

    @pytest.fixture(autouse=True, scope="function")
    async def before_each(self, db_connection: AsyncSession) -> None:
        """
        Before Each
        """
        await self.truncate_person_data(db_connection)
        await self.refresh_term_frequencies(db_connection)

    async def test_best_match_no_matching(self, call_endpoint: Callable) -> None:
        """
        Test person best match handles no matching match id
        """
        response = call_endpoint(
            "get",
            self.build_url(random_test_data.random_match_id()),
            client=Client.HMPPS_PERSON_MATCH,
        )
        assert response.status_code == 404
        assert response.json() == {}

    async def test_best_match_invalid_match_id(self, call_endpoint: Callable) -> None:
        """
        Test person best match handles non uuid match_id
        """
        match_id = "invalid_!!id123"
        response = call_endpoint("get", self.build_url(match_id), client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 404
        assert response.json() == {}

    async def test_best_match_does_not_match_self(self,
        call_endpoint: Callable, person_factory: PersonFactory) -> None:
        """
        Test person best match doesn't match itsself record
        """
        # Create person
        person = await person_factory.create_from(MockPerson())

        # Call best match for person
        response = call_endpoint("get", self.build_url(person.match_id), client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 200
        assert response.json() == {"match_status": "NO_MATCH"}

    async def test_best_match_returns_match_when_matching_record_exists(self,
        call_endpoint: Callable, person_factory: PersonFactory) -> None:
        """
        Test person best match returns match when a matching record exists
        in supplied source system
        """
        # Create person to match and score
        person_1 = await person_factory.create_from(MockPerson(sourceSystem="COMMON_PLATFORM"))

        # Create different matching person
        await person_factory.create_from(person_1.model_copy(update={"source_system":"DELIUS"}))


        # Call best match for person
        response = call_endpoint("get", self.build_url(person_1.match_id), client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 200
        assert response.json() == {"match_status": "MATCH"}

    async def test_best_match_returns_no_match_when_matching_record_exists_in_other_source_system(
        self,
        call_endpoint: Callable,
        person_factory: PersonFactory,
    ) -> None:
        """
        Test person probatiobestn match returns no match when a matching record exists
        but not in supplied source system
        """
        # Create person to match and score
        person_1 = await person_factory.create_from(MockPerson(sourceSystem="COMMON_PLATFORM"))

        # Create different matching person
        await person_factory.create_from(person_1.model_copy(update={"source_system":"NOMIS"}))

        # Create different matching person
        await person_factory.create_from(person_1.model_copy(update={"source_system":"LIBRA"}))

        # Create different matching person
        await person_factory.create_from(person_1.model_copy(update={"source_system":"COMMON_PLATFORM"}))


        # Call best match for person
        response = call_endpoint("get", self.build_url(person_1.match_id), client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 200
        assert response.json() == {"match_status": "NO_MATCH"}

    async def test_best_match_returns_possible_match_when_possibly_matching_record_exists_in_supplied_source_system(
        self,
        call_endpoint: Callable,
        person_factory: PersonFactory,
    ) -> None:
        """
        Test person best match returns possible match when a record exists
        which matches with a probability between negative 10 and positive 20
        """
        pnc = random_test_data.random_pnc()
        date_of_birth = random_test_data.random_date()
        first_name = random_test_data.random_name()
        last_name = random_test_data.random_name()

        # Create person to match and score
        person_1 = await person_factory.create_from(MockPerson(
            pncs=[pnc], dateOfBirth=date_of_birth,
            firstName= first_name, lastName =last_name ))

        # Create probation person as a possible match
        await person_factory.create_from(MockPerson(
            sourceSystem="DELIUS", dateOfBirth=date_of_birth,
            firstName = first_name,  lastName = last_name))

        # Call best match for person
        response = call_endpoint("get", self.build_url(person_1.match_id), client=Client.HMPPS_PERSON_MATCH)

        assert response.status_code == 200
        assert response.json() == {"match_status": "POSSIBLE_MATCH"}

    @staticmethod
    def build_url(match_id: str, source_system: str = "DELIUS") -> str:
        return ROUTE.format(match_id=match_id, source_system = source_system)
