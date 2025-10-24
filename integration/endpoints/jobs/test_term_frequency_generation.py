import pytest
import requests
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_person_match.routes.jobs.term_frequencies import ROUTE, TERM_FREQUENCY_TABLES
from integration import random_test_data
from integration.mock_person import MockPerson
from integration.person_factory import PersonFactory
from integration.test_base import IntegrationTestBase


class TestTermFrequencyGeneration(IntegrationTestBase):
    """
    Test term frequency generation
    """

    @pytest.fixture(autouse=True, scope="function")
    async def clean_db(self, db_connection: AsyncSession) -> None:
        """
        Before Each
        Delete all records from the database
        """
        await self.truncate_person_data(db_connection)

    async def test_term_frequency_generated(
        self,
        person_match_url: str,
        person_factory: PersonFactory,
        db_connection: AsyncSession,
    ) -> None:
        """
        Test all term frequency tables are generated
        """
        await person_factory.create_from(MockPerson())

        response = requests.post(person_match_url + ROUTE)  # noqa: ASYNC210
        assert response.status_code == 200

        for table in TERM_FREQUENCY_TABLES:
            await self.until_asserted(lambda: self.assert_size_of_table(db_connection, table, size=1))  # noqa: B023

    async def test_term_frequency_refreshed(
        self,
        person_match_url: str,
        person_factory: PersonFactory,
        db_connection: AsyncSession,
    ) -> None:
        """
        Test all term frequency tables are refreshed, with new value
        """
        cro_tf_table = "term_frequencies_cro_single"

        await person_factory.create_from(MockPerson())

        await person_factory.create_from(MockPerson(cros=[random_test_data.random_cro()]))

        response = requests.post(person_match_url + ROUTE)  # noqa: ASYNC210
        assert response.status_code == 200

        await self.until_asserted(lambda: self.assert_size_of_table(db_connection, cro_tf_table, size=2))

    async def test_term_frequency_unique(
        self,
        person_match_url: str,
        person_factory: PersonFactory,
        db_connection: AsyncSession,
    ) -> None:
        """
        Test all term frequency tables are unique values.
        Create 2 sperate people with the same value
        """
        cro_tf_table = "term_frequencies_cro_single"

        cro = random_test_data.random_cro()

        await person_factory.create_from(MockPerson(cros=[cro]))
        await person_factory.create_from(MockPerson(cros=[cro]))

        response = requests.post(person_match_url + ROUTE)  # noqa: ASYNC210
        assert response.status_code == 200

        await self.until_asserted(lambda: self.assert_size_of_table(db_connection, cro_tf_table, size=1))
