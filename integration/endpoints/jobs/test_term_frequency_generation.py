import uuid

import pytest
import requests
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_person_match.routes.jobs.term_frequencies import ROUTE, term_frequency_tables
from integration import random_test_data
from integration.mock_person import MockPerson
from integration.test_base import IntegrationTestBase


class TestTermFrequencyGeneration(IntegrationTestBase):
    """
    Test term frequency generation
    """

    @pytest.fixture(autouse=True, scope="function")
    async def clean_db(self, db_connection: AsyncSession):
        """
        Before Each
        Delete all records from the database
        """
        await self.truncate_person_data(db_connection)

    async def test_term_frequency_generated(
        self,
        person_match_url,
        create_person_record,
        create_person_data,
        match_id,
        db_connection: AsyncSession,
    ):
        """
        Test all term frequency tables are generated
        """
        await create_person_record(MockPerson(matchId=match_id))

        response = requests.post(person_match_url + ROUTE)
        assert response.status_code == 200

        for table in term_frequency_tables:
            await self.until_asserted(lambda: self.assert_size_of_table(db_connection, table, size=1))  # noqa: B023

    async def test_term_frequency_refreshed(
        self,
        person_match_url,
        create_person_record,
        create_person_data,
        match_id,
        db_connection: AsyncSession,
    ):
        """
        Test all term frequency tables are refreshed, with new value
        """
        cro_tf_table = "term_frequencies_cro_single"

        await create_person_record(MockPerson(matchId=match_id))

        await create_person_record(MockPerson(matchId=str(uuid.uuid4()), cros=[random_test_data.random_cro()]))

        response = requests.post(person_match_url + ROUTE)
        assert response.status_code == 200

        await self.until_asserted(lambda: self.assert_size_of_table(db_connection, cro_tf_table, size=2))

    async def test_term_frequency_unique(
        self,
        person_match_url,
        create_person_record,
        db_connection: AsyncSession,
    ):
        """
        Test all term frequency tables are unique values.
        Create 2 sperate people with the same value
        """
        cro_tf_table = "term_frequencies_cro_single"

        cro = random_test_data.random_cro()

        person_data_1 = MockPerson(matchId=str(uuid.uuid4()), cros=[cro])
        person_data_2 = MockPerson(matchId=str(uuid.uuid4()), cros=[cro])

        await create_person_record(person_data_1)
        await create_person_record(person_data_2)

        response = requests.post(person_match_url + ROUTE)
        assert response.status_code == 200

        await self.until_asserted(lambda: self.assert_size_of_table(db_connection, cro_tf_table, size=1))
