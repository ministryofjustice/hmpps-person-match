import time

import pytest
import requests
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_person_match.models.person.person import Person
from hmpps_person_match.routes.jobs.term_frequencies import ROUTE, term_frequency_tables


class TestTermFrequencyGeneration:
    """
    Test term frequency generation
    """

    @staticmethod
    @pytest.fixture(autouse=True, scope="function")
    async def clean_db(db_connection: AsyncSession):
        """
        Before Each
        Delete all records from the database
        """
        await db_connection.execute(text("TRUNCATE TABLE personmatch.person"))
        await db_connection.commit()

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
        await create_person_record(Person(**create_person_data(match_id)))

        response = requests.post(person_match_url + ROUTE)
        assert response.status_code == 200

        for table in term_frequency_tables:
            self.until_asserted(lambda: self.assert_size_of_table(db_connection, table))  # noqa: B023

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

        await create_person_record(Person(**create_person_data(match_id)))

        new_person_data = create_person_data()
        new_person_data["cros"] = ["1234567890"]
        await create_person_record(Person(**new_person_data))

        response = requests.post(person_match_url + ROUTE)
        assert response.status_code == 200

        self.until_asserted(lambda: self.assert_size_of_table(db_connection, cro_tf_table, size=2))

    async def test_term_frequency_unique(
        self,
        person_match_url,
        create_person_record,
        create_person_data,
        match_id,
        db_connection: AsyncSession,
    ):
        """
        Test all term frequency tables are unique values.
        Create 2 sperate people with the same value
        """
        cro_tf_table = "term_frequencies_cro_single"

        cro = "1234567890"

        person_data_1 = create_person_data()
        person_data_1["cros"] = [cro]

        person_data_2 = create_person_data()
        person_data_2["cros"] = [cro]

        await create_person_record(Person(**person_data_1))
        await create_person_record(Person(**person_data_2))

        response = requests.post(person_match_url + ROUTE)
        assert response.status_code == 200

        self.until_asserted(lambda: self.assert_size_of_table(db_connection, cro_tf_table))

    @staticmethod
    async def assert_size_of_table(db_connection, table, size=1):
        result = await db_connection.execute(text(f"SELECT * FROM personmatch.{table}"))
        rows = result.fetchall()
        assert len(rows) == size

    def until_asserted(self, assertion_func, max_retries=5, delay=0.3):
        """
        Repeatedly tries to assert something until it passes or max_retries is reached.

        :param assertion_func: A function that performs the assertion.
        :param max_retries: Maximum number of retries.
        :param delay: Delay (in seconds) between retries.
        :raises AssertionError: If the assertion fails after max_retries attempts.
        """
        for attempt in range(1, max_retries + 1):
            try:
                assertion_func()
                return
            except AssertionError as e:
                if attempt == max_retries:
                    raise e
                else:
                    print(f"Assertion failed, retrying... ({attempt}/{max_retries})")
                    time.sleep(delay)
