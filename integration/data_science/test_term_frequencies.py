import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_person_match.models.person.person import Person
from integration.test_base import IntegrationTestBase


class TestTFs(IntegrationTestBase):
    """
    Test term frequency tables
    """

    def create_person_data(self, first_name: str = "Henry", postcodes: list[str] = None):
        return {
            "matchId": str(uuid.uuid4()),
            "sourceSystem": "DELIUS",
            "firstName": first_name,
            "middleNames": "Ahmed",
            "lastName": "Junaed",
            "crn": "1234",
            "prisonNumber": "A1234BC",
            "dateOfBirth": "1992-03-02",
            "firstNameAliases": ["Henry"],
            "lastNameAliases": ["Junaed"],
            "dateOfBirthAliases": ["1992-01-01"],
            "postcodes": postcodes if postcodes is not None else ["B10 1EJ"],
            "cros": ["4444566"],
            "pncs": ["22224555"],
            "sentenceDates": ["2001-03-01"],
        }

    @pytest.fixture(autouse=True, scope="function")
    async def clean_db(self, db_connection: AsyncSession):
        """
        Before Each
        Delete all records from the database
        """
        await self.truncate_person_data(db_connection)

    async def test_term_frequencies_simple(self, create_person_record, db_connection):
        """
        Test term frequncies for a simple column
        """
        await create_person_record(Person(**self.create_person_data(first_name="Andy")))
        await create_person_record(Person(**self.create_person_data(first_name="Andy")))
        await create_person_record(Person(**self.create_person_data(first_name="Andy")))
        await create_person_record(Person(**self.create_person_data(first_name="Henry")))

        await db_connection.execute(
            text("REFRESH MATERIALIZED VIEW CONCURRENTLY personmatch.term_frequencies_name_1_std;"),
        )
        await db_connection.commit()

        tf_name_andy = await db_connection.execute(
            text("SELECT tf_name_1_std FROM personmatch.term_frequencies_name_1_std WHERE name_1_std = 'ANDY'"),
        )
        assert tf_name_andy.fetchone()[0] == 0.75

        tf_name_henry = await db_connection.execute(
            text("SELECT tf_name_1_std FROM personmatch.term_frequencies_name_1_std WHERE name_1_std = 'HENRY'"),
        )
        assert tf_name_henry.fetchone()[0] == 0.25

    async def test_term_frequencies_postcode(self, create_person_record, db_connection):
        """
        Test term frequencies for an array column
        """
        await create_person_record(Person(**self.create_person_data(postcodes=["AB1 1ZY"])))
        await create_person_record(Person(**self.create_person_data(postcodes=["CD2 2XW"])))
        await create_person_record(Person(**self.create_person_data(postcodes=["AB1 1ZY", "CD2 2XW"])))
        await create_person_record(Person(**self.create_person_data(postcodes=["AB1 1ZY", "CD2 2XW"])))
        await create_person_record(Person(**self.create_person_data(postcodes=["AB1 1ZY", "EF3 3VU"])))

        await db_connection.execute(
            text("REFRESH MATERIALIZED VIEW CONCURRENTLY personmatch.term_frequencies_postcode;"),
        )
        await db_connection.commit()

        tf_postcode_ab = await db_connection.execute(
            text("SELECT tf_postcode FROM personmatch.term_frequencies_postcode WHERE postcode = 'AB11ZY'"),
        )
        assert tf_postcode_ab.fetchone()[0] == 0.5

        tf_postcode_cd = await db_connection.execute(
            text("SELECT tf_postcode FROM personmatch.term_frequencies_postcode WHERE postcode = 'CD22XW'"),
        )
        assert tf_postcode_cd.fetchone()[0] == 0.375

        tf_postcode_ef = await db_connection.execute(
            text("SELECT tf_postcode FROM personmatch.term_frequencies_postcode WHERE postcode = 'EF33VU'"),
        )
        assert tf_postcode_ef.fetchone()[0] == 0.125
