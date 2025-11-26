import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from integration.mock_person import MockPerson
from integration.person_factory import PersonFactory
from integration.test_base import IntegrationTestBase


class TestTFs(IntegrationTestBase):
    """
    Test term frequency tables
    """

    @pytest.fixture(autouse=True, scope="function")
    async def clean_db(self, db_connection: AsyncSession) -> None:
        """
        Before Each
        Delete all records from the database
        """
        await self.truncate_person_data(db_connection)

    async def test_term_frequencies_simple(self, person_factory: PersonFactory, db_connection: AsyncSession) -> None:
        """
        Test term frequencies for a simple column
        """
        await person_factory.create_from(MockPerson(firstName="Andy"))
        await person_factory.create_from(MockPerson(firstName="Andy"))
        await person_factory.create_from(MockPerson(firstName="Andy"))
        await person_factory.create_from(MockPerson(firstName="Henry"))

        await db_connection.execute(
            text("REFRESH MATERIALIZED VIEW CONCURRENTLY personmatch.term_frequencies_name_1_std;"),
        )
        await db_connection.commit()

        tf_name_andy = await db_connection.execute(
            text("SELECT tf_name_1_std FROM personmatch.term_frequencies_name_1_std WHERE name_1_std = 'ANDY'"),
        )
        tf_name_andy_row = tf_name_andy.fetchone()
        assert tf_name_andy_row is not None and tf_name_andy_row[0] == 0.75

        tf_name_henry = await db_connection.execute(
            text("SELECT tf_name_1_std FROM personmatch.term_frequencies_name_1_std WHERE name_1_std = 'HENRY'"),
        )
        tf_name_henry_row = tf_name_henry.fetchone()
        assert tf_name_henry_row is not None and tf_name_henry_row[0] == 0.25

    async def test_term_frequencies_postcode(self, person_factory: PersonFactory, db_connection: AsyncSession) -> None:
        """
        Test term frequencies for an array column
        """
        await person_factory.create_from(MockPerson(postcodes=["AB1 1ZY"]))
        await person_factory.create_from(MockPerson(postcodes=["CD2 2XW"]))
        await person_factory.create_from(MockPerson(postcodes=["AB1 1ZY", "CD2 2XW"]))
        await person_factory.create_from(MockPerson(postcodes=["AB1 1ZY", "CD2 2XW"]))
        await person_factory.create_from(MockPerson(postcodes=["AB1 1ZY", "EF3 3VU"]))

        await db_connection.execute(
            text("REFRESH MATERIALIZED VIEW CONCURRENTLY personmatch.term_frequencies_postcode;"),
        )
        await db_connection.commit()

        tf_postcode_ab = await db_connection.execute(
            text("SELECT tf_postcode FROM personmatch.term_frequencies_postcode WHERE postcode = 'AB11ZY'"),
        )
        tf_postcode_ab_row = tf_postcode_ab.fetchone()
        assert tf_postcode_ab_row is not None and tf_postcode_ab_row[0] == 0.5

        tf_postcode_cd = await db_connection.execute(
            text("SELECT tf_postcode FROM personmatch.term_frequencies_postcode WHERE postcode = 'CD22XW'"),
        )
        tf_postcode_cd_row = tf_postcode_cd.fetchone()
        assert tf_postcode_cd_row is not None and tf_postcode_cd_row[0] == 0.375

        tf_postcode_ef = await db_connection.execute(
            text("SELECT tf_postcode FROM personmatch.term_frequencies_postcode WHERE postcode = 'EF33VU'"),
        )
        tf_postcode_ef_row = tf_postcode_ef.fetchone()
        assert tf_postcode_ef_row is not None and tf_postcode_ef_row[0] == 0.125
