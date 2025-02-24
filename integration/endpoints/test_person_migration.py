import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_person_match.routes.person.migration.person_migrate import ROUTE
from integration.client import Client


class TestPersonMigrationEndpoint:
    """
    Test Person Migration Endpoint.
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

    async def test_batch_clean_and_store_message(self, call_endpoint, db_connection, create_person_data):
        """
        Test person cleaned and stored on person endpoint
        """
        data = {
            "records": [create_person_data()],
        }

        response = call_endpoint("post", ROUTE, json=data, client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 200

        result = await db_connection.execute(text("SELECT count(*) FROM personmatch.person"))
        assert result.scalar_one() == 1

    async def test_batch_clean_errors(self, call_endpoint):
        """
        Test person cleaned and stored on person endpoint
        """
        data = {
            "records": ["invalid_data"],
        }
        response = call_endpoint("post", ROUTE, json=data, client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 400

    async def test_batch_clean_and_store_thousand_records(self, call_endpoint, db_connection, create_person_data):
        """
        Test person cleaned and stored on person endpoint
        """
        data = {
            "records": [create_person_data(uuid.uuid4()) for _ in range(1000)],
        }

        response = call_endpoint("post", ROUTE, json=data, client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 200

        result = await db_connection.execute(text("SELECT count(*) FROM personmatch.person"))
        assert result.scalar_one() == 1000
