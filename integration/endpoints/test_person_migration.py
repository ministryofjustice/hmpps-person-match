import uuid

import pytest

from hmpps_person_match.routes.person.migration.person_migrate import ROUTE
from integration.client import Client


class TestPersonMigrationEndpoint:
    """
    Test Person Migration Endpoint.
    """

    @staticmethod
    @pytest.fixture(autouse=True, scope="function")
    async def clean_db(db):
        """
        Before Each
        Delete all records from the database
        """
        await db.execute("TRUNCATE TABLE personmatch.person")

    async def test_batch_clean_and_store_message(self, call_endpoint, db, create_person_data):
        """
        Test person cleaned and stored on person endpoint
        """
        data = {
            "records": [create_person_data()],
        }

        response = call_endpoint("post", ROUTE, json=data, client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 200

        count = await db.fetchval("SELECT count(*) FROM personmatch.person")
        assert count == 1

    async def test_batch_clean_errors(self, call_endpoint, db, create_person_data):
        """
        Test person cleaned and stored on person endpoint
        """
        data = {
            "records": ["invalid_data"],
        }

        response = call_endpoint("post", ROUTE, json=data, client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 400

    async def test_batch_clean_and_store_thousand_records(self, call_endpoint, db, create_person_data):
        """
        Test person cleaned and stored on person endpoint
        """
        data = {
            "records": [create_person_data(uuid.uuid4()) for _ in range(1000)],
        }

        response = call_endpoint("post", ROUTE, json=data, client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 200

        count = await db.fetchval("SELECT count(*) FROM personmatch.person")
        assert count == 1000
