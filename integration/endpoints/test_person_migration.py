import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_person_match.routes.person.migration.person_migrate import ROUTE
from integration.client import Client
from integration.mock_person import MockPerson
from integration.test_base import IntegrationTestBase


class TestPersonMigrationEndpoint(IntegrationTestBase):
    """
    Test Person Migration Endpoint.
    """

    @pytest.fixture(autouse=True, scope="function")
    async def before_each(self, db_connection: AsyncSession):
        """
        Before Each
        """
        await self.truncate_person_data(db_connection)

    async def test_batch_clean_and_store_message(self, call_endpoint, db_connection, match_id):
        """
        Test person cleaned and stored on person endpoint
        """
        # Create person
        data = {
            "records": [MockPerson(matchId=match_id).model_dump(by_alias=True)],
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

    async def test_batch_clean_and_store_thousand_records(self, call_endpoint, db_connection):
        """
        Test person cleaned and stored on person endpoint
        """
        data = {
            "records": [MockPerson(matchId=str(uuid.uuid4())).model_dump(by_alias=True) for _ in range(1000)],
        }

        response = call_endpoint("post", ROUTE, json=data, client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 200

        result = await db_connection.execute(text("SELECT count(*) FROM personmatch.person"))
        assert result.scalar_one() == 1000
