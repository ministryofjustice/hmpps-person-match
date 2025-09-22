import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_person_match.routes.cluster.visualise_cluster import ROUTE
from integration import random_test_data
from integration.client import Client
from integration.conftest import PersonFactory
from integration.mock_person import MockPerson
from integration.test_base import IntegrationTestBase


class TestVisualiseClusterEndpoint(IntegrationTestBase):
    @pytest.fixture(autouse=True, scope="function")
    async def before_each(self, db_connection: AsyncSession):
        await self.truncate_person_data(db_connection)
        await self.refresh_term_frequencies(db_connection)

    async def test_visualise_cluster_unknown_id(self, call_endpoint):
        missing_id = random_test_data.random_match_id()

        response = call_endpoint(
            "post",
            ROUTE,
            client=Client.HMPPS_PERSON_MATCH,
            json=[missing_id],
        )

        assert response.status_code == 404
        assert response.json() == {"unknownIds": [missing_id]}

    async def test_visualise_cluster_returns_spec(
        self,
        call_endpoint,
        person_factory: PersonFactory,
    ):
        person_1 = await person_factory.create_from(MockPerson())
        person_2 = await person_factory.create_from(person_1)

        response = call_endpoint(
            "post",
            ROUTE,
            client=Client.HMPPS_PERSON_MATCH,
            json=[person_1.match_id, person_2.match_id],
        )

        assert response.status_code == 200
        payload = response.json()
        assert "spec" in payload
        spec = payload["spec"]
        assert isinstance(spec, dict)
        assert spec.get("data") is not None
