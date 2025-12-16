from collections.abc import Callable

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
    async def before_each(self, db_connection: AsyncSession) -> None:
        await self.truncate_person_data(db_connection)
        await self.refresh_term_frequencies(db_connection)

    async def test_visualise_cluster_unknown_id(self, call_endpoint: Callable) -> None:
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
        call_endpoint: Callable,
        person_factory: PersonFactory,
    ) -> None:
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

    async def test_visualise_cluster_with_twins(
        self,
        call_endpoint: Callable,
        person_factory: PersonFactory,
    ) -> None:
        """Test that visualise endpoint works when twins are detected."""
        person_data = MockPerson()
        person_data.pncs = []
        person_data.cros = []
        person_data.master_defendant_id = None
        person_data.first_name_aliases = []
        person_1 = await person_factory.create_from(person_data)

        # Create twin: same surname, dob, postcode but different first name
        person_data.first_name = random_test_data.random_name()
        person_2 = await person_factory.create_from(person_data)

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
        link_data = next(d for d in spec["data"] if d["name"] == "link-data")
        edges = link_data["values"]
        assert len(edges) == 1
        assert edges[0]["possible_twins"] is True
        assert "unaltered_match_weight" in edges[0]

    async def test_visualise_cluster_twins_waterfall_has_adjustment(
        self,
        call_endpoint: Callable,
        person_factory: PersonFactory,
    ) -> None:
        """Test that twins adjustment bar appears in waterfall data."""
        person_data = MockPerson()
        person_data.pncs = []
        person_data.cros = []
        person_data.master_defendant_id = None
        person_data.first_name_aliases = []
        person_1 = await person_factory.create_from(person_data)

        person_data.first_name = random_test_data.random_name()
        person_2 = await person_factory.create_from(person_data)

        response = call_endpoint(
            "post",
            ROUTE,
            client=Client.HMPPS_PERSON_MATCH,
            json=[person_1.match_id, person_2.match_id],
        )

        assert response.status_code == 200
        spec = response.json()["spec"]
        link_data = next(d for d in spec["data"] if d["name"] == "link-data")
        edge = link_data["values"][0]
        waterfall = edge["waterfall_data"]
        twins_bar = next((w for w in waterfall if w["column_name"] == "twins_adjustment"), None)
        assert twins_bar is not None
        assert twins_bar["log2_bayes_factor"] < 0  # Twins adjustment is negative
