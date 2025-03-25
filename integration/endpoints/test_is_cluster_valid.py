import uuid

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_person_match.routes.cluster.is_cluster_valid import ROUTE
from integration.client import Client
from integration.mock_person import MockPerson
from integration.test_base import IntegrationTestBase


class TestIsClusterValidEndpoint(IntegrationTestBase):
    """
    Test is-cluster-valid
    """

    @pytest.fixture(autouse=True, scope="function")
    async def before_each(self, db_connection: AsyncSession, person_match_url: str):
        """
        Before Each
        """
        await self.truncate_person_data(db_connection)
        await self.refresh_term_frequencies_assert_empty(person_match_url, db_connection)

    async def test_is_cluster_valid_unknown_id(self, call_endpoint, match_id):
        """
        Test is cluster valid handles an unknown match id
        """
        response = call_endpoint("get", self._build_is_cluster_valid_url([match_id]), client=Client.HMPPS_PERSON_MATCH)
        print(self._build_is_cluster_valid_url([match_id]))
        assert response.status_code == 404
        assert response.json() == {"unknown_ids": [match_id]}

    async def test_score_invalid_match_id(self, call_endpoint, match_id):
        """
        Test is cluster valid handles non uuid match_id
        """
        match_id = "invalid_!!id123"
        response = call_endpoint("get", self._build_is_cluster_valid_url([match_id]), client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 404
        assert response.json() == {"unknown_ids": [match_id]}

    async def test_is_cluster_valid_validates_cluster(self, call_endpoint, match_id, create_person_record):
        """
        Test is cluster valid correctly identifies a valid cluster
        """
        # Create person to match and score
        person_data = MockPerson(matchId=match_id)
        await create_person_record(person_data)
        # Create different person
        matching_person_id_1 = str(uuid.uuid4())
        person_data.match_id = matching_person_id_1
        await create_person_record(person_data)

        # Create different person
        matching_person_id_2 = str(uuid.uuid4())
        person_data.match_id = matching_person_id_2
        await create_person_record(person_data)

        # Call is-cluster-valid endpoint - should all be in same cluster
        response = call_endpoint(
            "get",
            self._build_is_cluster_valid_url(
                [match_id, matching_person_id_1, matching_person_id_2],
            ),
            client=Client.HMPPS_PERSON_MATCH,
        )
        assert response.status_code == 200
        response_data = response.json()
        assert len(response_data) == 2
        assert "isClusterValid" in response_data
        assert "clusters" in response_data
        assert response_data["isClusterValid"]
        # we should only have one cluster
        assert len(response_data["clusters"]) == 1
        # that cluster should be of size 3
        assert len(response_data["clusters"][0]) == 3

    @staticmethod
    def _build_is_cluster_valid_url(match_ids: list[str]):
        query_string = "&".join(map(lambda m_id: f"match_ids={m_id}", match_ids))
        return f"{ROUTE}?{query_string}"
