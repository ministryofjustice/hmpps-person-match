import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_person_match.routes.cluster.is_cluster_valid import ROUTE
from integration import random_test_data
from integration.client import Client
from integration.conftest import PersonFactory
from integration.mock_person import MockPerson
from integration.test_base import IntegrationTestBase


class TestIsClusterValidEndpoint(IntegrationTestBase):
    """
    Test is-cluster-valid
    """

    @pytest.fixture(autouse=True, scope="function")
    async def before_each(self, db_connection: AsyncSession):
        """
        Before Each
        """
        await self.truncate_person_data(db_connection)
        await self.refresh_term_frequencies(db_connection)

    async def test_is_cluster_valid_unknown_id(self, call_endpoint):
        """
        Test is cluster valid handles an unknown match id
        """
        match_id = random_test_data.random_match_id()
        response = call_endpoint("post", ROUTE, client=Client.HMPPS_PERSON_MATCH, json=[match_id])
        assert response.status_code == 404
        assert response.json() == {"unknownIds": [match_id]}

    async def test_score_invalid_match_id(self, call_endpoint):
        """
        Test is cluster valid handles non uuid match_id
        """
        match_id = "invalid_!!id123"
        response = call_endpoint("post", ROUTE, client=Client.HMPPS_PERSON_MATCH, json=[match_id])
        assert response.status_code == 404
        assert response.json() == {"unknownIds": [match_id]}

    async def test_is_cluster_valid_validates_cluster(
        self,
        call_endpoint,
        person_factory: PersonFactory,
    ):
        """
        Test is cluster valid correctly identifies a valid cluster
        """
        # Create person to match and score
        person_1 = await person_factory.create_from(MockPerson())

        # Create different person
        person_2 = await person_factory.create_from(person_1)

        # Create different person
        person_3 = await person_factory.create_from(person_1)

        # Call is-cluster-valid endpoint - should all be in same cluster
        data = [person_1.match_id, person_2.match_id, person_3.match_id]

        response = call_endpoint("post", ROUTE, client=Client.HMPPS_PERSON_MATCH, json=data)
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

    async def test_is_cluster_valid_identifies_multiple_clusters(
        self,
        call_endpoint,
        person_factory: PersonFactory,
    ):
        """
        Test is cluster valid correctly identifies when we have multiple clusters
        """
        # Create initial person
        person_1 = await person_factory.create_from(MockPerson())

        # Create a duplicate person
        person_2 = await person_factory.create_from(person_1)

        # Create an entirely different person
        person_3 = await person_factory.create_from(MockPerson())

        # Call is-cluster-valid endpoint - should all be in same cluster
        data = [person_1.match_id, person_2.match_id, person_3.match_id]

        response = call_endpoint("post", ROUTE, client=Client.HMPPS_PERSON_MATCH, json=data)
        assert response.status_code == 200
        response_data = response.json()

        assert len(response_data) == 2
        assert "isClusterValid" in response_data
        assert "clusters" in response_data
        # in this case the cluster is NOT valid
        assert not response_data["isClusterValid"]
        # we should have two clusters
        assert len(response_data["clusters"]) == 2
        # these should be of size 1 & 2
        cluster_len_1 = len(response_data["clusters"][0])
        cluster_len_2 = len(response_data["clusters"][1])
        assert {cluster_len_1, cluster_len_2} == {1, 2}
