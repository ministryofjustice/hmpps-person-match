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

    async def test_is_cluster_invalid_exclude_override_marker(self, call_endpoint, person_factory: PersonFactory):
        """
        Test is-cluster-valid correctly identifies a override marker within a invalid cluster
        Records have same scope but different override marker
        """
        # Create person to match and score
        scope = self.new_scope()
        person_data = MockPerson()
        person_1 = await person_factory.create_from(person_data)

        # Create different person
        person_data.override_marker = self.new_override_marker()
        person_data.override_scopes = [scope]
        person_2 = await person_factory.create_from(person_data)

        # Create different matching person
        person_data.override_marker = self.new_override_marker()
        person_data.override_scopes = [scope]
        person_3 = await person_factory.create_from(person_data)

        data = [person_1.match_id, person_2.match_id, person_3.match_id]
        response = call_endpoint("post", ROUTE, client=Client.HMPPS_PERSON_MATCH, json=data)
        assert response.status_code == 200
        response_data = response.json()
        # in this case the cluster is NOT valid
        assert not response_data["isClusterValid"]
        # override markers inconsistent, so should get back empty list
        assert len(response_data["clusters"]) == 0

    async def test_is_cluster_valid_exclude_override_marker_different_scopes(
        self,
        call_endpoint,
        person_factory: PersonFactory,
    ):
        """
        Test is-cluster-valid correctly ignores an override marker within a valid cluster
        Records have different override marker, but with no overlapping scope
        """
        # Create person to match and score
        person_data = MockPerson()
        person_1 = await person_factory.create_from(person_data)

        # Create different person
        person_data.override_marker = self.new_override_marker()
        person_data.override_scopes = [self.new_scope()]
        person_2 = await person_factory.create_from(person_data)

        # Create different matching person
        person_data.override_marker = self.new_override_marker()
        person_data.override_scopes = [self.new_scope()]
        person_3 = await person_factory.create_from(person_data)

        data = [person_1.match_id, person_2.match_id, person_3.match_id]
        response = call_endpoint("post", ROUTE, client=Client.HMPPS_PERSON_MATCH, json=data)
        assert response.status_code == 200
        response_data = response.json()
        # in this case the cluster is valid - the differing scopes mean that we should not care
        # that override_markers are different
        assert response_data["isClusterValid"]

    async def test_is_cluster_valid_include_override_marker(self, call_endpoint, person_factory: PersonFactory):
        """
        Test is-cluster-valid correctly identifies a override marker with a invalid cluster
        Records have same scope and override marker
        """
        # Create person to match and score
        person_1 = await person_factory.create_from(MockPerson())

        # Create different person
        non_matching_person_2 = await person_factory.create_from(MockPerson())

        # Create different person
        non_matching_person_3 = await person_factory.create_from(MockPerson())

        # Call is-cluster-valid endpoint - should all be in same cluster
        record_match_ids_data = [person_1.match_id, non_matching_person_2.match_id, non_matching_person_3.match_id]

        response = call_endpoint("post", ROUTE, client=Client.HMPPS_PERSON_MATCH, json=record_match_ids_data)
        assert response.status_code == 200
        response_data = response.json()
        # in this case the cluster is NOT valid
        assert not response_data["isClusterValid"]

        # Add include scope + override
        scope = self.new_scope()
        override_marker = self.new_override_marker()

        person_1.override_marker = override_marker
        person_1.override_scopes = [scope]
        await person_factory.update(person_1)

        non_matching_person_2.override_marker = override_marker
        non_matching_person_2.override_scopes = [scope]
        await person_factory.update(non_matching_person_2)

        non_matching_person_3.override_marker = override_marker
        non_matching_person_3.override_scopes = [scope]
        await person_factory.update(non_matching_person_3)

        response = call_endpoint("post", ROUTE, client=Client.HMPPS_PERSON_MATCH, json=record_match_ids_data)
        assert response.status_code == 200
        response_data = response.json()
        # in this case the cluster is now valid
        assert response_data["isClusterValid"]
