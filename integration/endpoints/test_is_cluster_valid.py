import uuid

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_person_match.models.person.person import Person
from hmpps_person_match.routes.cluster.is_cluster_valid import ROUTE
from integration import random_test_data
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
        await self.refresh_term_frequencies(db_connection)

    async def test_is_cluster_valid_unknown_id(self, call_endpoint, match_id):
        """
        Test is cluster valid handles an unknown match id
        """
        response = call_endpoint("post", ROUTE, client=Client.HMPPS_PERSON_MATCH, json=[match_id])
        assert response.status_code == 404
        assert response.json() == {"unknownIds": [match_id]}

    async def test_score_invalid_match_id(self, call_endpoint, match_id):
        """
        Test is cluster valid handles non uuid match_id
        """
        match_id = "invalid_!!id123"
        response = call_endpoint("post", ROUTE, client=Client.HMPPS_PERSON_MATCH, json=[match_id])
        assert response.status_code == 404
        assert response.json() == {"unknownIds": [match_id]}

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
        person_data.source_system_id = random_test_data.random_source_system_id()
        await create_person_record(person_data)

        # Create different person
        matching_person_id_2 = str(uuid.uuid4())
        person_data.match_id = matching_person_id_2
        person_data.source_system_id = random_test_data.random_source_system_id()
        await create_person_record(person_data)

        # Call is-cluster-valid endpoint - should all be in same cluster
        data = [match_id, matching_person_id_1, matching_person_id_2]
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

    async def test_is_cluster_valid_identifies_multiple_clusters(self, call_endpoint, match_id, create_person_record):
        """
        Test is cluster valid correctly identifies when we have multiple clusters
        """
        # Create initial person
        person_data = MockPerson(
            matchId=match_id,
            firstName="First",
            middleNames="",
            lastName="Last",
            firstNameAliases=[],
            lastNameAliases=[],
            dateOfBirth="1980-01-01",
            dateOfBirthAliases=[],
            postcodes=["AA1 1AA"],
            cros=["00/123456A"],
            pncs=["00/1234567A"],
            sentenceDates=["2005-04-04"],
        )
        await create_person_record(person_data)
        # Create a duplicate person
        matching_person_id_1 = str(uuid.uuid4())
        person_data.match_id = matching_person_id_1
        person_data.source_system_id = random_test_data.random_source_system_id()
        await create_person_record(person_data)

        # Create an entirely different person
        person_data = MockPerson(
            matchId=match_id,
            firstName="Different",
            middleNames="",
            lastName="Name",
            firstNameAliases=[],
            lastNameAliases=[],
            dateOfBirth="1990-07-23",
            dateOfBirthAliases=[],
            postcodes=["BB2 2BB"],
            cros=["11/654321Z"],
            pncs=["11/7654321Z"],
            sentenceDates=["2000-03-02"],
        )
        matching_person_id_2 = str(uuid.uuid4())
        person_data = MockPerson(
            matchId=matching_person_id_2,
            sourceSystemId=random_test_data.random_source_system_id(),
        )
        await create_person_record(person_data)

        # Call is-cluster-valid endpoint - should all be in same cluster
        data = [match_id, matching_person_id_1, matching_person_id_2]
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

    async def test_is_cluster_invalid_exclude_override_marker(self, call_endpoint, match_id, create_person_record):
        """
        Test is-cluster-valid correctly identifies a override marker within a invalid cluster
        Records have same scope but different override marker
        """
        # Create person to match and score
        scope = self.new_scope()
        person_data = MockPerson(matchId=match_id)

        await create_person_record(person_data)
        # Create different person
        matching_person_id_1 = str(uuid.uuid4())
        person_data.match_id = matching_person_id_1
        person_data.source_system_id = random_test_data.random_source_system_id()
        person_data.override_marker = self.new_override_marker()
        person_data.override_scopes = [scope]
        await create_person_record(person_data)

        # Create different matching person
        matching_person_id_2 = str(uuid.uuid4())
        person_data.match_id = matching_person_id_2
        person_data.source_system_id = random_test_data.random_source_system_id()
        person_data.override_marker = self.new_override_marker()
        person_data.override_scopes = [scope]
        await create_person_record(person_data)

        data = [match_id, matching_person_id_1, matching_person_id_2]
        response = call_endpoint("post", ROUTE, client=Client.HMPPS_PERSON_MATCH, json=data)
        assert response.status_code == 200
        response_data = response.json()
        # in this case the cluster is NOT valid
        assert not response_data["isClusterValid"]
        # override markers inconsistent, so should get back empty list
        assert len(response_data["clusters"]) == 0

    async def test_is_cluster_valid_include_override_marker(self, call_endpoint, match_id, create_person_record):
        """
        Test is-cluster-valid correctly identifies a override marker with a invalid cluster
        Records have same scope and override marker
        """
        # Create person to match and score
        person_data = MockPerson(matchId=match_id)
        await create_person_record(person_data)

        # Create different person
        non_matching_person_2_data = MockPerson(matchId=self.new_match_id())
        await create_person_record(non_matching_person_2_data)

        # Create different person
        non_matching_person_3_data = MockPerson(matchId=self.new_match_id())
        await create_person_record(non_matching_person_3_data)

        # Call is-cluster-valid endpoint - should all be in same cluster
        record_match_ids_data = [match_id, non_matching_person_2_data.match_id, non_matching_person_3_data.match_id]

        response = call_endpoint("post", ROUTE, client=Client.HMPPS_PERSON_MATCH, json=record_match_ids_data)
        assert response.status_code == 200
        response_data = response.json()
        # in this case the cluster is NOT valid
        assert not response_data["isClusterValid"]

        # Add include scope + override
        scope = self.new_scope()
        override_marker = self.new_override_marker()

        person_data.override_marker = override_marker
        person_data.override_scopes = [scope]
        await self._update_person(call_endpoint, person_data)

        non_matching_person_2_data.override_marker = override_marker
        non_matching_person_2_data.override_scopes = [scope]
        await self._update_person(call_endpoint, non_matching_person_2_data)

        non_matching_person_3_data.override_marker = override_marker
        non_matching_person_3_data.override_scopes = [scope]
        await self._update_person(call_endpoint, non_matching_person_3_data)

        response = call_endpoint("post", ROUTE, client=Client.HMPPS_PERSON_MATCH, json=record_match_ids_data)
        assert response.status_code == 200
        response_data = response.json()
        # in this case the cluster is now valid
        assert response_data["isClusterValid"]

    async def _update_person(self, call_endpoint, person_data: Person):
        response = call_endpoint(
            "post",
            "/person",
            json=person_data.model_dump(by_alias=True),
            client=Client.HMPPS_PERSON_MATCH,
        )
        assert response.status_code == 200
