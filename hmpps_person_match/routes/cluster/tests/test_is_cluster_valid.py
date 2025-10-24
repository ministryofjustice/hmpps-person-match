import uuid
from collections.abc import Callable, Generator
from unittest.mock import AsyncMock, Mock, patch

import pytest
from fastapi.testclient import TestClient

from hmpps_cpr_splink.cpr_splink.interface.score import Clusters
from hmpps_person_match.domain.roles import Roles
from hmpps_person_match.domain.telemetry_events import TelemetryEvents
from hmpps_person_match.routes.cluster.is_cluster_valid import ROUTE


class TestIsClusterValidRoute:
    """
    Test Is Cluster Valid Route
    """

    @staticmethod
    @pytest.fixture(autouse=True)
    def mock_ids_check() -> Generator[AsyncMock]:
        """
        Mock checking if match_ids exist
        """
        with patch(
            "hmpps_cpr_splink.cpr_splink.interface.score.get_missing_record_ids",
            new_callable=AsyncMock,
        ) as mocked_ids_check:
            yield mocked_ids_check

    @staticmethod
    @pytest.fixture(autouse=True)
    def mock_cluster_results() -> Generator[AsyncMock]:
        """
        Mock the cpr splink clusters results
        """
        with patch(
            "hmpps_cpr_splink.cpr_splink.interface.score.get_clusters",
            new_callable=AsyncMock,
        ) as mocked_clusters:
            yield mocked_clusters

    def test_is_cluster_valid_missing_id(self, call_endpoint: Callable, mock_ids_check: AsyncMock) -> None:
        """
        Test that we get 404 when we ask for match_id that isn't in db
        """
        match_id_1 = str(uuid.uuid4())
        match_id_2 = str(uuid.uuid4())
        mock_ids_check.return_value = [match_id_1]

        data = [match_id_1, match_id_2]
        response = call_endpoint(
            "post",
            ROUTE,
            json=data,
            roles=[Roles.ROLE_PERSON_MATCH],
        )
        assert response.status_code == 404
        assert response.json() == {"unknownIds": [match_id_1]}

    def test_is_cluster_valid_results(
        self,
        call_endpoint: Callable,
        mock_ids_check: AsyncMock,
        mock_cluster_results: AsyncMock,
        mock_logger: Mock,
    ) -> None:
        """
        Test that get results in right format when cluster is valid
        """
        match_id_1 = str(uuid.uuid4())
        match_id_2 = str(uuid.uuid4())
        mock_ids_check.return_value = []
        mock_cluster_results.return_value = Clusters([[match_id_1, match_id_2]])

        data = [match_id_1, match_id_2]
        response = call_endpoint(
            "post",
            ROUTE,
            json=data,
            roles=[Roles.ROLE_PERSON_MATCH],
        )
        assert response.status_code == 200
        assert response.json() == {
            "isClusterValid": True,
            "clusters": [[match_id_1, match_id_2]],
        }
        mock_logger.info.assert_called_with(
            TelemetryEvents.IS_CLUSTER_VALID,
            extra={
                "isClusterValid": True,
                "clusters": str([[match_id_1, match_id_2]]),
            },
        )

    def test_is_cluster_valid_multiple_clusters(
        self,
        call_endpoint: Callable,
        mock_ids_check: AsyncMock,
        mock_cluster_results: AsyncMock,
        mock_logger: Mock,
    ) -> None:
        """
        Test that we get information on multiple clusters if cluster is not valid
        """
        match_id_1 = str(uuid.uuid4())
        match_id_2 = str(uuid.uuid4())
        match_id_3 = str(uuid.uuid4())
        mock_ids_check.return_value = []
        mock_cluster_results.return_value = Clusters([[match_id_1, match_id_2], [match_id_3]])

        data = [match_id_1, match_id_2, match_id_3]
        response = call_endpoint(
            "post",
            ROUTE,
            json=data,
            roles=[Roles.ROLE_PERSON_MATCH],
        )
        assert response.status_code == 200
        assert response.json() == {
            "isClusterValid": False,
            "clusters": [[match_id_1, match_id_2], [match_id_3]],
        }
        mock_logger.info.assert_called_with(
            TelemetryEvents.IS_CLUSTER_VALID,
            extra={
                "isClusterValid": False,
                "clusters": str([[match_id_1, match_id_2], [match_id_3]]),
            },
        )

    def test_invalid_role_unauthorized(self, call_endpoint: Callable) -> None:
        match_id_1 = str(uuid.uuid4())
        match_id_2 = str(uuid.uuid4())
        data = [match_id_1, match_id_2]
        response = call_endpoint(
            "post",
            ROUTE,
            roles=["Invalid Role"],
            json=data,
        )
        assert response.status_code == 403
        assert response.json()["detail"] == "You do not have permission to access this resource."

    def test_no_auth_returns_unauthorized(self, client: TestClient) -> None:
        match_id_1 = str(uuid.uuid4())
        match_id_2 = str(uuid.uuid4())
        data = [match_id_1, match_id_2]
        response = client.post(ROUTE, json=data)
        assert response.status_code == 403
        assert response.json()["detail"] == "Not authenticated"
