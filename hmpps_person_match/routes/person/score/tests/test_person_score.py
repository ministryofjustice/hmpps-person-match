import uuid
from collections.abc import Callable, Generator
from unittest.mock import AsyncMock, Mock, patch

import pytest
from fastapi.testclient import TestClient

from hmpps_person_match.domain.roles import Roles
from hmpps_person_match.domain.telemetry_events import TelemetryEvents
from hmpps_person_match.models.person.person_score import PersonScore
from hmpps_person_match.routes.person.score.person_score import ROUTE


class TestPersonScoreRoute:
    """
    Test Person Score Route
    """

    @staticmethod
    @pytest.fixture(autouse=True)
    def mock_score_results() -> Generator[AsyncMock]:
        """
        Mock the cpr splink candidate results
        """
        with patch(
            "hmpps_cpr_splink.cpr_splink.interface.score.get_scored_candidates",
            new_callable=AsyncMock,
        ) as mocked_score:
            yield mocked_score

    @staticmethod
    @pytest.fixture(autouse=True)
    def mock_existence_check() -> Generator[AsyncMock]:
        """
        Mock the cpr splink record existence check
        """
        with patch(
            "hmpps_cpr_splink.cpr_splink.interface.score.match_record_exists",
            new_callable=AsyncMock,
        ) as mocked_existence:
            yield mocked_existence

    def test_person_score_no_results(
        self,
        call_endpoint: Callable,
        mock_score_results: AsyncMock,
        mock_logger: Mock,
    ) -> None:
        """
        Test that returns no results when no candidates results are returned
        """
        match_id = str(uuid.uuid4())
        mock_score_results.return_value = []

        response = call_endpoint("get", self._generate_match_score_url(match_id), roles=[Roles.ROLE_PERSON_MATCH])
        assert response.status_code == 200
        assert response.json() == []
        mock_logger.info.assert_called_with(
            TelemetryEvents.PERSON_SCORE,
            extra={
                "matchId": match_id,
                "candidate_size": 0,
            },
        )

    def test_person_score_with_results(
        self,
        call_endpoint: Callable,
        mock_score_results: AsyncMock,
        mock_logger: Mock,
    ) -> None:
        """
        Test that returns candidate results in correct format
        """
        searching_person = str(uuid.uuid4())
        match_id_1 = str(uuid.uuid4())
        match_id_2 = str(uuid.uuid4())
        mock_score_results.return_value = [
            PersonScore(
                candidate_match_id=match_id_1,
                candidate_match_probability=0.9999,
                candidate_match_weight=24,
                candidate_should_fracture=False,
                candidate_should_join=True,
                candidate_is_possible_twin=False,
                unadjusted_match_weight=24,
            ),
            PersonScore(
                candidate_match_id=match_id_2,
                candidate_match_probability=0.9999,
                candidate_match_weight=24,
                candidate_should_fracture=False,
                candidate_should_join=True,
                candidate_is_possible_twin=False,
                unadjusted_match_weight=24,
            ),
        ]
        response = call_endpoint(
            "get",
            self._generate_match_score_url(searching_person),
            roles=[Roles.ROLE_PERSON_MATCH],
        )
        assert response.status_code == 200
        assert response.json() == [
            {
                "candidate_match_id": match_id_1,
                "candidate_match_probability": 0.9999,
                "candidate_match_weight": 24,
                "candidate_should_fracture": False,
                "candidate_should_join": True,
                "candidate_is_possible_twin": False,
                "unadjusted_match_weight": 24,
            },
            {
                "candidate_match_id": match_id_2,
                "candidate_match_probability": 0.9999,
                "candidate_match_weight": 24,
                "candidate_should_fracture": False,
                "candidate_should_join": True,
                "candidate_is_possible_twin": False,
                "unadjusted_match_weight": 24,
            },
        ]
        mock_logger.info.assert_called_with(
            TelemetryEvents.PERSON_SCORE,
            extra={
                "matchId": searching_person,
                "candidate_size": 2,
            },
        )

    def test_missing_record_returns_404(self, call_endpoint: Callable, mock_existence_check: AsyncMock) -> None:
        """
        Test missing record to score
        Returns 404 Not Found
        """
        mock_existence_check.return_value = False
        response = call_endpoint(
            "get",
            self._generate_match_score_url(),
            roles=[Roles.ROLE_PERSON_MATCH],
        )
        assert response.status_code == 404

    def test_invalid_role_unauthorized(self, call_endpoint: Callable) -> None:
        """
        Test invalid role
        Returns 403 Forbidden
        """
        response = call_endpoint("get", self._generate_match_score_url(), roles=["Invalid Role"], json={})
        assert response.status_code == 403
        assert response.json()["detail"] == "You do not have permission to access this resource."

    def test_no_auth_returns_unauthorized(self, client: TestClient) -> None:
        response = client.get(self._generate_match_score_url())
        assert response.status_code == 403
        assert response.json()["detail"] == "Not authenticated"

    @staticmethod
    def _generate_match_score_url(match_id: str = str(uuid.uuid4())) -> str:
        return ROUTE.format(match_id=match_id)
