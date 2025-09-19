import uuid
from unittest.mock import AsyncMock, Mock, patch

import pytest

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
    def mock_existence_check():
        """
        Mock the cpr splink candidate results
        """
        with patch(
            "hmpps_cpr_splink.cpr_splink.interface.score.match_record_exists",
            new_callable=AsyncMock,
        ) as mocked_existence_check:
            yield mocked_existence_check

    @staticmethod
    @pytest.fixture(autouse=True)
    def mock_score_results():
        """
        Mock the cpr splink candidate results
        """
        with patch(
            "hmpps_cpr_splink.cpr_splink.interface.score.get_scored_candidates",
            new_callable=AsyncMock,
        ) as mocked_score:
            yield mocked_score

    def test_person_score_no_results(self, call_endpoint, mock_score_results: AsyncMock, mock_logger: Mock):
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

    def test_person_score_with_results(self, call_endpoint, mock_score_results: AsyncMock, mock_logger: Mock):
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
            ),
            PersonScore(
                candidate_match_id=match_id_2,
                candidate_match_probability=0.9999,
                candidate_match_weight=24,
                candidate_should_fracture=False,
                candidate_should_join=True,
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
            },
            {
                "candidate_match_id": match_id_2,
                "candidate_match_probability": 0.9999,
                "candidate_match_weight": 24,
                "candidate_should_fracture": False,
                "candidate_should_join": True,
            },
        ]
        mock_logger.info.assert_called_with(
            TelemetryEvents.PERSON_SCORE,
            extra={
                "matchId": searching_person,
                "candidate_size": 2,
            },
        )

    def test_candidate_does_not_exist(self, call_endpoint, mock_existence_check: AsyncMock):
        """
        Test when candidate score call for and does not exist
        Return 404
        """
        mock_existence_check.return_value = False
        response = call_endpoint("get", self._generate_match_score_url(), roles=[Roles.ROLE_PERSON_MATCH])
        assert response.status_code == 404

    def test_invalid_role_unauthorized(self, call_endpoint):
        """
        Test endpoint called with invalid role
        Return 403 FORBIDDEN
        """
        response = call_endpoint("get", self._generate_match_score_url(), roles=["Invalid Role"], json={})
        assert response.status_code == 403
        assert response.json()["detail"] == "You do not have permission to access this resource."

    def test_no_auth_returns_unauthorized(self, client):
        """
        Test endpoint called with no auth
        Return 403 FORBIDDEN
        """
        response = client.get(self._generate_match_score_url())
        assert response.status_code == 403
        assert response.json()["detail"] == "Not authenticated"

    @staticmethod
    def _generate_match_score_url(match_id=uuid.uuid4()):  # noqa: B008
        return ROUTE.format(match_id=match_id)
