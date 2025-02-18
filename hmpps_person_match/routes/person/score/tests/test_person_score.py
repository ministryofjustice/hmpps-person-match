import uuid
from unittest.mock import AsyncMock, patch

import pytest

from hmpps_cpr_splink.cpr_splink.interface.score import ScoredCandidate
from hmpps_person_match.domain.roles import Roles
from hmpps_person_match.routes.person.score.person_score import ROUTE


class TestPersonScoreRoute:
    """
    Test Person Score Route
    """

    @staticmethod
    @pytest.fixture(autouse=True)
    def mock_score_results():
        """
        Mock the cpr splink candidate results
        """
        with patch("hmpps_cpr_splink.cpr_splink.interface.score.get_scored_candidates",
                   new_callable=AsyncMock) as mocked_score:
            yield mocked_score

    def test_person_score_no_results(self, call_endpoint, mock_score_results):
        """
        Test that returns no results when no candidates results are returned
        """
        mock_score_results.return_value = []
        response = call_endpoint("get", self._generate_match_score_url(), roles=[Roles.ROLE_PERSON_MATCH])
        assert response.status_code == 200
        assert response.json() == []

    def test_person_score_with_results(self, call_endpoint, mock_score_results):
        """
        Test that returns candidate results in correct format
        """
        match_id_1 = str(uuid.uuid4())
        match_id_2 = str(uuid.uuid4())
        mock_score_results.return_value = [
            ScoredCandidate(candidate_match_id=match_id_1,
                            candidate_match_probability=0.9999,
                            candidate_match_weight=0.12345),
            ScoredCandidate(candidate_match_id=match_id_2,
                            candidate_match_probability=0.9999,
                            candidate_match_weight=0.12345),
        ]
        response = call_endpoint("get", self._generate_match_score_url(), roles=[Roles.ROLE_PERSON_MATCH])
        assert response.status_code == 200
        assert response.json() == [
            {
                "candidate_match_id": match_id_1,
                "candidate_match_probability": 0.9999,
                "candidate_match_weight": 0.12345,
            },
            {
                "candidate_match_id": match_id_2,
                "candidate_match_probability": 0.9999,
                "candidate_match_weight": 0.12345,
            },
        ]

    def test_invalid_role_unauthorized(self, call_endpoint):
        response = call_endpoint("get", self._generate_match_score_url(), roles=["Invalid Role"], json={})
        assert response.status_code == 403
        assert response.json()["detail"] == "You do not have permission to access this resource."

    def test_no_auth_returns_unauthorized(self, client):
        response = client.get(self._generate_match_score_url())
        assert response.status_code == 403
        assert response.json()["detail"] == "Not authenticated"

    @staticmethod
    def _generate_match_score_url():
        return ROUTE.format(match_id=uuid.uuid4())
