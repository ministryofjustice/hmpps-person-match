from collections.abc import Callable, Generator
from unittest.mock import AsyncMock, Mock, patch

import pytest
from fastapi.testclient import TestClient

from hmpps_person_match.domain.roles import Roles
from hmpps_person_match.domain.telemetry_events import TelemetryEvents
from hmpps_person_match.models.person.person_score import PersonScore
from hmpps_person_match.routes.person.search.person_search import ROUTE


@pytest.fixture()
def person_json() -> dict:
    return {
        "matchId": "caller-match-id",
        "sourceSystem": "DELIUS",
        "sourceSystemId": "A12345BC",
        "masterDefendantId": None,
        "firstName": "Henry",
        "middleNames": "Ahmed",
        "lastName": "Junaed",
        "dateOfBirth": "1992-03-02",
        "firstNameAliases": [],
        "lastNameAliases": [],
        "dateOfBirthAliases": [],
        "postcodes": [],
        "cros": [],
        "pncs": [],
        "sentenceDates": [],
        "overrideMarker": None,
        "overrideScopes": None,
    }


class TestPersonSearchRoute:
    @staticmethod
    @pytest.fixture(autouse=True)
    def mock_search_results() -> Generator[AsyncMock]:
        with patch(
            "hmpps_cpr_splink.cpr_splink.interface.search.search_candidates",
            new_callable=AsyncMock,
        ) as mocked_search:
            yield mocked_search

    def test_search_no_results(
        self,
        call_endpoint: Callable,
        person_json: dict,
        mock_search_results: AsyncMock,
        mock_db_connection: Mock,
        mock_logger: Mock,
    ) -> None:
        mock_search_results.return_value = []

        response = call_endpoint("post", ROUTE, roles=[Roles.ROLE_PERSON_MATCH], json=person_json)

        assert response.status_code == 200
        assert response.json() == []
        mock_search_results.assert_awaited_once()
        assert mock_search_results.await_args.args[0].match_id == "caller-match-id"
        assert mock_search_results.await_args.args[1] is mock_db_connection
        mock_logger.info.assert_called_once_with(
            TelemetryEvents.PERSON_SEARCH_COMPLETED,
            extra={"candidate_size": 0},
        )

    def test_search_returns_exact_person_score_shape(
        self,
        call_endpoint: Callable,
        person_json: dict,
        mock_search_results: AsyncMock,
        mock_logger: Mock,
    ) -> None:
        mock_search_results.return_value = [
            PersonScore(
                candidate_match_id="candidate-id",
                candidate_match_probability=0.9999,
                candidate_match_weight=24,
                candidate_should_join=True,
                candidate_should_fracture=False,
                candidate_is_possible_twin=True,
                unadjusted_match_weight=26,
            ),
        ]

        response = call_endpoint("post", ROUTE, roles=[Roles.ROLE_PERSON_MATCH], json=person_json)

        assert response.status_code == 200
        assert response.json() == [
            {
                "candidate_match_id": "candidate-id",
                "candidate_match_probability": 0.9999,
                "candidate_match_weight": 24,
                "candidate_should_join": True,
                "candidate_should_fracture": False,
                "candidate_is_possible_twin": True,
                "unadjusted_match_weight": 26,
            },
        ]
        mock_logger.info.assert_called_once_with(
            TelemetryEvents.PERSON_SEARCH_COMPLETED,
            extra={"candidate_size": 1},
        )

    def test_blank_optional_names_and_empty_arrays_are_supported(
        self,
        call_endpoint: Callable,
        person_json: dict,
        mock_search_results: AsyncMock,
    ) -> None:
        person_json.update({"firstName": "", "middleNames": "", "lastName": ""})
        mock_search_results.return_value = []

        response = call_endpoint("post", ROUTE, roles=[Roles.ROLE_PERSON_MATCH], json=person_json)

        assert response.status_code == 200
        mock_search_results.assert_awaited_once()

    def test_invalid_request_does_not_start_search(
        self,
        call_endpoint: Callable,
        mock_search_results: AsyncMock,
    ) -> None:
        response = call_endpoint("post", ROUTE, roles=[Roles.ROLE_PERSON_MATCH], json=None)

        assert response.status_code == 400
        assert response.json()["detail"] == "Invalid request."
        mock_search_results.assert_not_awaited()

    def test_invalid_role_returns_forbidden(self, call_endpoint: Callable) -> None:
        response = call_endpoint("post", ROUTE, roles=["Invalid Role"], json={})

        assert response.status_code == 403
        assert response.json()["detail"] == "You do not have permission to access this resource."

    def test_no_auth_returns_unauthorized(self, client: TestClient) -> None:
        response = client.post(ROUTE, json={})

        assert response.status_code == 401
        assert response.json()["detail"] == "Not authenticated"
