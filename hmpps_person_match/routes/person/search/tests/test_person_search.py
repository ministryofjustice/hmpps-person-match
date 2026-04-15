import uuid
from collections.abc import Callable, Generator
from unittest.mock import AsyncMock, Mock, patch

import pytest
from fastapi.testclient import TestClient

from hmpps_person_match.domain.roles import Roles
from hmpps_person_match.domain.telemetry_events import TelemetryEvents
from hmpps_person_match.models.person.person_score import PersonScore
from hmpps_person_match.routes.person.search.person_search import ROUTE


class TestPersonSearchRoute:
    @staticmethod
    @pytest.fixture(autouse=True)
    def mock_search_candidates() -> Generator[AsyncMock]:
        with patch(
            "hmpps_person_match.routes.person.search.person_search.search_candidates",
            new_callable=AsyncMock,
        ) as mocked_search:
            yield mocked_search

    def test_person_search_no_results(
        self,
        call_endpoint: Callable,
        mock_search_candidates: AsyncMock,
        mock_logger: Mock,
    ) -> None:
        mock_search_candidates.return_value = []

        response = call_endpoint("post", ROUTE, roles=[Roles.ROLE_PERSON_MATCH], json=self._search_payload())

        assert response.status_code == 200
        assert response.json() == []
        mock_logger.info.assert_called_with(
            TelemetryEvents.PERSON_SEARCH,
            extra={"candidate_size": 0},
        )

    def test_person_search_with_results(
        self,
        call_endpoint: Callable,
        mock_search_candidates: AsyncMock,
        mock_logger: Mock,
    ) -> None:
        match_id = str(uuid.uuid4())
        mock_search_candidates.return_value = [
            PersonScore(
                candidate_match_id=match_id,
                candidate_match_probability=0.98,
                candidate_match_weight=18.5,
                candidate_should_join=True,
                candidate_should_fracture=False,
                candidate_is_possible_twin=False,
                unadjusted_match_weight=18.5,
            ),
        ]

        response = call_endpoint("post", ROUTE, roles=[Roles.ROLE_PERSON_MATCH], json=self._search_payload())

        assert response.status_code == 200
        assert response.json() == [
            {
                "candidate_match_id": match_id,
                "candidate_match_probability": 0.98,
                "candidate_match_weight": 18.5,
                "candidate_should_join": True,
                "candidate_should_fracture": False,
                "candidate_is_possible_twin": False,
                "unadjusted_match_weight": 18.5,
            },
        ]
        mock_logger.info.assert_called_with(
            TelemetryEvents.PERSON_SEARCH,
            extra={"candidate_size": 1},
        )

    def test_invalid_role_unauthorized(self, call_endpoint: Callable) -> None:
        response = call_endpoint("post", ROUTE, roles=["Invalid Role"], json=self._search_payload())
        assert response.status_code == 403
        assert response.json()["detail"] == "You do not have permission to access this resource."

    def test_no_auth_returns_unauthorized(self, client: TestClient) -> None:
        response = client.post(ROUTE, json=self._search_payload())
        assert response.status_code == 401
        assert response.json()["detail"] == "Not authenticated"

    @staticmethod
    def _search_payload() -> dict:
        return {
            "matchId": "ignored-test-search-record",
            "sourceSystem": "TEST_SYSTEM",
            "sourceSystemId": "TEST-SOURCE-ID-0001",
            "masterDefendantId": "00000000-0000-4000-8000-000000000001",
            "firstName": "Testy",
            "middleNames": "Unit",
            "lastName": "Payload",
            "dateOfBirth": "2000-01-01",
            "firstNameAliases": ["Testy"],
            "lastNameAliases": ["Payload"],
            "dateOfBirthAliases": ["2000-01-02"],
            "postcodes": ["ZZ99 9ZZ"],
            "cros": ["TEST-CRO-001"],
            "pncs": ["TEST-PNC-001"],
            "sentenceDates": ["2020-01-01"],
            "overrideMarker": None,
            "overrideScopes": None,
        }
