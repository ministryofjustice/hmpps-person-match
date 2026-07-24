import uuid
from collections.abc import Callable, Generator
from unittest.mock import AsyncMock, Mock, patch

import pytest
from fastapi.testclient import TestClient

from hmpps_person_match.domain.roles import Roles
from hmpps_person_match.domain.telemetry_events import TelemetryEvents
from hmpps_person_match.models.person.person import Person
from hmpps_person_match.models.person.person_score import PersonScore
from hmpps_person_match.routes.person.search.person_search import ROUTE


class TestPersonSearchRoute:
    @staticmethod
    @pytest.fixture(autouse=True)
    def mock_search_candidates() -> Generator[AsyncMock]:
        with patch(
            "hmpps_cpr_splink.cpr_splink.interface.search.search_candidates",
            new_callable=AsyncMock,
        ) as mocked_search_candidates:
            yield mocked_search_candidates

    def test_person_search_no_results(
        self,
        call_endpoint: Callable,
        mock_search_candidates: AsyncMock,
        mock_db_engine: Mock,
        mock_logger: Mock,
    ) -> None:
        """
        If the mocked search_candidates function returns an empty list,
        then the endpoint should return HTTP 200 with an empty list,
        call search_candidates once with the expected Person and db engine,
        and log the search with a candidate size of 0.
        """

        payload = self._search_payload()
        mock_search_candidates.return_value = []

        response = call_endpoint(
            "post",
            ROUTE,
            roles=[Roles.ROLE_PERSON_MATCH],
            json=payload,
        )

        assert response.status_code == 200
        assert response.json() == []
        assert mock_search_candidates.await_count == 1
        called_person = mock_search_candidates.await_args.kwargs["person"]
        assert isinstance(called_person, Person)
        assert called_person.model_dump(mode="json", by_alias=True) == payload
        assert mock_search_candidates.await_args.kwargs["pg_engine"] is mock_db_engine
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
        """
        If the mocked search_candidates function returns two PersonScore results,
        then the endpoint should return HTTP 200 with those results serialized in
        the response body and log the search with a candidate size of 2.
        """
        match_id_1 = str(uuid.uuid4())
        match_id_2 = str(uuid.uuid4())
        mock_search_candidates.return_value = [
            PersonScore(
                candidate_match_id=match_id_1,
                candidate_match_probability=0.9999,
                candidate_match_weight=24.0,
                candidate_should_join=True,
                candidate_should_fracture=False,
                candidate_is_possible_twin=False,
                unadjusted_match_weight=24.0,
            ),
            PersonScore(
                candidate_match_id=match_id_2,
                candidate_match_probability=0.8123,
                candidate_match_weight=19.5,
                candidate_should_join=False,
                candidate_should_fracture=False,
                candidate_is_possible_twin=True,
                unadjusted_match_weight=18.1,
            ),
        ]

        response = call_endpoint(
            "post",
            ROUTE,
            roles=[Roles.ROLE_PERSON_MATCH],
            json=self._search_payload(),
        )

        assert response.status_code == 200
        assert response.json() == [
            {
                "candidate_match_id": match_id_1,
                "candidate_match_probability": 0.9999,
                "candidate_match_weight": 24.0,
                "candidate_should_join": True,
                "candidate_should_fracture": False,
                "candidate_is_possible_twin": False,
                "unadjusted_match_weight": 24.0,
            },
            {
                "candidate_match_id": match_id_2,
                "candidate_match_probability": 0.8123,
                "candidate_match_weight": 19.5,
                "candidate_should_join": False,
                "candidate_should_fracture": False,
                "candidate_is_possible_twin": True,
                "unadjusted_match_weight": 18.1,
            },
        ]
        mock_logger.info.assert_called_with(
            TelemetryEvents.PERSON_SEARCH,
            extra={"candidate_size": 2},
        )

    def test_bad_request_on_empty(self, call_endpoint: Callable, mock_search_candidates: AsyncMock) -> None:
        response = call_endpoint(
            "post",
            ROUTE,
            roles=[Roles.ROLE_PERSON_MATCH],
            json=None,
        )

        assert response.status_code == 400
        assert response.json()["detail"] == "Invalid request."
        mock_search_candidates.assert_not_awaited()

    def test_bad_request_different_data_types(
        self,
        call_endpoint: Callable,
        mock_search_candidates: AsyncMock,
    ) -> None:
        payload = self._search_payload()
        payload["sourceSystem"] = 1

        response = call_endpoint(
            "post",
            ROUTE,
            roles=[Roles.ROLE_PERSON_MATCH],
            json=payload,
        )

        assert response.status_code == 400
        assert response.json()["detail"] == "Invalid request."
        assert response.json()["errors"] == [
            {
                "type": "string_type",
                "loc": ["body", "sourceSystem"],
                "msg": "Input should be a valid string",
                "input": 1,
            },
        ]
        mock_search_candidates.assert_not_awaited()

    def test_invalid_role_unauthorized(self, call_endpoint: Callable) -> None:
        response = call_endpoint(
            "post",
            ROUTE,
            roles=["Invalid Role"],
            json=self._search_payload(),
        )

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
