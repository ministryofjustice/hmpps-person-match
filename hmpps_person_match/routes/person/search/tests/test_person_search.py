from collections.abc import Callable, Generator
from unittest.mock import AsyncMock, Mock, patch

import pytest
from fastapi.testclient import TestClient

from hmpps_person_match.domain.roles import Roles
from hmpps_person_match.routes.person.search.person_search import ROUTE


@pytest.fixture()
def search_request_json() -> dict:
    return {
        "fullName": "Henry Ahmed Junaed",
        "dateOfBirth": "1992-03-02",
        "firstNameAliases": ["Harry"],
        "lastNameAliases": ["June"],
        "dateOfBirthAliases": ["1992-03-03"],
        "postcodes": ["B10 1EJ"],
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

    def test_valid_request_calls_search(
        self,
        call_endpoint: Callable,
        search_request_json: dict,
        mock_search_results: AsyncMock,
        mock_db_connection: Mock,
    ) -> None:
        # The search service is mocked: this test checks that the route parses the request and passes it on.
        mock_search_results.return_value = []

        response = call_endpoint("post", ROUTE, roles=[Roles.ROLE_PERSON_MATCH], json=search_request_json)

        assert response.status_code == 200
        assert response.json() == []
        mock_search_results.assert_awaited_once()
        assert mock_search_results.await_args.args[0].full_name == "Henry Ahmed Junaed"
        assert mock_search_results.await_args.args[0].postcodes == ["B10 1EJ"]
        assert mock_search_results.await_args.args[1] is mock_db_connection

    def test_invalid_request_does_not_start_search(
        self,
        call_endpoint: Callable,
        mock_search_results: AsyncMock,
    ) -> None:
        response = call_endpoint("post", ROUTE, roles=[Roles.ROLE_PERSON_MATCH], json=None)

        assert response.status_code == 400
        assert response.json()["detail"] == "Invalid request."
        mock_search_results.assert_not_awaited()

    def test_unsupported_search_field_does_not_start_search(
        self,
        call_endpoint: Callable,
        search_request_json: dict,
        mock_search_results: AsyncMock,
    ) -> None:
        search_request_json["pncs"] = ["2000/1234567A"]

        response = call_endpoint("post", ROUTE, roles=[Roles.ROLE_PERSON_MATCH], json=search_request_json)

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
