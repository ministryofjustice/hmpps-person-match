import datetime
from collections.abc import Callable, Generator
from unittest.mock import AsyncMock, Mock, patch

import pytest
from fastapi.testclient import TestClient

from hmpps_cpr_splink.cpr_splink.interface.emdi_exact_match import InvalidEmdiExactMatchInputError
from hmpps_person_match.domain.roles import Roles
from hmpps_person_match.domain.telemetry_events import TelemetryEvents
from hmpps_person_match.models.person.emdi_exact_match_request import EmdiExactMatchRequest
from hmpps_person_match.routes.person.search.emdi_exact_match import ROUTE


@pytest.fixture()
def emdi_exact_match_request_json() -> dict:
    return {
        "firstName": "Henry",
        "lastName": "Junaed",
        "dateOfBirth": "1992-03-02",
        "pnc": "2000/1234567A",
    }


class TestEmdiExactMatchRoute:
    @staticmethod
    @pytest.fixture(autouse=True)
    def mock_emdi_exact_match() -> Generator[AsyncMock]:
        with patch(
            "hmpps_cpr_splink.cpr_splink.interface.emdi_exact_match.emdi_exact_match",
            new_callable=AsyncMock,
        ) as mocked_lookup:
            yield mocked_lookup

    @pytest.mark.parametrize("match_ids", [["ec30e2d2-b4c2-4c42-9e14-514aa58edff5"], []])
    def test_valid_request(
        self,
        call_endpoint: Callable,
        mock_emdi_exact_match: AsyncMock,
        mock_db_connection: Mock,
        mock_logger: Mock,
        match_ids: list[str],
    ) -> None:
        mock_emdi_exact_match.return_value = match_ids
        request = EmdiExactMatchRequest(
            firstName="Henry",
            lastName="Junaed",
            dateOfBirth=datetime.date(1992, 3, 2),
            pnc="2000/1234567A",
        )

        response = call_endpoint(
            "post",
            ROUTE,
            roles=[Roles.ROLE_PERSON_MATCH],
            json=request.model_dump(mode="json", by_alias=True),
        )

        assert response.status_code == 200
        assert response.json() == {"matchIds": match_ids}
        mock_emdi_exact_match.assert_awaited_once_with(request, mock_db_connection)
        mock_logger.info.assert_called_once_with(
            TelemetryEvents.EMDI_EXACT_MATCH_COMPLETED,
            extra={"candidate_size": len(match_ids)},
        )

    @pytest.mark.parametrize("field", ["firstName", "lastName", "dateOfBirth", "pnc"])
    def test_missing_field_does_not_start_lookup(
        self,
        call_endpoint: Callable,
        emdi_exact_match_request_json: dict,
        mock_emdi_exact_match: AsyncMock,
        field: str,
    ) -> None:
        del emdi_exact_match_request_json[field]

        response = call_endpoint("post", ROUTE, roles=[Roles.ROLE_PERSON_MATCH], json=emdi_exact_match_request_json)

        assert response.status_code == 400
        assert response.json()["detail"] == "Invalid request."
        mock_emdi_exact_match.assert_not_awaited()

    @pytest.mark.parametrize(
        ("field", "value"),
        [(field, value) for field in ("firstName", "lastName", "pnc") for value in ("", " \t ", None)]
        + [("dateOfBirth", "not-a-date"), ("dateOfBirth", None), ("unsupported", "value")],
    )
    def test_invalid_field_does_not_start_lookup(
        self,
        call_endpoint: Callable,
        emdi_exact_match_request_json: dict,
        mock_emdi_exact_match: AsyncMock,
        field: str,
        value: str | None,
    ) -> None:
        emdi_exact_match_request_json[field] = value

        response = call_endpoint("post", ROUTE, roles=[Roles.ROLE_PERSON_MATCH], json=emdi_exact_match_request_json)

        assert response.status_code == 400
        assert response.json()["detail"] == "Invalid request."
        mock_emdi_exact_match.assert_not_awaited()

    def test_missing_body_does_not_start_lookup(
        self,
        call_endpoint: Callable,
        mock_emdi_exact_match: AsyncMock,
    ) -> None:
        response = call_endpoint("post", ROUTE, roles=[Roles.ROLE_PERSON_MATCH], json=None)

        assert response.status_code == 400
        mock_emdi_exact_match.assert_not_awaited()

    def test_invalid_cleaned_input(
        self,
        call_endpoint: Callable,
        emdi_exact_match_request_json: dict,
        mock_emdi_exact_match: AsyncMock,
    ) -> None:
        mock_emdi_exact_match.side_effect = InvalidEmdiExactMatchInputError()

        response = call_endpoint("post", ROUTE, roles=[Roles.ROLE_PERSON_MATCH], json=emdi_exact_match_request_json)

        assert response.status_code == 400
        assert response.json()["detail"] == "Invalid request."

    def test_no_auth_returns_unauthorized(self, client: TestClient, mock_emdi_exact_match: AsyncMock) -> None:
        response = client.post(ROUTE, json={})

        assert response.status_code == 401
        assert response.json()["detail"] == "Not authenticated"
        mock_emdi_exact_match.assert_not_awaited()

    def test_invalid_role_returns_forbidden(self, call_endpoint: Callable, mock_emdi_exact_match: AsyncMock) -> None:
        response = call_endpoint("post", ROUTE, roles=["Invalid Role"], json={})

        assert response.status_code == 403
        assert response.json()["detail"] == "You do not have permission to access this resource."
        mock_emdi_exact_match.assert_not_awaited()
