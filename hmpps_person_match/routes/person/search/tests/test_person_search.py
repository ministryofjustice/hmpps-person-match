from collections.abc import Callable

from fastapi.testclient import TestClient

from hmpps_person_match.domain.roles import Roles
from hmpps_person_match.routes.person.search.person_search import ROUTE


class TestPersonSearchRoute:
    def test_person_search_success(self, call_endpoint: Callable) -> None:
        response = call_endpoint(
            "post",
            ROUTE,
            roles=[Roles.ROLE_PERSON_MATCH],
            json=self._search_payload(),
        )

        assert response.status_code == 200
        assert response.json() == []

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
