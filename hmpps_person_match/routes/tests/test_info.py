from fastapi.testclient import TestClient

from hmpps_person_match.routes.info import ROUTE


class TestInfoView:
    """
    Test info view
    """

    def test_response_ok(self, client: TestClient) -> None:
        """
        Test a get to the info endpoint returns a 200 ok
        """
        response = client.get(ROUTE)
        assert response is not None
        assert response.headers.get("Content-Type") == "application/json"
        assert response.status_code == 200
        assert response.json()["build"]["version"] == "number"
        assert response.json()["git"]["commit"]["id"] == "ref"
        assert response.json()["git"]["branch"] == "branch"
