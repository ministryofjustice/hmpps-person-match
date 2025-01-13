from hmpps_person_match.views.health_view import ROUTE


class TestHealthView:
    """
    Test health view
    """

    def test_response_ok(self, client):
        """
        Test a get to the health endpoint returns a 200 ok
        """
        response = client.get(ROUTE)
        assert response.status_code == 200
        assert response.headers.get("Content-Type") == "application/json"
        assert response.json() == {"status": "UP"}
