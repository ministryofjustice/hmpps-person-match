import requests


class TestMonitoring:
    """
    Test monitoring endpoints
    """

    def test_health(self, person_match_url: str) -> None:
        """
        Test health endpont
        """
        response = requests.get(f"{person_match_url}/health")
        assert response.status_code == 200
        assert response.json() == {"status": "UP"}

    def test_health_ping(self, person_match_url: str) -> None:
        """
        Test health ping endpont
        """
        response = requests.get(f"{person_match_url}/health/ping")
        assert response.status_code == 200
        assert response.json() == {"status": "UP"}

    def test_info(self, person_match_url: str) -> None:
        """
        Test info endpoint
        """
        response = requests.get(f"{person_match_url}/info")
        assert response.status_code == 200
        assert response.json()["build"]["version"] == "local"
        assert response.json()["git"]["commit"]["id"] == "ref"
        assert response.json()["git"]["branch"] == "branch"
