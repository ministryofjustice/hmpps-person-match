import requests


class TestMonitoring:
    """
    Test monitoring endpoints
    """

    def test_health(self, person_match_url):
        """
        Test health endpont
        """
        response = requests.get(f"{person_match_url}/health")
        assert response.status_code == 200
        assert response.json() == {"status": "UP"}

    def test_info(self, person_match_url):
        """
        Test info endpoint
        """
        response = requests.get(f"{person_match_url}/info")
        assert response.status_code == 200
        assert response.json()["version"] == "local"
        assert response.json()["commit_id"] == "ref"
        assert response.json()["branch"] == "branch"
