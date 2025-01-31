import pytest
import requests


@pytest.fixture
def get_access_token():
    INVALID_CLIENT = "aG1wcHMtdGllcjpjbGllbnRzZWNyZXQ=" # hmpps-tier client id + secret | base64
    """
    Generate access token for testing purposes
    """
    headers = {
        "Authorization": f"Basic {INVALID_CLIENT}",
    }
    params = {
        "grant_type": "client_credentials",
    }
    response = requests.post("http://localhost:8080/auth/oauth/token", headers=headers, params=params, timeout=30)
    return response.json()["access_token"]
