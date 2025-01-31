import pytest
import requests


@pytest.fixture
def get_access_token(self):
    """
    Generate access token for testing purposes
    """
    response = requests.post("http://localhost:8080/auth/oauth/token")