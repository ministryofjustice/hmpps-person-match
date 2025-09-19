import base64

import pytest
import requests

from integration.client import Client


@pytest.fixture
def access_token_factory():
    """
    Factory func to create access token
    """

    def generate_access_token(client: Client):
        """
        Generate access token for testing purposes
        """
        client_secret = "clientsecret"  # noqa: S105
        encoded_auth_basic: bytes = base64.b64encode(f"{client.value}:{client_secret}".encode())
        auth_basic: str = encoded_auth_basic.decode("utf-8")
        headers = {
            "Authorization": f"Basic {auth_basic}",
        }
        params = {
            "grant_type": "client_credentials",
        }
        response = requests.post(
            "http://localhost:9090/auth/oauth/token",
            headers=headers,
            params=params,
            timeout=30,
        )
        if response.status_code == 200:
            return response.json()["access_token"]
        else:
            raise Exception(f"Failed to generate access token: {response.status_code}, {response.text}")

    return generate_access_token


@pytest.fixture()
def call_endpoint(person_match_url, access_token_factory):
    """
    Factory func to call person-match endpoint with access token
    """

    def _call_endpoint(
        method: str,
        route: str,
        client: Client,
        json: dict = None,
    ) -> requests.Response:
        token = access_token_factory(client)
        headers = {"Authorization": f"Bearer {token}"}
        return requests.request(method, person_match_url + route, json=json, headers=headers, timeout=30)

    return _call_endpoint
