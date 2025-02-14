import base64

import pytest
import requests

from hmpps_person_match.models.person.person import Person
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
            "http://localhost:8080/auth/oauth/token",
            headers=headers,
            params=params,
            timeout=30,
        )
        if response.status_code == 200:
            return response.json()["access_token"]
        else:
            raise Exception(f"Failed to generate access token: {response.status_code}")

    return generate_access_token


@pytest.fixture()
def call_endpoint(person_match_url, access_token_factory):
    """
    Factory func to call person-match endpoint with access token
    """

    def _call_endpoint(
        method: str,
        route: str,
        json: dict,
        client: Client,
    ) -> requests.Response:
        token = access_token_factory(client)
        headers = {"Authorization": f"Bearer {token}"}
        return requests.request(method, person_match_url + route, json=json, headers=headers, timeout=30)

    return _call_endpoint


@pytest.fixture()
def create_person_record(call_endpoint):
    """
    Create a new person
    """

    def _create_and_insert_person(person: Person):
        """
        Commit person into
        """
        response = call_endpoint(
            "post", "/person", json=person.model_dump(mode="json", by_alias=True), client=Client.HMPPS_PERSON_MATCH,
        )
        if response.status_code == 200:
            return person.match_id
        raise Exception(f"Could not create person, {response.status_code}")

    return _create_and_insert_person
