import pytest
from fastapi import Response


@pytest.fixture()
def call_endpoint(client, jwt_token_factory, mock_jwks):
    def _call_endpoint(
        method: str,
        route: str,
        json: dict = None,
        roles: list[str] = None,
    ) -> Response:
        token = jwt_token_factory(roles=roles)
        headers = {"Authorization": f"Bearer {token}"}
        return client.request(method, route, json=json, headers=headers)

    return _call_endpoint
