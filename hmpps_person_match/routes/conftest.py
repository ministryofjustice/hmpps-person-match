from collections.abc import Callable

import pytest
from fastapi import Response
from fastapi.testclient import TestClient


@pytest.fixture()
def call_endpoint(client: TestClient, jwt_token_factory: Callable, mock_jwks: Callable) -> Callable:
    def _call_endpoint(
        method: str,
        route: str,
        json: dict | None = None,
        roles: list[str] | None = None,
    ) -> Response:
        token = jwt_token_factory(roles=roles)
        headers = {"Authorization": f"Bearer {token}"}
        return client.request(method, route, json=json, headers=headers)

    return _call_endpoint
