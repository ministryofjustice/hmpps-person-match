import pytest
from flask import Response


@pytest.fixture(scope="module")
def client(app):
    return app.test_client()


@pytest.fixture()
def post_to_endpoint(client, jwt_token_factory, mock_jwks):
    def _call_endpoint(
        route: str,
        json: dict,
        roles: list[str] = None,
    ) -> Response:
        token = jwt_token_factory(roles=roles)
        headers = {"Authorization": f"Bearer {token}"}
        return client.post(route, json=json, headers=headers)

    return _call_endpoint
