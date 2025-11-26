import datetime
from collections.abc import Callable, Generator
from unittest.mock import AsyncMock, Mock, patch

import jwt
import pytest
import requests_mock
from authlib.jose import JsonWebKey, Key, RSAKey
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import FastAPI
from fastapi.testclient import TestClient

from hmpps_person_match.app import PersonMatchApplication
from hmpps_person_match.db import get_db_session
from hmpps_person_match.dependencies.logger.log import get_logger
from hmpps_person_match.utils.environment import EnvVars, get_env_var


@pytest.fixture(autouse=True)
def set_env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_BUILD_NUMBER", "number")
    monkeypatch.setenv("APP_GIT_REF", "ref")
    monkeypatch.setenv("APP_GIT_BRANCH", "branch")
    monkeypatch.setenv("OAUTH_BASE_URL", "http://localhost:5000")
    monkeypatch.setenv("OAUTH_ISSUER_URL_KEY", "http://localhost:5000")


@pytest.fixture()
def app() -> Generator[FastAPI]:
    app = PersonMatchApplication().app
    # other setup can go here
    yield app
    # clean up / reset resources here


@pytest.fixture(autouse=True)
def mock_db_connection(app: FastAPI) -> Generator[Mock]:
    mock_connection = AsyncMock()
    app.dependency_overrides[get_db_session] = lambda: mock_connection
    yield mock_connection


@pytest.fixture(autouse=True)
def mock_logger(app: FastAPI) -> Generator[Mock]:
    mock_logger = Mock()
    app.dependency_overrides[get_logger] = lambda: mock_logger
    yield mock_logger


@pytest.fixture()
def client(app: FastAPI) -> TestClient:
    return TestClient(app)


@pytest.fixture(autouse=True)
def disable_cache() -> Generator:
    with patch("hmpps_person_match.dependencies.auth.jwks.jwks_cache", {}):
        yield


class TestContext:
    DEFAULT_KID = "test_kid"

    def __init__(self) -> None:
        self.kid = self.DEFAULT_KID
        self.issuer = f"{get_env_var(EnvVars.OAUTH_BASE_URL_KEY)}/auth/issuer"


@pytest.fixture
def context() -> TestContext:
    """
    Returns test context to use throughout app
    """
    return TestContext()


@pytest.fixture(scope="session")
def private_key() -> rsa.RSAPrivateKey:
    """
    Returns a generated private key for testing purposes.
    """
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )
    return private_key


@pytest.fixture(scope="session")
def public_key(private_key: rsa.RSAPrivateKey) -> rsa.RSAPublicKey:
    """
    Returns the public key from the private key for testing purposes.
    """
    public_key = private_key.public_key()
    return public_key


@pytest.fixture
def jwks(context: TestContext, public_key: rsa.RSAPublicKey) -> dict:
    """
    Return a JWKS for testing purposes
    """
    jwk: RSAKey | Key = JsonWebKey.import_key(
        public_key,  # type: ignore
        {
            "kty": "RSA",
            "kid": context.kid,
        },
    )
    jwks = {"keys": [jwk.as_dict()]}
    return jwks


@pytest.fixture
def jwt_token_factory(context: TestContext, private_key: rsa.RSAPrivateKey) -> Callable:
    """
    Returns a JWT token for testing purposes
    """

    def _create_token(
        kid: str = context.kid,
        roles: list[str] | None = None,
        issuer: str = context.issuer,
        expiry: datetime.timedelta = datetime.timedelta(hours=1),
    ) -> str:
        if roles is None:
            roles = []

        headers = {"kid": kid}
        payload = {
            "authorities": roles,
            "exp": datetime.datetime.now().astimezone() + expiry,
            "iss": issuer,
        }
        token = jwt.encode(payload, private_key, algorithm="RS256", headers=headers)
        return token

    return _create_token


@pytest.fixture
def mock_jwks_call_factory(jwks: dict, requests_mock: requests_mock.Mocker) -> Callable:
    """
    Returns a func to create a mock request to JWKS endpoint.
    """

    def _mock_jwks_call(status_code: int = 200, headers: dict | None = None, json_data: dict | None = None) -> None:
        """
        Mock call to JWKS endpoint.
        """
        url = f"{get_env_var(EnvVars.OAUTH_BASE_URL_KEY)}/auth/.well-known/jwks.json"

        if headers is None:
            headers = {"Content-Type": "application/json"}
        if json_data is None:
            json_data = jwks

        requests_mock.get(url, headers=headers, json=json_data, status_code=status_code)

    return _mock_jwks_call


@pytest.fixture()
def mock_jwks(mock_jwks_call_factory: Callable, jwks: dict) -> None:
    """
    Returns a mock JWKS with public key generated.
    """
    default_response_json = jwks
    mock_jwks_call_factory(json_data=default_response_json)
