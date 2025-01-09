import os

import pytest
import requests
import requests_mock
from requests.exceptions import ConnectionError as ConnError
from requests.exceptions import Timeout

from hmpps_person_match.utils.jwks import JWKS


class TestJwks:
    """
    Test JWKS class
    """

    def test_retrieves_jwks(self, jwt_token_factory, mock_jwks):
        """
        Test the JWKS class using a mock JWKS endpoint
        """
        token = jwt_token_factory(roles=["test_role"])
        public_key = JWKS().get_public_key_from_jwt(token)
        assert public_key is not None
        assert public_key.kid == "test_kid"
        assert public_key.kty == "RSA"

    def test_missing_oauth_env_var_throws_error(self):
        """
        Test that an error is raised when the OAUTH_BASE_URL environment variable is missing
        """
        os.environ.pop("OAUTH_BASE_URL", None)
        with pytest.raises(ValueError) as e:
            JWKS()
        assert str(e.value) == "Missing environment variable: OAUTH_BASE_URL"

    def test_raises_error_no_key_found(self, jwt_token_factory, mock_jwks):
        """
        Test that an error is raised when a public key for a specific kid is not found
        """
        token = jwt_token_factory(kid="invalid_kid")
        with pytest.raises(ValueError) as e:
            JWKS().get_public_key_from_jwt(token)
        assert str(e.value) == "Public key for kid: 'invalid_kid' not found."

    @pytest.mark.parametrize("status_code", [429, 500, 502, 503, 504])
    def test_raises_error_from_jwks_endpoint(self, status_code, jwt_token_factory, mock_jwks_call_factory):
        """
        Test that an error is raised when the JWKS endpoint returns an error response
        """
        mock_jwks_call_factory(status_code=status_code)
        token = jwt_token_factory()
        with pytest.raises(requests.exceptions.HTTPError):
            JWKS().get_public_key_from_jwt(token)

    @pytest.mark.parametrize("status_code", [429, 500, 502, 503, 504])
    def test_retry_on_error_from_jwks_endpoint(self, status_code, jwt_token_factory, jwks):
        """
        Test that an error is raised is retried and succeeds
        """
        token = jwt_token_factory()
        with requests_mock.Mocker() as mock:
            response_list = [
                {"status_code": status_code, "headers": {"Content-Type": "application/json"}},
                {"status_code": status_code, "headers": {"Content-Type": "application/json"}},
                {"status_code": 200, "headers": {"Content-Type": "application/json"}, "json": jwks},
            ]
            mock_requests = mock.get(f"{os.environ.get("OAUTH_BASE_URL")}/auth/.well-known/jwks.json", response_list)

            jwk = JWKS().get_public_key_from_jwt(token)
            assert jwk is not None
            assert mock_requests.call_count == 3

    @pytest.mark.parametrize("exception", [ConnError, Timeout])
    def test_retry_on_request_exceptions(self, exception, jwt_token_factory, jwks):
        """
        Test that an error is raised is retried and succeeds
        """
        token = jwt_token_factory()
        with requests_mock.Mocker() as mock:
            response_list = [
                {"exc": exception},
                {"exc": exception},
                {"status_code": 200, "headers": {"Content-Type": "application/json"}, "json": jwks},
            ]
            mock_requests = mock.get(f"{os.environ.get("OAUTH_BASE_URL")}/auth/.well-known/jwks.json", response_list)

            jwk = JWKS().get_public_key_from_jwt(token)
            assert jwk is not None
            assert mock_requests.call_count == 3
