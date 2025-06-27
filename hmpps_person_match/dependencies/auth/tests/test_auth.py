import datetime
from http import HTTPStatus

import pytest
from fastapi import Depends, FastAPI, Response
from fastapi.testclient import TestClient

from hmpps_person_match.dependencies.auth.jwt_bearer import JWTBearer


class TestAuth:
    """
    Test Auth class
    """

    TEST_ROLE = "ROLE_TEST"
    TEST_ROLE_2 = "ROLE_TEST_EXTRA"

    SINGLE_ROLE_ROUTE = "/single_role"
    NO_ROLE_ROUTE = "/no_role"
    MULTIPLE_ROLE_ROUTE = "/multiple_roles"

    app = FastAPI()

    @app.get(SINGLE_ROLE_ROUTE, dependencies=[Depends(JWTBearer(required_roles=[TEST_ROLE]))])
    def method_with_single_role():
        return Response(status_code=HTTPStatus.OK)

    @app.get(MULTIPLE_ROLE_ROUTE, dependencies=[Depends(JWTBearer(required_roles=[TEST_ROLE, TEST_ROLE_2]))])
    def method_with_multiple_role():
        return Response(status_code=HTTPStatus.OK)

    @app.get(NO_ROLE_ROUTE)
    def method_with_no_roles():
        return Response(status_code=HTTPStatus.OK)

    @pytest.fixture()
    def test_app(self):
        return TestClient(self.app)

    def test_allows_correct_level_of_auth(self, test_app, jwt_token_factory, mock_jwks):
        """
        Test that method with single role is accessible when authenticated with correct role
        """
        token = jwt_token_factory(roles=[self.TEST_ROLE])
        token_header = {"Authorization": f"Bearer {token}"}

        response = test_app.get(self.SINGLE_ROLE_ROUTE, headers=token_header)
        assert response.status_code == HTTPStatus.OK

    def test_allows_correct_level_of_auth_user_has_multiple_roles_on_endpoints_that_accepts_multi_roles(
        self, test_app, jwt_token_factory, mock_jwks,
    ):
        """
        Test that method with multiple role is accessible when authenticated with user that has multiple roles
        """
        token = jwt_token_factory(
            roles=[
                self.TEST_ROLE,
                "ROLE_EXTRA_ROLE_1",
                "ROLE_EXTRA_ROLE_2",
            ],
        )
        token_header = {"Authorization": f"Bearer {token}"}

        response = test_app.get(self.MULTIPLE_ROLE_ROUTE, headers=token_header)
        assert response.status_code == HTTPStatus.OK

    def test_allows_correct_level_of_auth_user_has_multiple_roles(self, test_app, jwt_token_factory, mock_jwks):
        """
        Test that method with single role is accessible when authenticated with user that has multiple roles
        """
        token = jwt_token_factory(
            roles=[
                self.TEST_ROLE,
                "ROLE_EXTRA_ROLE_1",
                "ROLE_EXTRA_ROLE_2",
            ],
        )
        token_header = {"Authorization": f"Bearer {token}"}

        response = test_app.get(self.SINGLE_ROLE_ROUTE, headers=token_header)
        assert response.status_code == HTTPStatus.OK

    def test_allows_correct_level_of_auth_no_roles(self, test_app, jwt_token_factory, mock_jwks):
        """
        Test that method with no role required is accessible when authenticated
        """
        token = jwt_token_factory(roles=[])
        token_header = {"Authorization": f"Bearer {token}"}

        response = test_app.get(self.NO_ROLE_ROUTE, headers=token_header)
        assert response.status_code == HTTPStatus.OK

    def test_unauthorised_when_expired_token(self, test_app, jwt_token_factory, mock_jwks):
        """
        Test that method with expired token throws unauthorized exception
        """
        token = jwt_token_factory(roles=[self.TEST_ROLE], expiry=datetime.timedelta(seconds=-1))
        token_header = {"Authorization": f"Bearer {token}"}

        response = test_app.get(self.SINGLE_ROLE_ROUTE, headers=token_header)
        assert response.status_code == HTTPStatus.UNAUTHORIZED
        assert response.json()["detail"] == "Invalid or expired token."

    def test_forbidden_when_user_does_not_have_role(self, test_app, jwt_token_factory, mock_jwks):
        """
        Test that method with roles is called with a user that does not have the role
        Forbidden exception is raised
        """
        token = jwt_token_factory(roles=["DIFFERENT_ROLE"])
        token_header = {"Authorization": f"Bearer {token}"}

        response = test_app.get(self.SINGLE_ROLE_ROUTE, headers=token_header)
        assert response.status_code == HTTPStatus.FORBIDDEN
        assert response.json()["detail"] == "You do not have permission to access this resource."

    def test_unauthorized_when_wrong_(self, test_app, jwt_token_factory, mock_jwks):
        """
        Test that token with wrong issuer is not authorized
        """
        token = jwt_token_factory(roles=[self.TEST_ROLE], issuer="invalid issuer")
        token_header = {"Authorization": f"Bearer {token}"}

        response = test_app.get(self.SINGLE_ROLE_ROUTE, headers=token_header)
        assert response.status_code == HTTPStatus.UNAUTHORIZED
        assert response.json()["detail"] == "Invalid or expired token."

    def test_forbidden_when_wrong_auth_scheme(self, test_app, jwt_token_factory, mock_jwks):
        """
        Test that token with wrong auth scheme returns forbidden
        """
        token = jwt_token_factory(roles=[self.TEST_ROLE])
        token_header = {"Authorization": f"Invalid {token}"}

        response = test_app.get(self.SINGLE_ROLE_ROUTE, headers=token_header)
        assert response.status_code == HTTPStatus.FORBIDDEN
        assert response.json()["detail"] == "Invalid authentication credentials"

    def test_forbidden_when_no_token_provided(self, test_app, jwt_token_factory, mock_jwks):
        """
        Test that token with wrong auth scheme returns forbidden
        """
        response = test_app.get(self.SINGLE_ROLE_ROUTE)
        assert response.status_code == HTTPStatus.FORBIDDEN
        assert response.json()["detail"] == "Not authenticated"
