import datetime
from http import HTTPStatus

from flask import Response

from hmpps_person_match.utils.auth import authorize


class TestAuth:
    """
    Test Auth class
    """

    TEST_ROLE = "ROLE_TEST"

    @authorize(required_roles=[TEST_ROLE])
    def method_with_single_role(self):
        return Response(status=HTTPStatus.OK)

    @authorize()
    def method_with_no_roles(self):
        return Response(status=HTTPStatus.OK)

    def test_allows_correct_level_of_auth(self, app, jwt_token_factory, mock_jwks):
        """
        Test that method with single role is accessible when authenticated with correct role
        """
        token = jwt_token_factory(roles=[self.TEST_ROLE])
        token_header = {"Authorization": f"Bearer {token}"}
        with app.test_request_context(headers=token_header):
            response = self.method_with_single_role()
            assert response.status_code == HTTPStatus.OK

    def test_allows_correct_level_of_auth_user_has_multiple_roles(self, app, jwt_token_factory, mock_jwks):
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
        with app.test_request_context(headers=token_header):
            response = self.method_with_single_role()
            assert response.status_code == HTTPStatus.OK

    def test_allows_correct_level_of_auth_no_roles(self, app, jwt_token_factory, mock_jwks):
        """
        Test that method with no role required is accessible when authenticated
        """
        token = jwt_token_factory(roles=[])
        token_header = {"Authorization": f"Bearer {token}"}
        with app.test_request_context(headers=token_header):
            response = self.method_with_no_roles()
            assert response.status_code == HTTPStatus.OK

    def test_unauthorised_when_expired_token(self, app, jwt_token_factory, mock_jwks):
        """
        Test that method with expired token throws unauthorized exception
        """
        token = jwt_token_factory(roles=[self.TEST_ROLE], expiry=datetime.timedelta(seconds=-1))
        token_header = {"Authorization": f"Bearer {token}"}
        with app.test_request_context(headers=token_header):
            response = self.method_with_single_role()
            assert response.status_code == HTTPStatus.UNAUTHORIZED
            assert response.json["timestamp"] is not None
            assert response.json["error_message"] == "Invalid or expired token."

    def test_forbidden_when_user_does_not_have_role(self, app, jwt_token_factory, mock_jwks):
        """
        Test that method with roles is called with a user that does not have the role
        Forbidden exception is raised
        """
        token = jwt_token_factory(roles=["DIFFERENT_ROLE"])
        token_header = {"Authorization": f"Bearer {token}"}
        with app.test_request_context(headers=token_header):
            response = self.method_with_single_role()
            assert response.status_code == HTTPStatus.FORBIDDEN
            assert response.json["error_message"] == "You do not have permission to access this resource."

    def test_unauthorized_when_wrong_(self, app, jwt_token_factory, mock_jwks):
        """
        Test that token with wrong issuer is not authorized
        """
        token = jwt_token_factory(roles=[self.TEST_ROLE], issuer="invalid issuer")
        token_header = {"Authorization": f"Bearer {token}"}
        with app.test_request_context(headers=token_header):
            response = self.method_with_single_role()
            assert response.status_code == HTTPStatus.UNAUTHORIZED
            assert response.json["error_message"] == "Invalid or expired token."
