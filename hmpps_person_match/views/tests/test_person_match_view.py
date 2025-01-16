from hmpps_person_match.domain.roles import Roles
from hmpps_person_match.views.person_match_view import ROUTE


class TestPersonMatchView:
    """
    Test match view
    """

    def test_complete_message(self, post_to_endpoint):
        response = post_to_endpoint(ROUTE, roles=[Roles.ROLE_PERSON_MATCH], json={})
        assert response.status_code == 200
        assert response.json() == {}

    def test_invalid_role_unauthorized(self, post_to_endpoint):
        response = post_to_endpoint(ROUTE, roles=["Invalid Role"], json={})
        assert response.status_code == 403
        assert response.json()["detail"] == "You do not have permission to access this resource."

    def test_no_auth_returns_unauthorized(self, client):
        response = client.post(ROUTE, json={})
        assert response.status_code == 403
        assert response.json()["detail"] == "Not authenticated"
