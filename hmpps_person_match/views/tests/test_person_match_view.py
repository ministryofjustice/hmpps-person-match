from hmpps_person_match.domain.roles import Roles
from hmpps_person_match.views.person_match_view import ROUTE


class TestPersonMatchView:
    """
    Test match view
    """

    def test_complete_message(self, post_to_endpoint):
        json = {
            "sourceSystem": "DELIUS",
            "firstName": "Henry",
            "middleNames": "Ahmed",
            "lastName": "Junaed",
            "crn": "1234",
            "dateOfBirth": "01/02/1992",
            "firstNameAliases": ["Henry"],
            "lastNameAliases": ["Junaed"],
            "dateOfBirthAliases": ["01/02/1992"],
            "postcodes": ["B10 1EJ"],
            "cros": ["4444566"],
            "pncs": ["22224555"],
            "sentenceDates": ["02/03/2001"],
        }
        response = post_to_endpoint(ROUTE, roles=[Roles.ROLE_PERSON_MATCH], json=json)
        assert response.status_code == 200
        assert response.json() == {}

    def test_bad_request_on_empty(self, post_to_endpoint):
        json = None
        response = post_to_endpoint(ROUTE, roles=[Roles.ROLE_PERSON_MATCH], json=json)
        assert response.status_code == 400
        assert response.json()["detail"] == "Invalid request"

    def test_bad_request_different_data_types(self, post_to_endpoint):
        json = {
            "sourceSystem": ["DELIUS", "NOMIS", "COMMON_PLATFORM"], # Should be string
            "firstName": "Henry",
            "middleNames": "Ahmed",
            "lastName": "Junaed",
            "crn": "1234",
            "dateOfBirth": "01/02/1992",
            "firstNameAliases": ["Henry"],
            "lastNameAliases": ["Junaed"],
            "dateOfBirthAliases": ["01/02/1992"],
            "postcodes": ["B10 1EJ"],
            "cros": ["4444566"],
            "pncs": ["22224555"],
            "sentenceDates": ["02/03/2001"],
        }
        response = post_to_endpoint(ROUTE, roles=[Roles.ROLE_PERSON_MATCH], json=json)
        assert response.status_code == 400
        assert response.json()["detail"] == "Invalid request"


    def test_bad_request_on_missing_fields(self, post_to_endpoint):
        json = {
            "sourceSystem": "DELIUS",
            "firstName": "Henry",
            "middleNames": "Ahmed",
            "lastName": "Junaed",
        }
        response = post_to_endpoint(ROUTE, roles=[Roles.ROLE_PERSON_MATCH], json=json)
        assert response.status_code == 400
        assert response.json()["detail"] == "Invalid request"

    def test_invalid_role_unauthorized(self, post_to_endpoint):
        response = post_to_endpoint(ROUTE, roles=["Invalid Role"], json={})
        assert response.status_code == 403
        assert response.json()["detail"] == "You do not have permission to access this resource."

    def test_no_auth_returns_unauthorized(self, client):
        response = client.post(ROUTE, json={})
        assert response.status_code == 403
        assert response.json()["detail"] == "Not authenticated"
