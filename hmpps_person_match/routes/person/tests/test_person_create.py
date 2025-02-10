from hmpps_person_match.domain.roles import Roles
from hmpps_person_match.routes.person.person_create import ROUTE


class TestPersonRoute:
    """
    Test Person Create Route
    """

    def test_complete_message(self, call_endpoint, mock_db_connection):
        json = {
            "matchId": "M1",
            "sourceSystem": "DELIUS",
            "firstName": "Henry",
            "middleNames": "Ahmed",
            "lastName": "Junaed",
            "crn": "1234",
            "dateOfBirth": "1992-03-02",
            "firstNameAliases": ["Henry"],
            "lastNameAliases": ["Junaed"],
            "dateOfBirthAliases": ["1992-01-01"],
            "postcodes": ["B10 1EJ"],
            "cros": ["4444566"],
            "pncs": ["22224555"],
            "sentenceDates": ["2001-03-01"],
        }
        response = call_endpoint("post", ROUTE, roles=[Roles.ROLE_PERSON_MATCH], json=json)
        assert response.status_code == 200
        assert response.json() == {}

    def test_bad_request_on_empty(self, call_endpoint):
        json = None
        response = call_endpoint("post", ROUTE, roles=[Roles.ROLE_PERSON_MATCH], json=json)
        assert response.status_code == 400
        assert response.json()["detail"] == "Invalid request."

    def test_bad_request_different_data_types(self, call_endpoint):
        json = {
            "matchId": "M1",
            "sourceSystem": ["DELIUS", "NOMIS", "COMMON_PLATFORM"],  # Should be string
            "firstName": "Henry",
            "middleNames": "Ahmed",
            "lastName": "Junaed",
            "crn": "1234",
            "dateOfBirth": "1992-03-02",
            "firstNameAliases": ["Henry"],
            "lastNameAliases": ["Junaed"],
            "dateOfBirthAliases": ["1992-01-01"],
            "postcodes": ["B10 1EJ"],
            "cros": ["4444566"],
            "pncs": ["22224555"],
            "sentenceDates": ["2001-03-01"],
        }
        response = call_endpoint("post", ROUTE, roles=[Roles.ROLE_PERSON_MATCH], json=json)
        assert response.status_code == 400
        assert response.json()["detail"] == "Invalid request."
        assert response.json()["errors"] == [
            {
                "type": "string_type",
                "loc": ["body", "sourceSystem"],
                "msg": "Input should be a valid string",
                "input": ["DELIUS", "NOMIS", "COMMON_PLATFORM"],
            },
        ]

    def test_bad_request_on_missing_fields(self, call_endpoint):
        json = {
            "sourceSystem": "DELIUS",
            "firstName": "Henry",
            "middleNames": "Ahmed",
            "lastName": "Junaed",
        }
        response = call_endpoint("post", ROUTE, roles=[Roles.ROLE_PERSON_MATCH], json=json)
        assert response.status_code == 400
        assert response.json()["detail"] == "Invalid request."

    def test_invalid_date_format(self, call_endpoint, mock_db_connection):
        json = {
            "matchId": "M1",
            "sourceSystem": "DELIUS",
            "firstName": "Henry",
            "middleNames": "Ahmed",
            "lastName": "Junaed",
            "crn": "1234",
            "dateOfBirth": "1992/03/02",
            "firstNameAliases": ["Henry"],
            "lastNameAliases": ["Junaed"],
            "dateOfBirthAliases": ["1992-01-01"],
            "postcodes": ["B10 1EJ"],
            "cros": ["4444566"],
            "pncs": ["22224555"],
            "sentenceDates": ["2001-03-01"],
        }
        response = call_endpoint("post", ROUTE, roles=[Roles.ROLE_PERSON_MATCH], json=json)
        assert response.status_code == 400
        assert response.json()["detail"] == "Invalid request."
        assert response.json()["errors"] == [
            {
                "type": "date_from_datetime_parsing",
                "loc": ["body", "dateOfBirth"],
                "msg": "Input should be a valid date or datetime, invalid date separator, expected `-`",
                "input": "1992/03/02",
                "ctx": {
                    "error": "invalid date separator, expected `-`",
                },
            },
        ]

    def test_invalid_role_unauthorized(self, call_endpoint):
        response = call_endpoint("post", ROUTE, roles=["Invalid Role"], json={})
        assert response.status_code == 403
        assert response.json()["detail"] == "You do not have permission to access this resource."

    def test_no_auth_returns_unauthorized(self, client):
        response = client.post(ROUTE, json={})
        assert response.status_code == 403
        assert response.json()["detail"] == "Not authenticated"
