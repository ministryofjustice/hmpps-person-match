import uuid

from hmpps_person_match.domain.roles import Roles
from hmpps_person_match.routes.person.migration.person_migrate import ROUTE


class TestPersonRoute:
    """
    Test Person Create Route
    """

    def test_batch_message_one_record(self, call_endpoint, mock_db_connection):
        data = {
            "records": [self._create_person_data()],
        }
        response = call_endpoint("post", ROUTE, roles=[Roles.ROLE_PERSON_MATCH], json=data)
        assert response.status_code == 200
        assert response.json() == {}

    def test_batch_message_one_thosand_records(self, call_endpoint, mock_db_connection):
        data = {
            "records": [self._create_person_data() for _ in range(1000)],
        }
        response = call_endpoint("post", ROUTE, roles=[Roles.ROLE_PERSON_MATCH], json=data)
        assert response.status_code == 200
        assert response.json() == {}

    def test_batch_message_no_records(self, call_endpoint, mock_db_connection):
        data = {
            "records": [],
        }
        response = call_endpoint("post", ROUTE, roles=[Roles.ROLE_PERSON_MATCH], json=data)
        assert response.status_code == 400
        assert response.json()["detail"] == "Invalid request."
        assert response.json()["errors"][0]["msg"] == "List should have at least 1 item after validation, not 0"

    def test_batch_message_more_than_thousand_records(self, call_endpoint, mock_db_connection):
        data = {
            "records": [self._create_person_data() for _ in range(1005)],
        }
        response = call_endpoint("post", ROUTE, roles=[Roles.ROLE_PERSON_MATCH], json=data)
        assert response.status_code == 400
        assert response.json()["detail"] == "Invalid request."
        assert response.json()["errors"][0]["msg"] == "List should have at most 1000 items after validation, not 1005"

    def test_invalid_role_unauthorized(self, call_endpoint):
        response = call_endpoint("post", ROUTE, roles=["Invalid Role"], json={})
        assert response.status_code == 403
        assert response.json()["detail"] == "You do not have permission to access this resource."

    def test_no_auth_returns_unauthorized(self, client):
        response = client.post(ROUTE, json={})
        assert response.status_code == 403
        assert response.json()["detail"] == "Not authenticated"

    @staticmethod
    def _create_person_data():
        return {
            "matchID": str(uuid.uuid4()),
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
