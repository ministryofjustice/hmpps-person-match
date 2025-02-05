from hmpps_person_match.domain.roles import Roles
from hmpps_person_match.routes.person.person_delete import ROUTE


class TestPersonDeleteRoute:
    """
    Test Person Delete Route
    """

    def test_delete_message_no_record(self, call_endpoint, mock_db_connection):
        # Mock db response
        mock_db_connection.execute.return_value.rowcount = 0
        json = {
            "matchID": "123",
        }
        response = call_endpoint("delete", ROUTE, roles=[Roles.ROLE_PERSON_MATCH], json=json)
        assert response.status_code == 404
        assert response.json() == {}

    def test_delete_message(self, call_endpoint, mock_db_connection):
        # Mock db response
        mock_db_connection.execute.return_value.rowcount = 1
        json = {
            "matchID": "123",
        }
        response = call_endpoint("delete", ROUTE, roles=[Roles.ROLE_PERSON_MATCH], json=json)
        assert response.status_code == 200
        assert response.json() == {}

    def test_delete_message_malformed_data(self, call_endpoint, mock_db_connection):
        json = {
            "identifier": "invalid",
        }
        response = call_endpoint("delete", ROUTE, roles=[Roles.ROLE_PERSON_MATCH], json=json)
        assert response.status_code == 400
        assert response.json()["errors"] == [
            {
                "input": {
                    "identifier": "invalid",
                },
                "loc": [
                    "body",
                    "matchID",
                ],
                "msg": "Field required",
                "type": "missing",
            },
        ]

    def test_delete_no_auth_returns_unauthorized(self, client):
        response = client.delete(ROUTE)
        assert response.status_code == 403
        assert response.json()["detail"] == "Not authenticated"

    def test_delete_invalid_role_unauthorized(self, call_endpoint):
        response = call_endpoint("delete", ROUTE, roles=["Invalid Role"], json={})
        assert response.status_code == 403
        assert response.json()["detail"] == "You do not have permission to access this resource."
