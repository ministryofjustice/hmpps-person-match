import requests

from hmpps_person_match.domain.roles import Roles
from hmpps_person_match.views.person_match_view import ROUTE


class TestPersonEndpoint:
    """
    Test match view
    """

    def test_complete_message(self, get_access_token):
        json = {
            "id": "123",
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
        headers = {"Authorization": f"Bearer {get_access_token}"}
        response = requests.post("http://localhost:5000/person/match", json=json, headers=headers, timeout=30)
        assert response.status_code == 200
