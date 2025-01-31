
from hmpps_person_match.views.person_match_view import ROUTE
from integration.client import Client


class TestPersonEndpoint:
    """
    Test match view
    """

    VALID_PERSON_DATA = {
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

    def test_complete_message(self, post_to_endpoint):
        """
        Test person endpoint
        """
        response = post_to_endpoint(ROUTE, json=self.VALID_PERSON_DATA, client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 200

    def test_invalid_client_returns_forbidden(self, post_to_endpoint):
        """
        Test person endpoint return 403 forbidden when invalid roles
        """
        response = post_to_endpoint(ROUTE, json=self.VALID_PERSON_DATA, client=Client.HMPPS_TIER)
        assert response.status_code == 403
