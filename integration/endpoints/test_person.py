import requests

from hmpps_person_match.domain.roles import Roles
from hmpps_person_match.views.person_match_view import ROUTE


class TestPersonEndpoint:
    """
    Test match view
    """

    INVALID_CLIENT = "aG1wcHMtdGllcjokMmEkMTAkbEJ3YnppUWxMZmlDbm44S2oxUGZNdWpFY0xkc0pZbFlTTkp2QlJPNjM4Z0NZVFM5eU4weG0="

    def test_complete_message(self):
        headers = {
            "Authorization": f"Basic {self.INVALID_CLIENT}",
        }
        params = {
            "grant_type": "client_credentials",
        }
        response = requests.post("http://localhost:8080/auth/oauth/token", headers=headers, params=params)
        assert response.status_code == 200
