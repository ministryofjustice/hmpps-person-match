from hmpps_person_match.views.person_match_view import ROUTE


class TestPersonMatchView:
    """
    Test match view
    """

    def test_complete_message(self, client):
        response = client.post(ROUTE)
        assert response.status_code == 200
