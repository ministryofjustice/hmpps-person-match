from sqlalchemy.exc import SQLAlchemyError

from hmpps_person_match.views.health_view import ROUTE


class TestHealthView:
    """
    Test health view
    """

    def test_response_ok(self, client, mock_db_connection):
        """
        Test a get to the health endpoint returns a 200 ok
        """
        # Mock db response
        mock_db_connection.execute.return_value.first.return_value = 1

        response = client.get(ROUTE)
        assert response.status_code == 200
        assert response.headers.get("Content-Type") == "application/json"
        assert response.json() == {"status": "UP"}

    def test_response_no_db_con(self, client, mock_db_connection):
        """
        Test a get to the health endpoint returns a 503
        When no db connection
        """
        # Mock db response
        mock_db_connection.execute.side_effect = SQLAlchemyError("Mocked error", None, None)

        response = client.get(ROUTE)
        assert response.status_code == 503
        assert response.headers.get("Content-Type") == "application/json"
        assert response.json() == {"status": "DOWN"}
