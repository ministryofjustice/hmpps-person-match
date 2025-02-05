class TestApiDocsView:
    """
    Test info view
    """

    def test_response_ok(self, client):
        """
        Test a get to the OpenAPI Docs endpoint returns a 200 ok
        """
        response = client.get("/swagger-ui.html")
        assert response.status_code == 200
