from hmpps_person_match.views.base_view import BaseView


class HealthView(BaseView):
    """
    Health View
    """

    ROUTE = "/health"

    def get(self):
        """
        GET request handler
        """
        return {"status": "UP"}
