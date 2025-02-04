from hmpps_person_match.views.person_view import ROUTE
from integration.client import Client


class TestPersonDeletionEndpoint:
    """
    Test person deletion
    """

    @staticmethod
    def create_person_id_data(uuid: str):
        return {
            "id": uuid,
        }

    async def test_person_deletion_no_record(self, call_endpoint, generate_uuid, db):
        """
        Test person cleaned and stored on person endpoint
        """
        person_id = generate_uuid
        data = self.create_person_id_data(person_id)

        response = call_endpoint("delete", ROUTE, json=data, client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 404

    def test_invalid_client_returns_forbidden(self, call_endpoint, generate_uuid):
        """
        Test person endpoint return 403 forbidden when invalid roles
        """
        data = self.create_person_id_data(generate_uuid)
        response = call_endpoint("delete", ROUTE, json=data, client=Client.HMPPS_TIER)
        assert response.status_code == 403
