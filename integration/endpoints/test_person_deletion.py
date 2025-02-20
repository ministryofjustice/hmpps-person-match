from sqlalchemy import text

from hmpps_person_match.models.person.person import Person
from hmpps_person_match.routes.person.person_delete import ROUTE
from integration.client import Client


class TestPersonDeletionEndpoint:
    """
    Test person deletion
    """

    @staticmethod
    def create_person_id_data(uuid: str):
        return {
            "matchId": uuid,
        }

    async def test_person_deletion(
        self,
        call_endpoint,
        match_id,
        db_connection,
        create_person_record,
        create_person_data,
    ):
        """
        Test person cleaned and stored on person endpoint
        """
        # Create person
        await create_person_record(Person(**create_person_data(match_id)))

        result = await db_connection.execute(text(f"SELECT * FROM personmatch.person WHERE match_id = '{match_id}'"))
        result = result.fetchall()
        assert len(result) == 1

        data = self.create_person_id_data(match_id)
        response = call_endpoint("delete", ROUTE, json=data, client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 200

        result = await db_connection.execute(text(f"SELECT * FROM personmatch.person WHERE match_id = '{match_id}'"))
        result = result.fetchall()
        assert len(result) == 0

    async def test_person_deletion_no_record(self, call_endpoint, match_id):
        """
        Test person cleaned and stored on person endpoint
        """
        data = self.create_person_id_data(match_id)

        response = call_endpoint("delete", ROUTE, json=data, client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 404

    def test_invalid_client_returns_forbidden(self, call_endpoint, match_id):
        """
        Test person endpoint return 403 forbidden when invalid roles
        """
        data = self.create_person_id_data(match_id)
        response = call_endpoint("delete", ROUTE, json=data, client=Client.HMPPS_TIER)
        assert response.status_code == 403
