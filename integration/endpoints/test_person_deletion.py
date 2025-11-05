from collections.abc import Callable

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_person_match.routes.person.person_delete import ROUTE
from integration import random_test_data
from integration.client import Client
from integration.mock_person import MockPerson
from integration.person_factory import PersonFactory


class TestPersonDeletionEndpoint:
    """
    Test person deletion
    """

    @staticmethod
    def create_person_id_data(uuid: str) -> dict:
        return {
            "matchId": uuid,
        }

    async def test_person_deletion(
        self,
        call_endpoint: Callable,
        db_connection: AsyncSession,
        person_factory: PersonFactory,
    ) -> None:
        """
        Test person cleaned and stored on person endpoint
        """
        # Create person
        person = await person_factory.create_from(MockPerson())

        db_result = await db_connection.execute(
            text(f"SELECT * FROM personmatch.person WHERE match_id = '{person.match_id}'"),
        )
        result = db_result.fetchall()
        assert len(result) == 1

        data = self.create_person_id_data(person.match_id)
        response = call_endpoint("delete", ROUTE, json=data, client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 200

        db_result = await db_connection.execute(
            text(f"SELECT * FROM personmatch.person WHERE match_id = '{person.match_id}'"),
        )
        result = db_result.fetchall()
        assert len(result) == 0

    async def test_person_deletion_no_record(self, call_endpoint: Callable) -> None:
        """
        Test person cleaned and stored on person endpoint
        """
        data = self.create_person_id_data(random_test_data.random_match_id())

        response = call_endpoint("delete", ROUTE, json=data, client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 404

    def test_invalid_client_returns_forbidden(self, call_endpoint: Callable) -> None:
        """
        Test person endpoint return 403 forbidden when invalid roles
        """
        data = self.create_person_id_data(random_test_data.random_match_id())
        response = call_endpoint("delete", ROUTE, json=data, client=Client.HMPPS_TIER)
        assert response.status_code == 403
