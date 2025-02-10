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

    async def test_person_deletion(self, call_endpoint, person_id, db, create_person_record):
        """
        Test person cleaned and stored on person endpoint
        """
        # Create a new person
        create_person_record(Person(
            matchId=person_id,
            sourceSystem="DELIUS",
            firstName="Henry",
            middleNames="Ahmed",
            lastName="Junaed",
            crn="1234",
            dateOfBirth="1992-03-02",
            firstNameAliases=["Henry"],
            lastNameAliases=["Junaed"],
            dateOfBirthAliases=["1992-01-01"],
            postcodes=["B10 1EJ"],
            cros=["4444566"],
            pncs=["22224555"],
            sentenceDates=["2001-03-01"],
        ))

        result = await db.fetch(f"SELECT * FROM personmatch.person WHERE match_id = '{person_id}'")
        assert len(result) == 1

        data = self.create_person_id_data(person_id)
        response = call_endpoint("delete", ROUTE, json=data, client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 200

        result = await db.fetch(f"SELECT * FROM personmatch.person WHERE match_id = '{person_id}'")
        assert len(result) == 0

    async def test_person_deletion_no_record(self, call_endpoint, person_id):
        """
        Test person cleaned and stored on person endpoint
        """
        data = self.create_person_id_data(person_id)

        response = call_endpoint("delete", ROUTE, json=data, client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 404

    def test_invalid_client_returns_forbidden(self, call_endpoint, person_id):
        """
        Test person endpoint return 403 forbidden when invalid roles
        """
        data = self.create_person_id_data(person_id)
        response = call_endpoint("delete", ROUTE, json=data, client=Client.HMPPS_TIER)
        assert response.status_code == 403
