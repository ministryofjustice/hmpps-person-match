import datetime

from hmpps_person_match.views.person_view import ROUTE
from integration.client import Client


class TestPersonEndpoint:
    """
    Test match view
    """

    @staticmethod
    def create_person_data(uuid: str):
        return {
            "id": uuid,
            "matchID": uuid,
            "sourceSystem": "DELIUS",
            "firstName": "Henry",
            "middleNames": "Ahmed",
            "lastName": "Junaed",
            "crn": "1234",
            "dateOfBirth": "1992-03-02",
            "firstNameAliases": ["Henry"],
            "lastNameAliases": ["Junaed"],
            "dateOfBirthAliases": ["1992-01-01"],
            "postcodes": ["B10 1EJ"],
            "cros": ["4444566"],
            "pncs": ["22224555"],
            "sentenceDates": ["2001-03-01"],
        }

    async def test_clean_and_store_message(self, post_to_endpoint, generate_uuid, db):
        """
        Test person cleaned and stored on person endpoint
        """
        person_id = generate_uuid
        data = self.create_person_data(person_id)
        response = post_to_endpoint(ROUTE, json=data, client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 200
        row = await db.fetchrow(f"SELECT * FROM personmatch.person WHERE id = '{person_id}'")
        assert row["id"] == person_id
        assert row["match_id"] == person_id
        assert row["name_1_std"] == "HENRY"
        assert row["name_2_std"] == "AHMED"
        assert row["name_3_std"] is None
        assert row["last_name_std"] == "JUNAED"
        assert row["first_and_last_name_std"] == "HENRY JUNAED"
        assert row["forename_std_arr"] == ["HENRY"]
        assert row["last_name_std_arr"] == ["JUNAED"]
        assert row["sentence_date_single"] == datetime.date(2001, 3, 1)
        assert row["sentence_date_arr"] == [datetime.date(2001, 3, 1)]
        assert row["date_of_birth"] == datetime.date(1992, 3, 2)
        assert row["date_of_birth_arr"] == [datetime.date(1992, 1, 1), datetime.date(1992, 3, 2)]
        assert row["postcode_arr"] == ["B101EJ"]
        assert row["postcode_outcode_arr"] == ["B10"]
        assert row["cro_single"] == "4444566"
        assert row["pnc_single"] == "22224555"
        assert row["source_system"] == "DELIUS"

    async def test_clean_and_update_message(self, post_to_endpoint, generate_uuid, db):
        """
        Test person cleaned and update existing person on person endpoint
        """
        # Create person
        person_id = generate_uuid
        data = self.create_person_data(person_id)
        response = post_to_endpoint(ROUTE, json=data, client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 200

        data = self.create_person_data(person_id)
        # Update person
        data["firstName"] = "andrew"
        data["firstNameAliases"] = ["andy"]
        data["dateOfBirthAliases"] = ["1980-01-01"]
        data["postcodes"] = ["a34 8fr"]
        response = post_to_endpoint(ROUTE, json=data, client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 200
        row = await db.fetchrow(f"SELECT * FROM personmatch.person WHERE id = '{person_id}'")
        assert row["id"] == person_id
        assert row["match_id"] == person_id
        assert row["name_1_std"] == "ANDREW"
        assert row["name_2_std"] == "AHMED"
        assert row["name_3_std"] is None
        assert row["last_name_std"] == "JUNAED"
        assert row["first_and_last_name_std"] == "ANDREW JUNAED"
        assert row["forename_std_arr"] == ["ANDREW", "ANDY"]
        assert row["last_name_std_arr"] == ["JUNAED"]
        assert row["sentence_date_single"] == datetime.date(2001, 3, 1)
        assert row["sentence_date_arr"] == [datetime.date(2001, 3, 1)]
        assert row["date_of_birth"] == datetime.date(1992, 3, 2)
        assert row["date_of_birth_arr"] == [datetime.date(1980, 1, 1), datetime.date(1992, 3, 2)]
        assert row["postcode_arr"] == ["A348FR"]
        assert row["postcode_outcode_arr"] == ["A34"]
        assert row["cro_single"] == "4444566"
        assert row["pnc_single"] == "22224555"
        assert row["source_system"] == "DELIUS"

    def test_invalid_client_returns_forbidden(self, post_to_endpoint, generate_uuid):
        """
        Test person endpoint return 403 forbidden when invalid roles
        """
        data = self.create_person_data(generate_uuid)
        response = post_to_endpoint(ROUTE, json=data, client=Client.HMPPS_TIER)
        assert response.status_code == 403
