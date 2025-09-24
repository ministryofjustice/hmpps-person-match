import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_person_match.routes.person.person_create import ROUTE
from integration import random_test_data
from integration.client import Client
from integration.mock_person import MockPerson
from integration.person_factory import PersonFactory
from integration.test_base import IntegrationTestBase


class TestPersonCreationEndpoint(IntegrationTestBase):
    """
    Test person creation
    """

    async def test_clean_and_store_message(
        self,
        call_endpoint,
        db_connection: AsyncSession,
    ):
        """
        Test person cleaned and stored on person endpoint
        """
        person_data = MockPerson()
        response = call_endpoint(
            "post",
            ROUTE,
            json=person_data.model_dump(by_alias=True),
            client=Client.HMPPS_PERSON_MATCH,
        )
        assert response.status_code == 200
        row = await self.find_by_match_id(db_connection, person_data.match_id)
        assert row["match_id"] == person_data.match_id
        assert row["name_1_std"] == person_data.first_name.upper()
        assert row["name_2_std"] == person_data.middle_names.upper()
        assert row["name_3_std"] is None
        assert row["last_name_std"] == person_data.last_name.upper()
        assert row["first_and_last_name_std"] == person_data.first_name.upper() + " " + person_data.last_name.upper()
        assert set(row["forename_std_arr"]) == set(
            [person_data.first_name.upper(), person_data.first_name_aliases[0].upper()],
        )
        assert set(row["last_name_std_arr"]) == set(
            [person_data.last_name.upper(), person_data.last_name_aliases[0].upper()],
        )
        assert row["sentence_date_arr"] == [self.to_datetime_object(person_data.sentence_dates[0])]
        assert row["date_of_birth"] == self.to_datetime_object(person_data.date_of_birth)
        assert set(row["date_of_birth_arr"]) == set(
            [
                self.to_datetime_object(person_data.date_of_birth),
                self.to_datetime_object(person_data.date_of_birth_aliases[0]),
            ],
        )
        assert row["postcode_arr"] == [person_data.postcodes[0].replace(" ", "")]
        assert row["postcode_outcode_arr"] == [person_data.postcodes[0][:3]]
        assert row["cro_single"] == person_data.cros[0]
        assert row["pnc_single"] == person_data.pncs[0]
        assert row["source_system"] == person_data.source_system
        assert row["source_system_id"] == person_data.source_system_id
        assert row["master_defendant_id"] == person_data.master_defendant_id
        assert row["postcode_first"] == person_data.postcodes[0].replace(" ", "")
        assert row["postcode_second"] is None
        assert row["postcode_last"] == person_data.postcodes[0].replace(" ", "")
        assert row["postcode_outcode_first"] == person_data.postcodes[0][:3]
        assert row["postcode_outcode_last"] == person_data.postcodes[0][:3]
        assert row["date_of_birth_last"] == self.to_datetime_object(person_data.date_of_birth_aliases[0])
        assert (
            row["forename_first"] == person_data.first_name_aliases[0].upper()
            or row["forename_first"] == person_data.first_name.upper()
        )
        assert (
            row["forename_last"] == person_data.first_name_aliases[0].upper()
            or row["forename_last"] == person_data.first_name.upper()
        )
        assert (
            row["last_name_first"] == person_data.last_name_aliases[0].upper()
            or row["last_name_first"] == person_data.last_name.upper()
        )
        assert (
            row["last_name_last"] == person_data.last_name_aliases[0].upper()
            or row["last_name_last"] == person_data.last_name.upper()
        )
        assert row["sentence_date_first"] == self.to_datetime_object(person_data.sentence_dates[0])
        assert row["sentence_date_last"] == self.to_datetime_object(person_data.sentence_dates[0])
        assert row["override_marker"] is None
        assert row["override_scopes"] is None

    async def test_clean_and_update_message(
        self,
        call_endpoint,
        db_connection: AsyncSession,
        person_factory: PersonFactory,
    ):
        """
        Test person cleaned and update existing person on person endpoint
        """
        # Create person
        person = await person_factory.create_from(MockPerson())

        # Update person
        updated_first_name = random_test_data.random_name()
        updated_dob = random_test_data.random_date()
        person.first_name = updated_first_name
        person.date_of_birth = updated_dob

        response = call_endpoint(
            "post",
            ROUTE,
            json=person.model_dump(by_alias=True),
            client=Client.HMPPS_PERSON_MATCH,
        )
        assert response.status_code == 200
        row = await self.find_by_match_id(db_connection, person.match_id)
        assert row["match_id"] == person.match_id
        assert row["name_1_std"] == updated_first_name.upper()
        assert row["date_of_birth"] == self.to_datetime_object(updated_dob)

    def test_invalid_client_returns_forbidden(self, call_endpoint):
        """
        Test person endpoint return 403 forbidden when invalid roles
        """
        response = call_endpoint(
            "post",
            ROUTE,
            json=MockPerson(matchId=random_test_data.random_match_id()).model_dump(by_alias=True),
            client=Client.HMPPS_TIER,
        )
        assert response.status_code == 403

    async def test_does_not_create_duplicates_on_source_system_id(
        self,
        call_endpoint,
        db_connection: AsyncSession,
    ):
        """
        Test only unique source system id allowed. Even if match_id is different
        """
        source_system_id = random_test_data.random_source_system_id()
        source_system = random_test_data.random_source_system()

        for _ in range(5):
            person_data = MockPerson(
                matchId=str(uuid.uuid4()),
                sourceSystemId=source_system_id,
                sourceSystem=source_system,
            )
            call_endpoint(
                "post",
                ROUTE,
                json=person_data.model_dump(by_alias=True),
                client=Client.HMPPS_PERSON_MATCH,
            )

        result = await db_connection.execute(
            text(f"SELECT * FROM personmatch.person WHERE source_system_id = '{source_system_id}'"),
        )
        result = result.mappings().fetchall()
        assert len(result) == 1

    async def test_match_id_is_upserted_on_same_source_system_id(
        self,
        call_endpoint,
        db_connection: AsyncSession,
    ):
        """
        Test match id is upserted on conflict with source system id
        """
        source_system_id = random_test_data.random_source_system_id()
        source_system = random_test_data.random_source_system()

        person = MockPerson(sourceSystemId=source_system_id, sourceSystem=source_system)

        call_endpoint(
            "post",
            ROUTE,
            json=person.model_dump(by_alias=True),
            client=Client.HMPPS_PERSON_MATCH,
        )

        updated_last_name = random_test_data.random_name()

        updated_person_data = MockPerson(
            lastName=updated_last_name,
            sourceSystemId=source_system_id,
            sourceSystem=source_system,
        )

        assert updated_person_data.match_id != person.match_id

        call_endpoint(
            "post",
            ROUTE,
            json=updated_person_data.model_dump(by_alias=True),
            client=Client.HMPPS_PERSON_MATCH,
        )

        result = await self.find_by_match_id(db_connection, updated_person_data.match_id)
        assert result["last_name_std"] == updated_last_name.upper()

        assert await self.find_by_match_id(db_connection, person.match_id) is None

    async def test_source_system_id_can_be_same_if_different_source_system(
        self,
        call_endpoint,
        db_connection: AsyncSession,
    ):
        """
        Test unique constraint applies to source system
        """
        source_system_id = random_test_data.random_source_system_id()

        person_1_data = MockPerson(
            sourceSystemId=source_system_id,
            sourceSystem="NOMIS",
        )

        call_endpoint(
            "post",
            ROUTE,
            json=person_1_data.model_dump(by_alias=True),
            client=Client.HMPPS_PERSON_MATCH,
        )

        person_2_data = MockPerson(
            sourceSystemId=source_system_id,
            sourceSystem="DELIUS",
        )

        call_endpoint(
            "post",
            ROUTE,
            json=person_2_data.model_dump(by_alias=True),
            client=Client.HMPPS_PERSON_MATCH,
        )

        record_1 = await self.find_by_match_id(db_connection, person_1_data.match_id)
        assert record_1["source_system_id"] == source_system_id
        assert record_1["source_system"] == "NOMIS"

        record_2 = await self.find_by_match_id(db_connection, person_2_data.match_id)
        assert record_2["source_system_id"] == source_system_id
        assert record_2["source_system"] == "DELIUS"

    async def test_stores_override_markers(
        self,
        call_endpoint,
        db_connection: AsyncSession,
    ):
        """
        Test persons override marker data is stored
        """
        override_marker = str(uuid.uuid4())
        override_scope1 = str(uuid.uuid4())
        override_scope2 = str(uuid.uuid4())
        person_data = MockPerson(
            matchId=random_test_data.random_match_id(),
            overrideMarker=override_marker,
            overrideScopes=[
                override_scope1,
                override_scope2,
            ],
        )
        response = call_endpoint(
            "post",
            ROUTE,
            json=person_data.model_dump(by_alias=True),
            client=Client.HMPPS_PERSON_MATCH,
        )
        assert response.status_code == 200
        row = await self.find_by_match_id(db_connection, person_data.match_id)
        assert row["override_marker"] == override_marker
        assert set(row["override_scopes"]) == set([override_scope1, override_scope2])

    @pytest.mark.parametrize(
        "person_fields",
        [
            (
                {
                    "firstName": "",
                    "middleNames": "",
                    "lastName": "",
                },
                ["name_1_std", "name_2_std", "name_3_std"],
            ),
            ({"cros": []}, ["cro_single"]),
            ({"pncs": []}, ["pnc_single"]),
            (
                {"postcodes": []},
                [
                    "postcode_arr",
                    "postcode_first",
                    "postcode_second",
                    "postcode_last",
                    "postcode_outcode_first",
                    "postcode_outcode_last",
                ],
            ),
            ({"sentenceDates": []}, ["sentence_date_first", "sentence_date_last"]),
            ({"overrideMarker": ""}, ["override_marker"]),
            ({"overrideScopes": []}, ["override_scopes"]),
        ],
    )
    async def test_data_stored_as_none_if_blank(
        self,
        call_endpoint,
        db_connection,
        person_fields: tuple,
    ):
        """
        Test that we can score candidates even if fields are 'empty'
        """
        fields, db_row_names = person_fields
        # primary record
        person_data = MockPerson(**fields)

        response = call_endpoint(
            "post",
            ROUTE,
            json=person_data.model_dump(by_alias=True),
            client=Client.HMPPS_PERSON_MATCH,
        )
        assert response.status_code == 200
        row = await self.find_by_match_id(db_connection, person_data.match_id)
        for db_row_name in db_row_names:
            assert row[db_row_name] is None
