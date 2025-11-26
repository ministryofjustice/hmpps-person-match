import uuid
from collections.abc import Callable, Sequence

import pytest
from sqlalchemy import RowMapping, text
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
        call_endpoint: Callable,
        db_connection: AsyncSession,
    ) -> None:
        """
        Test person cleaned and stored on person endpoint
        """
        person_data = MockPerson()
        response = call_endpoint(
            "post",
            ROUTE,
            data=person_data.as_json(),
            client=Client.HMPPS_PERSON_MATCH,
        )
        assert response.status_code == 200
        row = await self.find_by_match_id(db_connection, person_data.match_id)
        assert row.get("match_id") == person_data.match_id
        assert row.get("name_1_std") == person_data.first_name.upper()
        assert row.get("name_2_std") == person_data.middle_names.upper()
        assert row.get("name_3_std") is None
        assert row.get("last_name_std") == person_data.last_name.upper()
        assert (
            row.get("first_and_last_name_std") == person_data.first_name.upper() + " " + person_data.last_name.upper()
        )
        assert set(row.get("forename_std_arr") or []) == set(
            [person_data.first_name.upper(), person_data.first_name_aliases[0].upper()],
        )
        assert set(row.get("last_name_std_arr") or []) == set(
            [person_data.last_name.upper(), person_data.last_name_aliases[0].upper()],
        )
        assert row.get("sentence_date_arr") == [person_data.sentence_dates[0]]
        assert row.get("date_of_birth") == person_data.date_of_birth
        assert set(row.get("date_of_birth_arr") or []) == set(
            [
                person_data.date_of_birth,
                person_data.date_of_birth_aliases[0],
            ],
        )
        assert row.get("postcode_arr") == [person_data.postcodes[0].replace(" ", "")]
        assert row.get("postcode_outcode_arr") == [person_data.postcodes[0][:3]]
        assert row.get("cro_single") == person_data.cros[0]
        assert row.get("pnc_single") == person_data.pncs[0]
        assert row.get("source_system") == person_data.source_system
        assert row.get("source_system_id") == person_data.source_system_id
        assert row.get("master_defendant_id") == person_data.master_defendant_id
        assert row.get("postcode_first") == person_data.postcodes[0].replace(" ", "")
        assert row.get("postcode_second") is None
        assert row.get("postcode_last") == person_data.postcodes[0].replace(" ", "")
        assert row.get("postcode_outcode_first") == person_data.postcodes[0][:3]
        assert row.get("postcode_outcode_last") == person_data.postcodes[0][:3]
        assert row.get("date_of_birth_last") == person_data.date_of_birth_aliases[0]
        assert (
            row.get("forename_first") == person_data.first_name_aliases[0].upper()
            or row.get("forename_first") == person_data.first_name.upper()
        )
        assert (
            row.get("forename_last") == person_data.first_name_aliases[0].upper()
            or row.get("forename_last") == person_data.first_name.upper()
        )
        assert (
            row.get("last_name_first") == person_data.last_name_aliases[0].upper()
            or row.get("last_name_first") == person_data.last_name.upper()
        )
        assert (
            row.get("last_name_last") == person_data.last_name_aliases[0].upper()
            or row.get("last_name_last") == person_data.last_name.upper()
        )
        assert row.get("sentence_date_first") == person_data.sentence_dates[0]
        assert row.get("sentence_date_last") == person_data.sentence_dates[0]
        assert row.get("override_marker") is None
        assert row.get("override_scopes") is None

    async def test_clean_and_update_message(
        self,
        call_endpoint: Callable,
        db_connection: AsyncSession,
        person_factory: PersonFactory,
    ) -> None:
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
            data=person.as_json(),
            client=Client.HMPPS_PERSON_MATCH,
        )
        assert response.status_code == 200
        row = await self.find_by_match_id(db_connection, person.match_id)
        assert row.get("match_id") == person.match_id
        assert row.get("name_1_std") == updated_first_name.upper()
        assert row.get("date_of_birth") == updated_dob

    def test_invalid_client_returns_forbidden(self, call_endpoint: Callable) -> None:
        """
        Test person endpoint return 403 forbidden when invalid roles
        """
        response = call_endpoint(
            "post",
            ROUTE,
            data=MockPerson(matchId=random_test_data.random_match_id()).model_dump_json(by_alias=True),
            client=Client.HMPPS_TIER,
        )
        assert response.status_code == 403

    async def test_does_not_create_duplicates_on_source_system_id(
        self,
        call_endpoint: Callable,
        db_connection: AsyncSession,
    ) -> None:
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
                data=person_data.as_json(),
                client=Client.HMPPS_PERSON_MATCH,
            )

        db_result = await db_connection.execute(
            text(f"SELECT * FROM personmatch.person WHERE source_system_id = '{source_system_id}'"),
        )
        result: Sequence[RowMapping] = db_result.mappings().fetchall()
        assert len(result) == 1

    async def test_match_id_is_upserted_on_same_source_system_id(
        self,
        call_endpoint: Callable,
        db_connection: AsyncSession,
    ) -> None:
        """
        Test match id is upserted on conflict with source system id
        """
        source_system_id = random_test_data.random_source_system_id()
        source_system = random_test_data.random_source_system()

        person = MockPerson(sourceSystemId=source_system_id, sourceSystem=source_system)

        call_endpoint(
            "post",
            ROUTE,
            data=person.as_json(),
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
            data=updated_person_data.as_json(),
            client=Client.HMPPS_PERSON_MATCH,
        )

        result = await self.find_by_match_id(db_connection, updated_person_data.match_id)
        assert result.get("last_name_std") == updated_last_name.upper()

        assert await self.find_by_match_id(db_connection, person.match_id) is None

    async def test_source_system_id_can_be_same_if_different_source_system(
        self,
        call_endpoint: Callable,
        db_connection: AsyncSession,
    ) -> None:
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
            data=person_1_data.as_json(),
            client=Client.HMPPS_PERSON_MATCH,
        )

        person_2_data = MockPerson(
            sourceSystemId=source_system_id,
            sourceSystem="DELIUS",
        )

        call_endpoint(
            "post",
            ROUTE,
            data=person_2_data.as_json(),
            client=Client.HMPPS_PERSON_MATCH,
        )

        record_1 = await self.find_by_match_id(db_connection, person_1_data.match_id)
        assert record_1.get("source_system_id") == source_system_id
        assert record_1.get("source_system") == "NOMIS"

        record_2 = await self.find_by_match_id(db_connection, person_2_data.match_id)
        assert record_2.get("source_system_id") == source_system_id
        assert record_2.get("source_system") == "DELIUS"

    async def test_stores_override_markers(
        self,
        call_endpoint: Callable,
        db_connection: AsyncSession,
    ) -> None:
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
            data=person_data.as_json(),
            client=Client.HMPPS_PERSON_MATCH,
        )
        assert response.status_code == 200
        row = await self.find_by_match_id(db_connection, person_data.match_id)
        assert row.get("override_marker") == override_marker
        assert set(row.get("override_scopes") or []) == set([override_scope1, override_scope2])

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
        call_endpoint: Callable,
        db_connection: AsyncSession,
        person_fields: tuple,
    ) -> None:
        """
        Test that we can score candidates even if fields are 'empty'
        """
        fields, db_row_names = person_fields
        # primary record
        person_data = MockPerson(**fields)

        response = call_endpoint(
            "post",
            ROUTE,
            data=person_data.as_json(),
            client=Client.HMPPS_PERSON_MATCH,
        )
        assert response.status_code == 200
        row = await self.find_by_match_id(db_connection, person_data.match_id)
        for db_row_name in db_row_names:
            assert row.get(db_row_name) is None
