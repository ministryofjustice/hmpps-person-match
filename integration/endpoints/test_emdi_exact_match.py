from collections.abc import Callable
from datetime import date

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_person_match.routes.person.search.emdi_exact_match import ROUTE
from integration.client import Client
from integration.mock_person import MockPerson
from integration.person_factory import PersonFactory
from integration.test_base import IntegrationTestBase


@pytest.fixture()
def emdi_exact_match_request_json() -> dict:
    return {
        "firstName": "Henry",
        "lastName": "Junaed",
        "dateOfBirth": "1992-03-02",
        "pnc": "2000/1234567A",
    }


@pytest.fixture()
def emdi_exact_match_person() -> MockPerson:
    return MockPerson(
        firstName="Henry",
        middleNames="Ahmed",
        lastName="Junaed",
        dateOfBirth=date(1992, 3, 2),
        firstNameAliases=[],
        lastNameAliases=[],
        dateOfBirthAliases=[],
        postcodes=[],
        cros=[],
        pncs=["2000/1234567A"],
        sentenceDates=[],
        sourceSystem="NOMIS",
        masterDefendantId=None,
        overrideMarker=None,
        overrideScopes=None,
    )


class TestEmdiExactMatchEndpoint(IntegrationTestBase):
    @pytest.fixture(autouse=True, scope="function")
    async def before_each(self, db_connection: AsyncSession) -> None:
        await self.truncate_person_data(db_connection)

    async def test_emdi_exact_match_does_not_insert_search_record(
        self,
        call_endpoint: Callable,
        person_factory: PersonFactory,
        db_connection: AsyncSession,
        emdi_exact_match_person: MockPerson,
        emdi_exact_match_request_json: dict,
    ) -> None:
        candidate = await person_factory.create_from(emdi_exact_match_person)

        response = call_endpoint("post", ROUTE, json=emdi_exact_match_request_json, client=Client.HMPPS_PERSON_MATCH)

        assert response.status_code == 200
        assert response.json() == {"matchIds": [candidate.match_id]}
        await self.assert_size_of_table(db_connection, "person", size=1)

    async def test_cleaning_equivalence(
        self,
        call_endpoint: Callable,
        person_factory: PersonFactory,
        emdi_exact_match_person: MockPerson,
        emdi_exact_match_request_json: dict,
    ) -> None:
        candidate = await person_factory.create_from(emdi_exact_match_person)
        emdi_exact_match_request_json.update(firstName="  henry Ahmed ", lastName=" junaed ", pnc="2000/1234567a")

        response = call_endpoint("post", ROUTE, json=emdi_exact_match_request_json, client=Client.HMPPS_PERSON_MATCH)

        assert response.status_code == 200
        assert response.json() == {"matchIds": [candidate.match_id]}

    async def test_no_match(
        self,
        call_endpoint: Callable,
        emdi_exact_match_request_json: dict,
    ) -> None:
        response = call_endpoint("post", ROUTE, json=emdi_exact_match_request_json, client=Client.HMPPS_PERSON_MATCH)

        assert response.status_code == 200
        assert response.json() == {"matchIds": []}

    async def test_multiple_matches_are_sorted(
        self,
        call_endpoint: Callable,
        person_factory: PersonFactory,
        emdi_exact_match_person: MockPerson,
        emdi_exact_match_request_json: dict,
    ) -> None:
        candidate_1 = await person_factory.create_from(emdi_exact_match_person)
        candidate_2 = await person_factory.create_from(emdi_exact_match_person)

        response = call_endpoint("post", ROUTE, json=emdi_exact_match_request_json, client=Client.HMPPS_PERSON_MATCH)

        assert response.status_code == 200
        assert response.json() == {"matchIds": sorted([candidate_1.match_id, candidate_2.match_id])}

    @pytest.mark.parametrize(
        "mismatch",
        [
            ("firstName", "Harry"),
            ("lastName", "Smith"),
            ("dateOfBirth", "1992-03-03"),
            ("pnc", "2001/1234567A"),
        ],
    )
    async def test_each_field_must_match(
        self,
        call_endpoint: Callable,
        person_factory: PersonFactory,
        emdi_exact_match_person: MockPerson,
        emdi_exact_match_request_json: dict,
        mismatch: tuple[str, str],
    ) -> None:
        await person_factory.create_from(emdi_exact_match_person)
        field, value = mismatch
        emdi_exact_match_request_json[field] = value

        response = call_endpoint("post", ROUTE, json=emdi_exact_match_request_json, client=Client.HMPPS_PERSON_MATCH)

        assert response.status_code == 200
        assert response.json() == {"matchIds": []}

    async def test_only_first_sorted_pnc_matches(
        self,
        call_endpoint: Callable,
        person_factory: PersonFactory,
        emdi_exact_match_person: MockPerson,
        emdi_exact_match_request_json: dict,
    ) -> None:
        emdi_exact_match_person.pncs = ["2001/1234567a", "2000/1234567a", "2000/1234567A"]
        candidate = await person_factory.create_from(emdi_exact_match_person)

        response = call_endpoint("post", ROUTE, json=emdi_exact_match_request_json, client=Client.HMPPS_PERSON_MATCH)

        assert response.status_code == 200
        assert response.json() == {"matchIds": [candidate.match_id]}

        emdi_exact_match_request_json["pnc"] = "2001/1234567A"
        response = call_endpoint("post", ROUTE, json=emdi_exact_match_request_json, client=Client.HMPPS_PERSON_MATCH)

        assert response.status_code == 200
        assert response.json() == {"matchIds": []}

    async def test_invalid_cleaned_input(
        self,
        call_endpoint: Callable,
        emdi_exact_match_request_json: dict,
    ) -> None:
        emdi_exact_match_request_json.update(firstName="---", lastName="---")

        response = call_endpoint("post", ROUTE, json=emdi_exact_match_request_json, client=Client.HMPPS_PERSON_MATCH)

        assert response.status_code == 400
        assert response.json()["detail"] == "Invalid request."
