from collections.abc import Callable

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_person_match.routes.person.search.person_search import ROUTE
from integration.client import Client
from integration.mock_person import MockPerson
from integration.person_factory import PersonFactory
from integration.test_base import IntegrationTestBase


def search_request_from(person: MockPerson) -> dict[str, object]:
    assert person.date_of_birth is not None
    return {
        "fullName": " ".join(
            name_part for name_part in (person.first_name, person.middle_names, person.last_name) if name_part
        ),
        "dateOfBirth": person.date_of_birth.isoformat(),
        "firstNameAliases": person.first_name_aliases,
        "lastNameAliases": person.last_name_aliases,
        "dateOfBirthAliases": [alias.isoformat() for alias in person.date_of_birth_aliases],
        "postcodes": person.postcodes,
    }


class TestPersonSearchEndpoint(IntegrationTestBase):
    @pytest.fixture(autouse=True, scope="function")
    async def before_each(self, db_connection: AsyncSession) -> None:
        await self.truncate_person_data(db_connection)
        await self.refresh_term_frequencies(db_connection)

    async def test_no_match_returns_empty_without_persisting_search_record(
        self,
        call_endpoint: Callable,
        db_connection: AsyncSession,
    ) -> None:
        response = call_endpoint(
            "post",
            ROUTE,
            json={},
            client=Client.HMPPS_PERSON_MATCH,
        )

        assert response.status_code == 200
        assert response.json() == []
        await self.assert_size_of_table(db_connection, "person", size=0)

    async def test_copied_record_returns_all_candidates_without_mutation(
        self,
        call_endpoint: Callable,
        person_factory: PersonFactory,
        db_connection: AsyncSession,
    ) -> None:
        candidate_1 = await person_factory.create_from(MockPerson())
        candidate_2 = await person_factory.create_from(candidate_1)
        candidate_1_before = dict(await self.find_by_match_id(db_connection, candidate_1.match_id) or {})
        candidate_2_before = dict(await self.find_by_match_id(db_connection, candidate_2.match_id) or {})

        response = call_endpoint(
            "post",
            ROUTE,
            json=search_request_from(candidate_1),
            client=Client.HMPPS_PERSON_MATCH,
        )

        assert response.status_code == 200
        results = response.json()
        assert len(results) == 2
        assert {result["candidate_match_id"] for result in results} == {
            candidate_1.match_id,
            candidate_2.match_id,
        }

        assert dict(await self.find_by_match_id(db_connection, candidate_1.match_id) or {}) == candidate_1_before
        assert dict(await self.find_by_match_id(db_connection, candidate_2.match_id) or {}) == candidate_2_before
        await self.assert_size_of_table(db_connection, "person", size=2)
