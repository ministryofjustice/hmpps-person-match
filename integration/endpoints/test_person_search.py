import asyncio
from collections.abc import Callable

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_person_match.routes.person.search.person_search import ROUTE
from integration import random_test_data
from integration.client import Client
from integration.mock_person import MockPerson
from integration.person_factory import PersonFactory
from integration.test_base import IntegrationTestBase


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
        search_person = MockPerson(
            firstName="",
            middleNames="",
            lastName="",
            firstNameAliases=[],
            lastNameAliases=[],
            dateOfBirthAliases=[],
            postcodes=[],
            cros=[],
            pncs=[],
            sentenceDates=[],
        )

        response = call_endpoint(
            "post",
            ROUTE,
            data=search_person.as_json(),
            client=Client.HMPPS_PERSON_MATCH,
        )

        assert response.status_code == 200
        assert response.json() == []
        assert await self.find_by_match_id(db_connection, search_person.match_id) is None
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
            data=candidate_1.as_json(),
            client=Client.HMPPS_PERSON_MATCH,
        )

        assert response.status_code == 200
        results = response.json()
        assert len(results) == 2
        assert {result["candidate_match_id"] for result in results} == {
            candidate_1.match_id,
            candidate_2.match_id,
        }

        expected_fields = {
            "candidate_match_id",
            "candidate_match_probability",
            "candidate_match_weight",
            "candidate_should_join",
            "candidate_should_fracture",
            "candidate_is_possible_twin",
            "unadjusted_match_weight",
        }
        assert all(set(result) == expected_fields for result in results)
        assert all(result["candidate_should_join"] for result in results)
        assert all(not result["candidate_should_fracture"] for result in results)
        assert all(not result["candidate_is_possible_twin"] for result in results)

        assert dict(await self.find_by_match_id(db_connection, candidate_1.match_id) or {}) == candidate_1_before
        assert dict(await self.find_by_match_id(db_connection, candidate_2.match_id) or {}) == candidate_2_before
        await self.assert_size_of_table(db_connection, "person", size=2)

    async def test_search_returns_fracture_flag(
        self,
        call_endpoint: Callable,
        person_factory: PersonFactory,
    ) -> None:
        pnc = random_test_data.random_pnc()
        search_person = MockPerson(pncs=[pnc])
        candidate = await person_factory.create_from(MockPerson(pncs=[pnc]))

        response = call_endpoint(
            "post",
            ROUTE,
            data=search_person.as_json(),
            client=Client.HMPPS_PERSON_MATCH,
        )

        assert response.status_code == 200
        assert len(response.json()) == 1
        assert response.json()[0]["candidate_match_id"] == candidate.match_id
        assert response.json()[0]["candidate_should_fracture"]

    async def test_search_flags_possible_twins(
        self,
        call_endpoint: Callable,
        person_factory: PersonFactory,
    ) -> None:
        search_person = MockPerson(masterDefendantId=None)
        matching_candidate = await person_factory.create_from(search_person)

        twin_source = search_person.model_copy(deep=True)
        twin_source.first_name = random_test_data.random_name()
        twin_source.first_name_aliases = []
        twin_source.pncs = []
        twin_source.cros = []
        twin_candidate = await person_factory.create_from(twin_source)

        response = call_endpoint(
            "post",
            ROUTE,
            data=search_person.as_json(),
            client=Client.HMPPS_PERSON_MATCH,
        )

        assert response.status_code == 200
        results_by_id = {result["candidate_match_id"]: result for result in response.json()}
        assert not results_by_id[matching_candidate.match_id]["candidate_is_possible_twin"]
        assert results_by_id[matching_candidate.match_id]["candidate_match_weight"] > 24
        assert results_by_id[twin_candidate.match_id]["candidate_is_possible_twin"]
        assert results_by_id[twin_candidate.match_id]["candidate_match_weight"] < 0

    async def test_concurrent_searches_are_isolated(
        self,
        call_endpoint: Callable,
        person_factory: PersonFactory,
        db_connection: AsyncSession,
    ) -> None:
        search_person = MockPerson()
        candidate = await person_factory.create_from(search_person)

        async def search() -> tuple[int, list[dict]]:
            response = await asyncio.to_thread(
                call_endpoint,
                "post",
                ROUTE,
                Client.HMPPS_PERSON_MATCH,
                None,
                search_person.as_json(),
            )
            return response.status_code, response.json()

        responses = await asyncio.gather(*(search() for _ in range(3)))

        assert all(status_code == 200 for status_code, _ in responses)
        assert all(
            [result["candidate_match_id"] for result in results] == [candidate.match_id] for _, results in responses
        )
        await self.assert_size_of_table(db_connection, "person", size=1)
