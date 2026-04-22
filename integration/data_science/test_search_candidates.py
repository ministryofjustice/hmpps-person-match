from typing import cast

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from hmpps_cpr_splink.cpr_splink.interface.search import search_candidates
from integration.mock_person import MockPerson
from integration.person_factory import PersonFactory
from integration.test_base import IntegrationTestBase


class TestSearchCandidates(IntegrationTestBase):
    @pytest.fixture(autouse=True, scope="function")
    async def clean_db(self, db_connection: AsyncSession) -> None:
        await self.truncate_person_data(db_connection)

    async def test_search_candidates_returns_scored_candidates_without_persisting_search_record(
        self,
        person_factory: PersonFactory,
        db_connection: AsyncSession,
    ) -> None:
        """
        If three matching records (identical to the search record) exist in the database,
        then search_candidates should return all three as scored candidates,
        and should not persist the transient search record.
        """
        template = MockPerson()
        stored_records = [await person_factory.create_from(template) for _ in range(3)]
        search_person = template.model_copy(
            update={
                "match_id": "client-supplied-search-id",
                "source_system_id": "SEARCH-SOURCE-ID-0001",
            },
            deep=True,
        )

        scores = await search_candidates(
            person=search_person,
            pg_engine=self._get_pg_engine(db_connection),
        )

        assert len(scores) == len(stored_records)
        assert {score.candidate_match_id for score in scores} == {record.match_id for record in stored_records}
        await self.assert_size_of_table(db_connection, "person", size=len(stored_records))
        assert await self.find_by_match_id(db_connection, search_person.match_id) is None

    async def test_search_candidates_ignores_client_supplied_match_id(
        self,
        person_factory: PersonFactory,
        db_connection: AsyncSession,
    ) -> None:
        """
        If the client supplies a match_id that already exists in the database,
        then search_candidates should still return the stored record as a candidate.
        """
        stored_record = await person_factory.create_from(MockPerson())
        search_person = stored_record.model_copy(deep=True)

        scores = await search_candidates(
            person=search_person,
            pg_engine=self._get_pg_engine(db_connection),
        )

        assert len(scores) == 1
        assert scores[0].candidate_match_id == stored_record.match_id
        await self.assert_size_of_table(db_connection, "person", size=1)

    @pytest.mark.parametrize(
        "person_data",
        [
            {"firstName": ""},
            {"middleNames": ""},
            {"postcodes": []},
            {"pncs": []},
        ],
    )
    async def test_search_candidates_supports_blank_optional_fields(
        self,
        person_factory: PersonFactory,
        db_connection: AsyncSession,
        person_data: dict,
    ) -> None:
        template = MockPerson(**person_data)
        stored_record = await person_factory.create_from(template)
        search_person = template.model_copy(
            update={
                "match_id": "client-supplied-search-id",
                "source_system_id": "SEARCH-SOURCE-ID-0003",
            },
            deep=True,
        )

        scores = await search_candidates(
            person=search_person,
            pg_engine=self._get_pg_engine(db_connection),
        )

        assert len(scores) == 1
        assert scores[0].candidate_match_id == stored_record.match_id

    async def test_search_candidates_returns_empty_when_database_has_no_matches(
        self,
        db_connection: AsyncSession,
    ) -> None:
        search_person = MockPerson(
            matchId="client-supplied-search-id",
            sourceSystemId="SEARCH-SOURCE-ID-0004",
        )

        scores = await search_candidates(
            person=search_person,
            pg_engine=self._get_pg_engine(db_connection),
        )

        assert scores == []
        await self.assert_size_of_table(db_connection, "person", size=0)

    @staticmethod
    def _get_pg_engine(db_connection: AsyncSession) -> AsyncEngine:
        assert db_connection.bind is not None
        return cast(AsyncEngine, db_connection.bind)
