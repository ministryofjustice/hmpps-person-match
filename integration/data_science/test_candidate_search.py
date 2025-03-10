import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_cpr_splink.cpr_splink.interface.block import candidate_search
from integration.mock_person import MockPerson


class TestCandidateSearch:
    """
    Test functioning of candidate search
    """

    @staticmethod
    @pytest.fixture(autouse=True, scope="function")
    async def clean_db(db_connection: AsyncSession):
        """
        Before Each
        Delete all records from the database
        """
        await db_connection.execute(text("TRUNCATE TABLE personmatch.person"))
        await db_connection.commit()

    async def test_candidate_search(self, match_id, create_person_record, db_connection):
        """
        Test candidate search returns correct number of people
        """
        # primary record
        person_data = MockPerson(matchId=match_id)
        await create_person_record(person_data)
        # candidates - all should match
        n_candidates = 10
        for _ in range(n_candidates):
            person_data.match_id = str(uuid.uuid4())
            await create_person_record(person_data)

        candidate_data = await candidate_search(match_id, db_connection)

        # we have all candidates + original record
        assert len(candidate_data) == n_candidates + 1

    async def test_candidate_search_no_record_in_db(
        self,
        create_person_record,
        db_connection,
    ):
        """
        Test candidate search returns nothing if the given match_id is not in db
        """
        n_candidates = 10
        for _ in range(n_candidates):
            await create_person_record(MockPerson(matchId=str(uuid.uuid4())))

        candidate_data = await candidate_search("unknown_match_id", db_connection)

        # don't have an original record, so can't have any candidates
        assert len(candidate_data) == 0
