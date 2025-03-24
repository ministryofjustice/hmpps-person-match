import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_cpr_splink.cpr_splink.interface.score import get_scored_candidates
from integration.mock_person import MockPerson


class TestPersonScore:
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

    async def test_get_scored_candidates(
        self,
        match_id,
        create_person_record,
        pg_db_url,
        db_connection,
    ):
        """
        Test retrieving scored candidates gives correct number
        """
        # primary record
        person_data = MockPerson(matchId=match_id)
        await create_person_record(person_data)
        # candidates - all should match with high match weight
        n_candidates = 10
        for _ in range(n_candidates):
            person_data.match_id = str(uuid.uuid4())
            await create_person_record(person_data)

        res = await get_scored_candidates(match_id, pg_db_url, db_connection)

        # we have all candidates + original record
        assert len(res) == n_candidates
        assert len([match_weight for r in res if (match_weight := r["candidate_match_weight"]) > 20]) == n_candidates

    @pytest.mark.parametrize(
        "person_data",
        [
            {"firstName": ""},
            {"middleNames": ""},
            {"lastName": ""},
            {"crn": ""},
            {"firstNameAliases": []},
            {"lastNameAliases": []},
            {"dateOfBirthAliases": []},
            {"postcodes": []},
            {"cros": []},
            {"pncs": []},
            {"sentenceDates": []},
            {"sourceSystem": ""},
        ],
    )
    async def test_get_scored_candidates_blank_data(
        self,
        match_id,
        create_person_record,
        pg_db_url,
        db_connection,
        person_data,
    ):
        """
        Test that we can scor candidates even if fields are 'empty'
        """
        # primary record
        person_data = MockPerson(matchId=match_id, **person_data)
        await create_person_record(person_data)
        # candidates - all should match with high match weight
        n_candidates = 10
        for _ in range(n_candidates):
            person_data.match_id = str(uuid.uuid4())
            await create_person_record(person_data)

        res = await get_scored_candidates(match_id, pg_db_url, db_connection)

        # we have all candidates + original record
        assert len(res) == n_candidates
        assert len([match_weight for r in res if (match_weight := r["candidate_match_weight"]) > 20]) == n_candidates

    async def test_get_scored_candidates_none_in_db(
        self,
        create_person_record,
        pg_db_url,
        db_connection,
    ):
        """
        Test scoring returns nothing if the given match_id is not in db
        """
        n_candidates = 10
        for _ in range(n_candidates):
            await create_person_record(MockPerson(matchId=str(uuid.uuid4())))

        res = await get_scored_candidates("bogus_match_id", pg_db_url, db_connection)

        assert len(res) == 0
