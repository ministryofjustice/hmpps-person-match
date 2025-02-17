import pytest

from hmpps_cpr_splink.cpr_splink.interface.score import get_scored_candidates
from hmpps_person_match.models.person.person import Person


class TestPersonScore:
    """
    Test functioning of candidate search
    """

    @staticmethod
    @pytest.fixture(autouse=True, scope="function")
    async def clean_db(db):
        """
        Before Each
        Delete all records from the database
        """
        await db.execute("TRUNCATE TABLE personmatch.person")

    async def test_get_scored_candidates(
        self, person_id, create_person_record, create_person_data, sqlalchemy_db_connection,
    ):
        """
        Test retrieving scored candidates gives correct number
        """
        # primary record
        await create_person_record(Person(**create_person_data(person_id)))
        # candidates - all should match with high match weight
        n_candidates = 10
        for _ in range(n_candidates):
            await create_person_record(Person(**create_person_data()))

        res = await get_scored_candidates(person_id, sqlalchemy_db_connection)

        # we have all candidates + original record
        assert len(res) == n_candidates
        assert len([match_weight for r in res if (match_weight := r["candidate_match_weight"]) > 20])

    async def test_get_scored_candidates_none_in_db(
        self,
        create_person_record,
        create_person_data,
        sqlalchemy_db_connection,
    ):
        """
        Test scoring returns nothing if the given match_id is not in db
        """
        n_candidates = 10
        for _ in range(n_candidates):
            await create_person_record(Person(**create_person_data()))

        res = await get_scored_candidates("bogus_match_id", sqlalchemy_db_connection)

        assert len(res) == 0
