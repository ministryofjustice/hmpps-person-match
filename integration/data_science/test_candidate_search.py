import pytest

from hmpps_cpr_splink.cpr_splink.interface.block import candidate_search
from hmpps_person_match.models.person.person import Person


class TestCandidateSearch:
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

    async def test_candidate_search(self, match_id, create_person_record, create_person_data, duckdb_con_with_pg):
        """
        Test candidate search returns correct number of people
        """
        # primary record
        await create_person_record(Person(**create_person_data(match_id)))
        # candidates - all should match
        n_candidates = 10
        for _ in range(n_candidates):
            await create_person_record(Person(**create_person_data()))

        table_name = await candidate_search(match_id, duckdb_con_with_pg)

        row = duckdb_con_with_pg.execute(f"SELECT * FROM {table_name}").fetchall()
        # we have all candidates + original record
        assert len(row) == n_candidates + 1

    async def test_candidate_search_no_record_in_db(
        self,
        create_person_record,
        create_person_data,
        duckdb_con_with_pg,
    ):
        """
        Test candidate search returns nothing if the given match_id is not in db
        """
        n_candidates = 10
        for _ in range(n_candidates):
            await create_person_record(Person(**create_person_data()))

        table_name = await candidate_search("unknown_match_id", duckdb_con_with_pg)

        row = duckdb_con_with_pg.execute(f"SELECT * FROM {table_name}").fetchall()
        # don't have an original record, so can't have any candidates
        assert len(row) == 0
