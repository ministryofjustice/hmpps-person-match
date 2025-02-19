import pytest

from hmpps_cpr_splink.cpr_splink.interface.block import candidate_search

from .data.blocked_pairs import blocked_pairs


class TestBlockingRules:
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

    @pytest.mark.parametrize("record_pair", blocked_pairs)
    async def test_candidate_search(self, record_pair, create_cleaned_person, duckdb_con_with_pg):
        """
        Test candidate search correctly returns those that should be blocked together
        """
        for person in record_pair.records:
            await create_cleaned_person(person)

        primary_match_id = record_pair.record_1.match_id
        table_name = await candidate_search(primary_match_id, duckdb_con_with_pg)

        matches = duckdb_con_with_pg.execute(f"SELECT match_id, match_key FROM {table_name}").fetchall()
        columns = [desc[0] for desc in duckdb_con_with_pg.description]

        match_data = [dict(zip(columns, row, strict=True)) for row in matches]
        match_ids = [r["match_id"] for r in match_data]
        match_keys = [r["match_key"] for r in match_data]

        candidate_match_id = record_pair.record_2.match_id
        assert candidate_match_id in match_ids, f"failure on rule: {record_pair.reason_for_passing_blocking}"
        assert len(match_data) == 2, f"extra matches on rule: {record_pair.reason_for_passing_blocking}"
        assert record_pair.expected_match_key == match_keys[match_ids.index(candidate_match_id)]
