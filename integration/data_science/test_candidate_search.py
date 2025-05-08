import uuid

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_cpr_splink.cpr_splink.interface.block import candidate_search
from integration import random_test_data
from integration.mock_person import MockPerson
from integration.test_base import IntegrationTestBase


class TestCandidateSearch(IntegrationTestBase):
    """
    Test functioning of candidate search
    """

    @pytest.fixture(autouse=True, scope="function")
    async def clean_db(self, db_connection: AsyncSession):
        """
        Before Each
        Delete all records from the database
        """
        await self.truncate_person_data(db_connection)

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
            person_data.source_system_id = random_test_data.random_crn()
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

    async def test_candidate_search_match_on_pnc(
        self,
        create_person_record,
        db_connection,
    ):
        """
        Test candidate search returns person on match on pnc
        """
        searching_person = str(uuid.uuid4())
        expected_found_person = str(uuid.uuid4())

        pnc = random_test_data.random_pnc()

        await create_person_record(MockPerson(matchId=searching_person, pncs=[pnc]))
        await create_person_record(MockPerson(matchId=expected_found_person, pncs=[pnc]))

        candidate_data = await candidate_search(searching_person, db_connection)

        assert self.extract_match_ids(candidate_data) == set([searching_person, expected_found_person])

    async def test_candidate_search_match_on_cro(
        self,
        create_person_record,
        db_connection,
    ):
        """
        Test candidate search returns person on match on cro
        """
        searching_person = str(uuid.uuid4())
        expected_found_person = str(uuid.uuid4())

        cro = random_test_data.random_cro()

        await create_person_record(MockPerson(matchId=searching_person, cros=[cro]))
        await create_person_record(MockPerson(matchId=expected_found_person, cros=[cro]))

        candidate_data = await candidate_search(searching_person, db_connection)

        assert self.extract_match_ids(candidate_data) == set([searching_person, expected_found_person])

    async def test_candidate_search_match_on_dob_postcode_first(
        self,
        create_person_record,
        db_connection,
    ):
        """
        Test candidate search returns person on match on:
        date_of_birth + postcode_first
        """
        searching_person = str(uuid.uuid4())
        expected_found_person = str(uuid.uuid4())

        date_of_birth = random_test_data.random_date()
        postcode = random_test_data.random_postcode()

        await create_person_record(
            MockPerson(matchId=searching_person, dateOfBirth=date_of_birth, postcodes=[postcode]),
        )
        await create_person_record(
            MockPerson(matchId=expected_found_person, dateOfBirth=date_of_birth, postcodes=[postcode]),
        )

        candidate_data = await candidate_search(searching_person, db_connection)

        assert self.extract_match_ids(candidate_data) == set([searching_person, expected_found_person])

    async def test_candidate_search_match_on_dob_postcode_first_name_1_substr(
        self,
        create_person_record,
        db_connection,
    ):
        """
        Test candidate search returns person on match on:
        date_of_birth + postcode_outcode_first + substr(name_1_std, 1, 2)
        """
        searching_person = str(uuid.uuid4())
        expected_found_person = str(uuid.uuid4())

        date_of_birth = random_test_data.random_date()

        await create_person_record(
            MockPerson(matchId=searching_person, dateOfBirth=date_of_birth, postcodes=["AB1 2BC"], firstName="Brian"),
        )
        await create_person_record(
            MockPerson(
                matchId=expected_found_person, dateOfBirth=date_of_birth, postcodes=["AB1 3DE"], firstName="Bruck",
            ),
        )

        candidate_data = await candidate_search(searching_person, db_connection)

        assert self.extract_match_ids(candidate_data) == set([searching_person, expected_found_person])

    async def test_candidate_search_match_on_dob_postcode_last_name_std_substr(
        self,
        create_person_record,
        db_connection,
    ):
        """
        Test candidate search returns person on match on:
        date_of_birth_last + postcode_outcode_first + substr(last_name_std, 1, 2)
        """
        searching_person = str(uuid.uuid4())
        expected_found_person = str(uuid.uuid4())

        date_of_birth = "2000-01-01"
        alias_date_of_birth = "1990-01-01"

        await create_person_record(
            MockPerson(
                matchId=searching_person,
                dateOfBirth=date_of_birth,
                dateOfBirthAliases=[alias_date_of_birth],
                postcodes=["AB1 2BC"],
                lastName="Smith",
            ),
        )
        await create_person_record(
            MockPerson(
                matchId=expected_found_person,
                dateOfBirth=date_of_birth,
                dateOfBirthAliases=[alias_date_of_birth],
                postcodes=["AB1 3DE"],
                lastName="Smythe",
            ),
        )

        candidate_data = await candidate_search(searching_person, db_connection)

        assert self.extract_match_ids(candidate_data) == set([searching_person, expected_found_person])

    async def test_candidate_search_match_on_forename_first_last_name_first_postcode_first(
        self,
        create_person_record,
        db_connection,
    ):
        """
        Test candidate search returns person on match on:
        forename_first + last_name_first + postcode_first
        """
        searching_person = str(uuid.uuid4())
        expected_found_person = str(uuid.uuid4())

        first_name = random_test_data.random_name()
        last_name = random_test_data.random_name()
        postcode = random_test_data.random_postcode()

        await create_person_record(
            MockPerson(matchId=searching_person, firstName=first_name, lastName=last_name, postcodes=[postcode]),
        )
        await create_person_record(
            MockPerson(matchId=expected_found_person, firstName=first_name, lastName=last_name, postcodes=[postcode]),
        )

        candidate_data = await candidate_search(searching_person, db_connection)

        assert self.extract_match_ids(candidate_data) == set([searching_person, expected_found_person])

    async def test_candidate_search_match_on_date_of_birth_postcode_last(
        self,
        create_person_record,
        db_connection,
    ):
        """
        Test candidate search returns person on match on:
        date_of_birth + postcode_last
        """
        searching_person = str(uuid.uuid4())
        expected_found_person = str(uuid.uuid4())

        date_of_birth = random_test_data.random_date()
        postcode = random_test_data.random_postcode()

        await create_person_record(
            MockPerson(
                matchId=searching_person,
                dateOfBirth=date_of_birth,
                postcodes=[random_test_data.random_postcode(), postcode],
            ),
        )
        await create_person_record(
            MockPerson(
                matchId=expected_found_person,
                dateOfBirth=date_of_birth,
                postcodes=[random_test_data.random_postcode(), postcode],
            ),
        )

        candidate_data = await candidate_search(searching_person, db_connection)

        assert self.extract_match_ids(candidate_data) == set([searching_person, expected_found_person])

    async def test_candidate_search_match_on_sentence_dates_first_date_of_birth(
        self,
        create_person_record,
        db_connection,
    ):
        """
        Test candidate search returns person on match on:
        sentence_date_first + date_of_birth
        """
        searching_person = str(uuid.uuid4())
        expected_found_person = str(uuid.uuid4())

        date_of_birth = random_test_data.random_date()
        sentence_date = random_test_data.random_date()

        await create_person_record(
            MockPerson(matchId=searching_person, dateOfBirth=date_of_birth, sentenceDates=[sentence_date]),
        )
        await create_person_record(
            MockPerson(matchId=expected_found_person, dateOfBirth=date_of_birth, sentenceDates=[sentence_date]),
        )

        candidate_data = await candidate_search(searching_person, db_connection)

        assert self.extract_match_ids(candidate_data) == set([searching_person, expected_found_person])

    @staticmethod
    def extract_match_ids(candidate_data: list[dict]) -> set[str]:
        """
        Extract a list of match_ids from
        """
        return set(map(lambda candidate: candidate["match_id"], candidate_data))
