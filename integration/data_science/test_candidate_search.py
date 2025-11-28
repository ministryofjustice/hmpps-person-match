import uuid
from collections.abc import Sequence
from datetime import date

import pytest
from sqlalchemy import RowMapping
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_cpr_splink.cpr_splink.interface.block import candidate_search
from integration import random_test_data
from integration.mock_person import MockPerson
from integration.person_factory import PersonFactory
from integration.test_base import IntegrationTestBase


class TestCandidateSearch(IntegrationTestBase):
    """
    Test functioning of candidate search
    """

    @pytest.fixture(autouse=True, scope="function")
    async def clean_db(self, db_connection: AsyncSession) -> None:
        """
        Before Each
        Delete all records from the database
        """
        await self.truncate_person_data(db_connection)

    async def test_candidate_search(self, person_factory: PersonFactory, db_connection: AsyncSession) -> None:
        """
        Test candidate search returns correct number of people
        """
        # primary record
        primary_record = await person_factory.create_from(MockPerson())
        # candidates - all should match
        n_candidates = 10
        for _ in range(n_candidates):
            await person_factory.create_from(primary_record)

        candidate_data = await candidate_search(primary_record.match_id, db_connection)

        # we have all candidates + original record
        assert len(candidate_data) == n_candidates + 1

    async def test_candidate_search_no_record_in_db(
        self,
        person_factory: PersonFactory,
        db_connection: AsyncSession,
    ) -> None:
        """
        Test candidate search returns nothing if the given match_id is not in db
        """
        n_candidates = 10
        for _ in range(n_candidates):
            await person_factory.create_from(MockPerson())

        candidate_data = await candidate_search("unknown_match_id", db_connection)

        # don't have an original record, so can't have any candidates
        assert len(candidate_data) == 0

    async def test_candidate_search_match_on_pnc(
        self,
        person_factory: PersonFactory,
        db_connection: AsyncSession,
    ) -> None:
        """
        Test candidate search returns person on match on pnc
        """
        pnc = random_test_data.random_pnc()

        searching_person = await person_factory.create_from(MockPerson(pncs=[pnc]))
        expected_found_person = await person_factory.create_from(MockPerson(pncs=[pnc]))

        candidate_data = await candidate_search(searching_person.match_id, db_connection)

        assert self.extract_match_ids(candidate_data) == set(
            [searching_person.match_id, expected_found_person.match_id],
        )

    async def test_candidate_search_match_on_cro(
        self,
        person_factory: PersonFactory,
        db_connection: AsyncSession,
    ) -> None:
        """
        Test candidate search returns person on match on cro
        """
        cro = random_test_data.random_cro()

        searching_person = await person_factory.create_from(MockPerson(cros=[cro]))
        expected_found_person = await person_factory.create_from(MockPerson(cros=[cro]))

        candidate_data = await candidate_search(searching_person.match_id, db_connection)

        assert self.extract_match_ids(candidate_data) == set(
            [searching_person.match_id, expected_found_person.match_id],
        )

    async def test_candidate_search_match_on_dob_postcode_first(
        self,
        person_factory: PersonFactory,
        db_connection: AsyncSession,
    ) -> None:
        """
        Test candidate search returns person on match on:
        date_of_birth + postcode_first
        """
        date_of_birth = random_test_data.random_date()
        postcode = random_test_data.random_postcode()

        searching_person = await person_factory.create_from(
            MockPerson(dateOfBirth=date_of_birth, postcodes=[postcode]),
        )
        expected_found_person = await person_factory.create_from(
            MockPerson(dateOfBirth=date_of_birth, postcodes=[postcode]),
        )

        candidate_data = await candidate_search(searching_person.match_id, db_connection)

        assert self.extract_match_ids(candidate_data) == set(
            [searching_person.match_id, expected_found_person.match_id],
        )

    async def test_candidate_search_match_on_dob_postcode_first_name_1_substr(
        self,
        person_factory: PersonFactory,
        db_connection: AsyncSession,
    ) -> None:
        """
        Test candidate search returns person on match on:
        date_of_birth + postcode_outcode_first + substr(name_1_std, 1, 2)
        """
        date_of_birth = random_test_data.random_date()

        searching_person = await person_factory.create_from(
            MockPerson(dateOfBirth=date_of_birth, postcodes=["AB1 2BC"], firstName="Brian"),
        )
        expected_found_person = await person_factory.create_from(
            MockPerson(
                dateOfBirth=date_of_birth,
                postcodes=["AB1 3DE"],
                firstName="Bruck",
            ),
        )

        candidate_data = await candidate_search(searching_person.match_id, db_connection)

        assert self.extract_match_ids(candidate_data) == set(
            [searching_person.match_id, expected_found_person.match_id],
        )

    async def test_candidate_search_match_on_dob_postcode_last_name_std_substr(
        self,
        person_factory: PersonFactory,
        db_connection: AsyncSession,
    ) -> None:
        """
        Test candidate search returns person on match on:
        date_of_birth_last + postcode_outcode_first + substr(last_name_std, 1, 2)
        """

        date_of_birth = self.to_date_object("2000-01-01")
        alias_date_of_birth = self.to_date_object("1990-01-01")

        searching_person = await person_factory.create_from(
            MockPerson(
                dateOfBirth=date_of_birth,
                dateOfBirthAliases=[alias_date_of_birth],
                postcodes=["AB1 2BC"],
                lastName="Smith",
            ),
        )
        expected_found_person = await person_factory.create_from(
            MockPerson(
                dateOfBirth=date_of_birth,
                dateOfBirthAliases=[alias_date_of_birth],
                postcodes=["AB1 3DE"],
                lastName="Smythe",
            ),
        )

        candidate_data = await candidate_search(searching_person.match_id, db_connection)

        assert self.extract_match_ids(candidate_data) == set(
            [searching_person.match_id, expected_found_person.match_id],
        )

    async def test_candidate_search_match_on_forename_frst_last_name_frst_postcode_frst(
        self,
        person_factory: PersonFactory,
        db_connection: AsyncSession,
    ) -> None:
        """
        Test candidate search returns person on match on:
        forename_first + last_name_first + postcode_first
        """
        first_name = random_test_data.random_name()
        last_name = random_test_data.random_name()
        postcode = random_test_data.random_postcode()

        searching_person = await person_factory.create_from(
            MockPerson(firstName=first_name, lastName=last_name, postcodes=[postcode]),
        )
        expected_found_person = await person_factory.create_from(
            MockPerson(firstName=first_name, lastName=last_name, postcodes=[postcode]),
        )

        candidate_data = await candidate_search(searching_person.match_id, db_connection)

        assert self.extract_match_ids(candidate_data) == set(
            [searching_person.match_id, expected_found_person.match_id],
        )

    async def test_candidate_search_match_on_sentence_dates_first_date_of_birth(
        self,
        person_factory: PersonFactory,
        db_connection: AsyncSession,
    ) -> None:
        """
        Test candidate search returns person on match on:
        sentence_date_first + date_of_birth
        """
        date_of_birth = random_test_data.random_date()
        sentence_date = random_test_data.random_date()

        searching_person = await person_factory.create_from(
            MockPerson(dateOfBirth=date_of_birth, sentenceDates=[sentence_date]),
        )
        expected_found_person = await person_factory.create_from(
            MockPerson(dateOfBirth=date_of_birth, sentenceDates=[sentence_date]),
        )

        candidate_data = await candidate_search(searching_person.match_id, db_connection)

        assert self.extract_match_ids(candidate_data) == set(
            [searching_person.match_id, expected_found_person.match_id],
        )

    async def test_candidate_search_match_on_master_defendant_id(
        self,
        person_factory: PersonFactory,
        db_connection: AsyncSession,
    ) -> None:
        """
        Test candidate search returns person on match on:
        master defendant id
        """
        master_defendant_id = str(uuid.uuid4())

        searching_person = await person_factory.create_from(
            MockPerson(masterDefendantId=master_defendant_id),
        )
        expected_found_person = await person_factory.create_from(
            MockPerson(masterDefendantId=master_defendant_id),
        )

        candidate_data = await candidate_search(searching_person.match_id, db_connection)

        assert self.extract_match_ids(candidate_data) == set(
            [searching_person.match_id, expected_found_person.match_id],
        )

    async def test_candidate_search_match_sentence_date_where_order_does_not_matter(
        self,
        person_factory: PersonFactory,
        db_connection: AsyncSession,
    ) -> None:
        """
        Test candidate search returns person on match on:
        date of birth + sentence date
        """
        date_of_birth = random_test_data.random_date()
        sentence_date = random_test_data.random_date()

        searching_person = await person_factory.create_from(
            MockPerson(
                dateOfBirth=date_of_birth, sentenceDates=self.generate_sentence_dates_with(sentence_date, size=10),
            ),
        )
        expected_found_person = await person_factory.create_from(
            MockPerson(
                dateOfBirth=date_of_birth, sentenceDates=self.generate_sentence_dates_with(sentence_date, size=10),
            ),
        )
        candidate_data = await candidate_search(searching_person.match_id, db_connection)

        assert self.extract_match_ids(candidate_data) == set(
            [searching_person.match_id, expected_found_person.match_id],
        )

    async def test_candidate_search_match_postcode_where_order_does_not_matter(
        self,
        person_factory: PersonFactory,
        db_connection: AsyncSession,
    ) -> None:
        """
        Test candidate search returns person on match on:
        date of birth + postcode
        """
        date_of_birth = random_test_data.random_date()
        postcode = random_test_data.random_postcode()

        searching_person = await person_factory.create_from(
            MockPerson(dateOfBirth=date_of_birth, postcodes=self.generate_postcodes_with(postcode, size=10)),
        )
        expected_found_person = await person_factory.create_from(
            MockPerson(dateOfBirth=date_of_birth, postcodes=self.generate_postcodes_with(postcode, size=10)),
        )

        candidate_data = await candidate_search(searching_person.match_id, db_connection)

        assert self.extract_match_ids(candidate_data) == set(
            [searching_person.match_id, expected_found_person.match_id],
        )

    def generate_postcodes_with(self, postcode: str, size: int) -> list[str]:
        """
        Create list of random postcodes
        """
        postcodes = self.generate_postcodes_of_size(size)
        postcodes.append(postcode)
        return postcodes

    @staticmethod
    def generate_postcodes_of_size(size: int) -> list[str]:
        """
        Create list of random postcodes
        """
        return [random_test_data.random_postcode() for _ in range(size)]

    def generate_sentence_dates_with(self, sentence_date: date, size: int) -> list[date]:
        """
        Create list of random sentence dates
        """
        sentence_dates = self.generate_sentence_dates_of_size(size)
        sentence_dates.append(sentence_date)
        return sentence_dates

    @staticmethod
    def generate_sentence_dates_of_size(size: int) -> list[date]:
        """
        Create list of random sentence dates
        """
        return [random_test_data.random_date() for _ in range(size)]

    @staticmethod
    def extract_match_ids(candidate_data: Sequence[RowMapping]) -> set[str]:
        """
        Extract a list of match_ids from
        """
        return set(map(lambda candidate: candidate["match_id"], candidate_data))
