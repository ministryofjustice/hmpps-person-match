from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_cpr_splink.cpr_splink.interface import clean
from hmpps_person_match.models.person.person_batch import PersonBatch
from integration import random_test_data
from integration.mock_person import MockPerson


class PersonFactory:
    def __init__(self, db_conn: AsyncSession) -> None:
        self.db_connection = db_conn

    async def create_from(self, person: MockPerson) -> MockPerson:
        new_person = person.model_copy(
            update={
                "match_id": random_test_data.random_match_id(),
                "source_system_id": random_test_data.random_source_system_id(),
            },
            deep=True,
        )
        await self._upsert_record(new_person)
        return new_person

    async def update(self, person: MockPerson) -> None:
        await self._upsert_record(person)

    async def _upsert_record(self, person: MockPerson) -> None:
        await clean.clean_and_insert(PersonBatch(records=[person]), self.db_connection)
