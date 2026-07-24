from uuid import uuid4

import duckdb
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_cpr_splink.cpr_splink.interface.block import candidate_search_for_record
from hmpps_cpr_splink.cpr_splink.interface.clean import clean_person
from hmpps_cpr_splink.cpr_splink.interface.score import score_candidates
from hmpps_person_match.models.person.person import Person
from hmpps_person_match.models.person.person_score import PersonScore


async def search_candidates(
    person: Person,
    connection_pg: AsyncSession,
) -> list[PersonScore]:
    internal_match_id = str(uuid4())
    cleaned_person = clean_person(person, internal_match_id)

    with duckdb.connect(":memory:") as connection_duckdb:
        candidates_data = await candidate_search_for_record(cleaned_person, connection_pg)
        if not any(candidate["match_id"] != internal_match_id for candidate in candidates_data):
            return []

        return score_candidates(connection_duckdb, internal_match_id, candidates_data)
