from uuid import uuid4

import duckdb
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_cpr_splink.cpr_splink.interface.block import candidate_search_for_record
from hmpps_cpr_splink.cpr_splink.interface.clean import clean_person_for_search
from hmpps_cpr_splink.cpr_splink.interface.score import score_candidates
from hmpps_person_match.models.person.person_score import PersonScore
from hmpps_person_match.models.person.person_search_request import (
    PersonSearchRequest,
    person_search_request_to_person,
)


async def search_candidates(
    search_request: PersonSearchRequest,
    connection_pg: AsyncSession,
) -> list[PersonScore]:
    internal_match_id = str(uuid4())
    person = person_search_request_to_person(search_request, internal_match_id)
    cleaned_person = clean_person_for_search(person, internal_match_id)

    with duckdb.connect(":memory:") as connection_duckdb:
        candidate_records = await candidate_search_for_record(cleaned_person, connection_pg)
        if not any(candidate["match_id"] != internal_match_id for candidate in candidate_records):
            return []

        return score_candidates(connection_duckdb, internal_match_id, candidate_records)
