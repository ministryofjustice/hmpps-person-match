from hmpps_person_match.models.person.person import Person
from hmpps_person_match.models.person.person_score import PersonScore


async def search_candidates(person: Person) -> list[PersonScore]:
    """
    Temporary stub service.

    Later this will orchestrate the PostgreSQL blocking phase and DuckDB
    scoring phase for an ad hoc search record.
    """

    return []
