from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_cpr_splink.cpr_splink.interface.clean import clean_person_for_search
from hmpps_person_match.models.person.emdi_exact_match_request import (
    EmdiExactMatchRequest,
    emdi_exact_match_request_to_person,
)


class InvalidEmdiExactMatchInputError(ValueError):
    pass


async def emdi_exact_match(request: EmdiExactMatchRequest, session: AsyncSession) -> list[str]:
    internal_match_id = str(uuid4())
    person = emdi_exact_match_request_to_person(request, internal_match_id)
    cleaned_person = clean_person_for_search(person, internal_match_id)
    parameters = {
        column: cleaned_person[column] for column in ("pnc_single", "name_1_std", "last_name_std", "date_of_birth")
    }
    if any(value is None or (isinstance(value, str) and not value.strip()) for value in parameters.values()):
        raise InvalidEmdiExactMatchInputError("Required comparison values must not be blank")

    result = await session.execute(
        text("""
            SELECT match_id
            FROM personmatch.person
            WHERE pnc_single = :pnc_single
              AND name_1_std = :name_1_std
              AND last_name_std = :last_name_std
              AND date_of_birth = :date_of_birth
            ORDER BY match_id
        """),
        parameters,
    )
    return list(result.scalars().all())
