from typing import Annotated

from annotated_types import Len
from pydantic import BaseModel

from hmpps_person_match.models.person.person import Person


class PersonBatch(BaseModel):
    """
    Pydantic Batch Person Model
    """

    records: Annotated[list[Person], Len(min_length=1, max_length=1000)]
