from typing import Annotated, Optional

from annotated_types import Len
from pydantic import BaseModel


class Person(BaseModel):
    """
    Pydantic Person Model
    """

    unique_id: str
    pnc: Optional[str] = None
    dob: Optional[str] = None
    lastname: Optional[str] = None
    firstname1: Optional[str] = None
    firstname2: Optional[str] = None
    firstname3: Optional[str] = None


class MatchingFromPerson(Person):
    source_dataset: str = "matching_from"


class MatchingToPerson(Person):
    source_dataset: str = "matching_to"


class PersonMatching(BaseModel):
    """
    List of people to match
    """

    matching_from: MatchingFromPerson
    matching_to: Annotated[list[MatchingToPerson], Len(min_length=1, max_length=100)]
