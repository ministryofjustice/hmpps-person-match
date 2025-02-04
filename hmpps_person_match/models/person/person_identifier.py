from pydantic import BaseModel, Field


class PersonIdentifier(BaseModel):
    """
    Pydantic Person Identifier Model
    """

    match_id: str = Field(alias="matchID")
