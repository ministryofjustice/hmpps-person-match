from pydantic import BaseModel, Field


class PersonIdentifier(BaseModel):
    """
    Pydantic Person Identifier Model
    """

    match_id: str = Field(alias="matchId", examples=["ec30e2d2-b4c2-4c42-9e14-514aa58edff5"])
