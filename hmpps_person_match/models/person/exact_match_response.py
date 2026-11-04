from pydantic import BaseModel, Field


class ExactMatchResponse(BaseModel):
    match_ids: list[str] = Field(alias="matchIds")
