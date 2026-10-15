from pydantic import BaseModel, Field


class EmdiExactMatchResponse(BaseModel):
    match_ids: list[str] = Field(alias="matchIds")
