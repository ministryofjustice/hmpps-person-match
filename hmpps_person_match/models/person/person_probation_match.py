from pydantic import BaseModel, Field


class PersonProbationMatch(BaseModel):
     match_status: str = Field(examples=["MATCH","NO_MATCH","POSSIBLE_MATCH"])
