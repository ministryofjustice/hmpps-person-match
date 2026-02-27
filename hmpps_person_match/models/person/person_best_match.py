from pydantic import BaseModel, Field


class PersonBestMatch(BaseModel):
     match_status: str = Field(examples=["MATCH","NO_MATCH","POSSIBLE_MATCH"])
