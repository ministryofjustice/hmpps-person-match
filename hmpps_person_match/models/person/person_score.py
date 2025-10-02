from pydantic import BaseModel, Field


class PersonScore(BaseModel):
    candidate_match_id: str = Field(examples=["ec30e2d2-b4c2-4c42-9e14-514aa58edff5"])
    candidate_match_probability: float = Field(examples=[0.9999999])
    candidate_match_weight: float = Field(examples=[24.0])
    candidate_should_join: bool = Field(examples=[True])
    candidate_should_fracture: bool = Field(examples=[False])
    possible_twins: bool = Field(examples=False)
