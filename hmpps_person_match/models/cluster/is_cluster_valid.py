from pydantic import BaseModel, Field


class IsClusterValid(BaseModel):
    is_cluster_valid: bool = Field(alias="isClusterValid")
    clusters: list[list[str]]


class MissingRecordIds(BaseModel):
    unknown_ids: list[str]
