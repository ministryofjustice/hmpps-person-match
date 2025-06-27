from pydantic import BaseModel, Field


class IsClusterValid(BaseModel):
    is_cluster_valid: bool = Field(alias="isClusterValid", examples=[True])
    clusters: list[list[str]] = Field(
        examples=[
            ["27fb6f34-ead9-4c62-b1b0-327a0c05c526", "e73386ef-b7bd-485a-827d-0bc1e330d779"],
        ],
    )


class MissingRecordIds(BaseModel):
    unknown_ids: list[str] = Field(alias="unknownIds")
