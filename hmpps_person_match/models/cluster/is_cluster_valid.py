from pydantic import BaseModel, Field

from hmpps_cpr_splink.cpr_splink.model.model import IS_CLUSTER_VALID_MATCH_WEIGHT_THRESHOLD


class IsClusterValidRequest(BaseModel):
    match_ids: list[str] = Field(alias="matchIds", examples=[["ea59b57f-f3b6-4f77-88dd-64f86d37dffd"]])
    clustering_threshold: float = Field(
        default=IS_CLUSTER_VALID_MATCH_WEIGHT_THRESHOLD, alias="clusteringThreshold", examples=[21.0],
    )


class IsClusterValid(BaseModel):
    is_cluster_valid: bool = Field(alias="isClusterValid", examples=[False])
    clusters: list[list[str]] = Field(
        examples=[
            [["27fb6f34-ead9-4c62-b1b0-327a0c05c526"], ["e73386ef-b7bd-485a-827d-0bc1e330d779"]],
        ],
    )


class MissingRecordIds(BaseModel):
    unknown_ids: list[str] = Field(alias="unknownIds")
