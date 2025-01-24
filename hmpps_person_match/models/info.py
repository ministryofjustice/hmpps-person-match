from pydantic import BaseModel


class Info(BaseModel):
    """
    Info response model
    """

    version: str
    commit_id: str
    branch: str
