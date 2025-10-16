from pydantic import BaseModel


class Build(BaseModel):
    """
    Part of Info response model
    """
    version: str

class Commit(BaseModel):
    """
    Part of Info response model
    """
    id: str

class Git(BaseModel):
    """
    Part of Info response model
    """
    branch: str
    commit: Commit

class Info(BaseModel):
    """
    Info response model
    """
    build: Build
    git: Git

