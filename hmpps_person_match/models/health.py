from enum import StrEnum

from pydantic import BaseModel


class Status(StrEnum):
    """
    Application status enum
    """

    UP = "UP"
    DOWN = "DOWN"


class Health(BaseModel):
    """
    Health response model
    """

    status: Status
