from enum import Enum

from pydantic import BaseModel


class Status(str, Enum):
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
