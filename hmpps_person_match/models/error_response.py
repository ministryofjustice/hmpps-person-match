import datetime
from typing import Optional

from pydantic import BaseModel


class ErrorResponse(BaseModel):
    """
    Error response model for application error messages
    """

    timestamp: str = datetime.datetime.now(datetime.timezone.utc).isoformat()
    detail: str
    errors: Optional[list[dict]] = []
