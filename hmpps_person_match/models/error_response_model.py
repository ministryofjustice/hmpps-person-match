import datetime

from pydantic import BaseModel


class ErrorResponse(BaseModel):
    """
    Error Response Model
    """
    timestamp: str = datetime.datetime.now(datetime.timezone.utc).isoformat()
    error_message: str
    status_code: int
