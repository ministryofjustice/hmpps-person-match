import datetime

from pydantic import BaseModel


class ErrorResponse(BaseModel):
    """
    Error response model for application error messages
    """

    timestamp: str = datetime.datetime.now(datetime.UTC).isoformat()
    detail: str
    errors: list[dict] | None = []
