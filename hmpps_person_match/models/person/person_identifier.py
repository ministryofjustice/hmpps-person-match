from pydantic import BaseModel


class PersonIdentifier(BaseModel):
    """
    Pydantic Person Identifier Model
    """

    id: str
