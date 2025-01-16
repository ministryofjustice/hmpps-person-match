from pydantic import BaseModel


class Person(BaseModel):
    """
    Pydantic Person Model
    """
    source_system: str
