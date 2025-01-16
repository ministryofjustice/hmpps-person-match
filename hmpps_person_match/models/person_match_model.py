from pydantic import BaseModel, Field


class Person(BaseModel):
    """
    Pydantic Person Model
    """

    source_system: str = Field(alias="sourceSystem")
