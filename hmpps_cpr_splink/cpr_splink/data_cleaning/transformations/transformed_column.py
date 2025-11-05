from dataclasses import dataclass, field

from hmpps_cpr_splink.cpr_splink.data_cleaning.transformations.transformation import Transformation


@dataclass
class TransformedColumn:
    column_name: str | Transformation
    sql_transformations: list[Transformation] = field(default_factory=list)
    column_type: str | None = None
    alias: str | None = None

    @property
    def as_column(self) -> str:
        return self.alias or self.column_name

    @property
    def expression(self) -> str:
        expr = f"{self.column_name}"
        for transformation in self.sql_transformations:
            expr = transformation.full_expression(expr)
        return expr

    @property
    def select_expression(self) -> str:
        return f"{self.expression} AS {self.as_column}"

    def __str__(self) -> str:
        return self.expression
