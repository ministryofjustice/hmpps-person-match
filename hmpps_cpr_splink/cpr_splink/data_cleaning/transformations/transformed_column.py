from dataclasses import dataclass, field


@dataclass
class TransformedColumn:
    column_name: str
    sql_transformations: list[str] = field(default_factory=list)
    column_type: str = None
    alias: str = None

    @property
    def as_column(self):
        return self.alias or self.column_name

    @property
    def expression(self):
        expr = f"{self.column_name}"
        for transformation in self.sql_transformations:
            expr = transformation.full_expression(expr)
        return expr

    @property
    def select_expression(self):
        return f"{self.expression} AS {self.as_column}"

    def __str__(self):
        return self.expression
