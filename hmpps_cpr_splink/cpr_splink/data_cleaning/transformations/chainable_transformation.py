from hmpps_cpr_splink.cpr_splink.data_cleaning.transformations.transformation import Transformation


class ChainableTransformation(Transformation):
    def __init__(self, expression: str) -> None:
        super().__init__(expression=expression)

    def full_expression(self, expr: str) -> str:
        return f"{expr}\n  .{self.expression}"
