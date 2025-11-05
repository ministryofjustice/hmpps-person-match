from hmpps_cpr_splink.cpr_splink.data_cleaning.transformations.non_chainable_transformation import (
    NonChainableTransformation,
)


class ArrayLengthGreaterEqualOrNull(NonChainableTransformation):
    def __init__(self, threshold: int, then_clause: str) -> None:
        super().__init__(expression="")
        self.threshold = threshold
        self.then_clause = then_clause

    def full_expression(self, column: str) -> str:
        return f"CASE\n    WHEN array_length({column}) >= {self.threshold} THEN {self.then_clause}\n    ELSE NULL\nEND"
