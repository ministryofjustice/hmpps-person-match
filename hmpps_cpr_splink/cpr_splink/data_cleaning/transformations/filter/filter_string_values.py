from hmpps_cpr_splink.cpr_splink.data_cleaning.transformations.chainable_transformation import ChainableTransformation


class FilterByStringValues(ChainableTransformation):
    def __init__(self, *values: str) -> None:
        values_str = ", ".join(f"'{value}'" for value in values)
        super().__init__(expression=f"LIST_FILTER(x -> x not in ({values_str}))")
