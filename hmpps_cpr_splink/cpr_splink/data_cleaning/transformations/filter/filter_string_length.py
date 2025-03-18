from hmpps_cpr_splink.cpr_splink.data_cleaning.transformations.chainable_transformation import ChainableTransformation


class FilterByStringLength(ChainableTransformation):
    def __init__(self, length: int):
        super().__init__(expression=f"LIST_FILTER(x -> LENGTH(x) >= {length})")
