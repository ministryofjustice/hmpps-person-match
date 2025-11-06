from hmpps_cpr_splink.cpr_splink.data_cleaning.transformations.chainable_transformation import ChainableTransformation


class FilterByDateValues(ChainableTransformation):
    def __init__(self, *dates: str) -> None:
        dates_str = ", ".join(f"DATE '{date}'" for date in dates)
        super().__init__(expression=f"LIST_FILTER(x -> CAST(x AS DATE) not in ({dates_str}))")
