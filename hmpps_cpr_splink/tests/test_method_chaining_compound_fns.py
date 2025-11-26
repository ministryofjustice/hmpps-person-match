from hmpps_cpr_splink.cpr_splink.data_cleaning.transformations import (
    LIST_TRANSFORM_NAME_CLEANING,
    NAME_CLEANING,
)
from hmpps_cpr_splink.cpr_splink.model_cleaning.clean import POSTCODE_BASIC
from hmpps_cpr_splink.tests.utils.table_assertions import check_data

sql_name_cleaning = f"""
SELECT
    input_column.{NAME_CLEANING} AS output_column
FROM input_table
"""


@check_data(
    "test_method_chaining_compound_fns/test_individual_names_cleaning.yaml",
    sql_name_cleaning,
)
def test_name_cleaning() -> None: ...


sql_list_transform_name_cleaning = f"""
SELECT
    input_column.{LIST_TRANSFORM_NAME_CLEANING} AS output_column
FROM input_table
"""


@check_data(
    "test_method_chaining_compound_fns/test_list_transform_name_cleaning.yaml",
    sql_list_transform_name_cleaning,
)
def test_list_transform_name_cleaning() -> None: ...


POSTCODE_BASIC_INPUT_COLUMN = POSTCODE_BASIC.__str__().replace("postcodes", "input_column")
sql_postcode_cleaning = f"""
SELECT
    {POSTCODE_BASIC_INPUT_COLUMN} AS output_column
FROM input_table
"""


@check_data(
    "test_method_chaining_compound_fns/test_individual_postcode_cleaning.yaml",
    sql_postcode_cleaning,
)
def test_postcode_cleaning() -> None: ...
