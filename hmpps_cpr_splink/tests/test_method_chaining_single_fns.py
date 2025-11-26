from hmpps_cpr_splink.cpr_splink.data_cleaning.transformations import (
    LIST_FILTER_PROBLEM_CROS,
    LIST_TRANSFORM_REMOVE_ALL_SPACES,
)
from hmpps_cpr_splink.cpr_splink.data_cleaning.transformations.filter.filter_string_length import FilterByStringLength
from hmpps_cpr_splink.tests.utils.table_assertions import check_data

sql_list_transform_remove_spaces = f"""
SELECT
    input_column.{LIST_TRANSFORM_REMOVE_ALL_SPACES} AS output_column
FROM input_table
"""


@check_data(
    "test_method_chaining_single_fns/test_list_transform_remove_all_spaces.yaml",
    sql_list_transform_remove_spaces,
)
def test_list_transform_remove_all_spaces() -> None: ...


sql_list_filter_problem_cros = f"""
SELECT
    input_column.{LIST_FILTER_PROBLEM_CROS} AS output_column
FROM input_table
"""


@check_data(
    "test_method_chaining_single_fns/test_list_filter_problem_cros.yaml",
    sql_list_filter_problem_cros,
)
def test_list_filter_problem_cros() -> None: ...


sql_list_append_from_scalar = """
SELECT
    input_column.LIST_APPEND("scalar_column") AS output_column
FROM input_table
"""


@check_data(
    "test_method_chaining_single_fns/test_list_append_from_scalar.yaml",
    sql_list_append_from_scalar,
    expected_output_table="output_table",
)
def test_list_append_from_scalar() -> None: ...


sql_list_filter_out_strings_of_length_lt = f"""
SELECT
    input_column.{FilterByStringLength(length=2)} AS output_column
FROM input_table
"""


@check_data(
    "test_method_chaining_single_fns/test_list_filter_out_strings_of_length_lt.yaml",
    sql_list_filter_out_strings_of_length_lt,
)
def test_list_filter_out_strings_of_length_lt() -> None: ...
