from cpr_splink.data_cleaning.transformation import (
    LIST_FILTER_PROBLEM_CROS,
    LIST_TRANSFORM_REMOVE_ALL_SPACES,
    list_append_from_scalar_column,
    list_filter_out_strings_of_length_lt,
)
from tests.utils.table_assertions import check_data

sql_list_transform_remove_spaces = f"""
SELECT
    input_column.{LIST_TRANSFORM_REMOVE_ALL_SPACES} AS output_column
FROM input_table
"""


@check_data(
    "test_method_chaining_single_fns/test_list_transform_remove_all_spaces.yaml",
    sql_list_transform_remove_spaces,
)
def test_list_transform_remove_all_spaces(): ...


sql_list_filter_problem_cros = f"""
SELECT
    input_column.{LIST_FILTER_PROBLEM_CROS} AS output_column
FROM input_table
"""


@check_data(
    "test_method_chaining_single_fns/test_list_filter_problem_cros.yaml",
    sql_list_filter_problem_cros,
)
def test_list_filter_problem_cros(): ...


sql_list_append_from_scalar = f"""
SELECT
    input_column.{list_append_from_scalar_column("scalar_column")} AS output_column
FROM input_table
"""


@check_data(
    "test_method_chaining_single_fns/test_list_append_from_scalar.yaml",
    sql_list_append_from_scalar,
    expected_output_table="output_table",
)
def test_list_append_from_scalar(): ...


sql_list_filter_out_strings_of_length_lt = f"""
SELECT
    input_column.{list_filter_out_strings_of_length_lt(2)} AS output_column
FROM input_table
"""


@check_data(
    "test_method_chaining_single_fns/test_list_filter_out_strings_of_length_lt.yaml",
    sql_list_filter_out_strings_of_length_lt,
)
def test_list_filter_out_strings_of_length_lt(): ...
