from hmpps_cpr_splink.cpr_splink.model_cleaning.tables import (
    simple_clean_whole_joined_table,
)
from hmpps_cpr_splink.tests.utils.table_assertions import check_data


def sql_for_test_all_cleaning():
    clean_table = simple_clean_whole_joined_table("candidate_search_return_format")
    clean_table.name = "splink_model_format"
    return clean_table.select_statement_with_lineage


@check_data(
    "test_all_cleaning/test_all_cleaning.yaml",
    sql_for_test_all_cleaning(),
    expected_output_table="splink_model_format",
)
def test_all_cleaning() -> None: ...
