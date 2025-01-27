from cpr_splink.data_cleaning.join_raw_tables import join_raw_tables_sql
from tests.utils.table_assertions import check_data

sql_join_raw_tables = join_raw_tables_sql(
    "person", "pseudonym", "address", "reference", "sentence_info"
)


@check_data(
    "test_joining_raw/test_joining_raw_tables.yaml",
    sql_join_raw_tables,
    expected_output_table="candidate_search_return_format",
)
def test_join_raw_tables(): ...
