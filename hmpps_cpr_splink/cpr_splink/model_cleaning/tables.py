from hmpps_cpr_splink.cpr_splink.data_cleaning.table import Table
from hmpps_cpr_splink.cpr_splink.model_cleaning.clean import (
    columns_basic,
    columns_reshaping,
    columns_simple_select,
)

CLEANED_TABLE_SCHEMA = [("id", "INTEGER")] + [(col.as_column, col.column_type) for col in columns_simple_select]


def simple_clean_whole_joined_table(base_table_name: str) -> Table:
    t_basic_cleaned = Table("cleaned_1", *columns_basic, from_table=base_table_name)
    t_name_enhanced = Table("cleaned_2", *columns_reshaping, from_table=t_basic_cleaned)
    return Table("df_cleaned", *columns_simple_select, from_table=t_name_enhanced)
