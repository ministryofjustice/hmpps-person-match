from hmpps_cpr_splink.cpr_splink.data_cleaning.table import Table
from hmpps_cpr_splink.cpr_splink.model_cleaning.clean import (
    POSTCODE_BASIC,
    columns_basic,
    columns_reshaping,
    columns_simple_select,
)

CLEANED_TABLE_SCHEMA = [("id", "INTEGER")] + [(col.as_column, col.column_type) for col in columns_simple_select]


def simple_clean_whole_joined_table(base_table_name: str) -> Table:
    t_basic_cleaned = Table("cleaned_1", *columns_basic, from_table=base_table_name)
    t_name_enhanced = Table("cleaned_2", *columns_reshaping, from_table=t_basic_cleaned)
    return Table("df_cleaned", *columns_simple_select, from_table=t_name_enhanced)


def clean_and_explode_distinct_postcode_arr(base_table_name: str = "df_raw") -> Table:
    """This function finds the distinct values of cleaned postcodes.  It is
    for use in the real-time (scoring) context, where we need to lookup tf
    values for the incoming postcodes from an existing table of tf values

    These values can then be used to create a tf_postcodes table for use by cleaning
    """

    pc_basic_cleaned = Table("pc_cleaned_1", POSTCODE_BASIC, from_table=base_table_name)

    pc_distinct = Table(
        "pc_distinct",
        "DISTINCT UNNEST(postcode_arr) AS value",
        from_table=pc_basic_cleaned,
    )

    return pc_distinct


def create_postcode_tf_from_cpr_joined(base_table_name: str) -> Table:
    """Creates a table of term frequencies for postcodes from the joined CPR data.

    Args:
        base_table_name: Name of the base table containing raw data

    Returns:
        Table: A table with columns (value, rel_freq) containing postcode frequencies
    """
    # First clean the postcode array
    pc_basic_cleaned = Table("pc_cleaned_1", "id", POSTCODE_BASIC, from_table=base_table_name)

    # Get distinct postcodes by unnesting
    pc_distinct_unnested = Table("pc_unnested", "UNNEST(postcode_arr) AS value", from_table=pc_basic_cleaned)

    # Count frequencies
    pc_frequencies = Table(
        "tf_postcodes",
        "value",
        "COUNT(*) as freq",
        "COUNT(*) * 1.0 / (SELECT COUNT(*) FROM pc_unnested) as rel_freq",
        from_table=pc_distinct_unnested,
        post_from_clauses="GROUP BY value",
    )

    return pc_frequencies
