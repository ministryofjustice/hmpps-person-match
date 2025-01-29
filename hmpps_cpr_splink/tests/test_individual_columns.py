from hmpps_cpr_splink.cpr_splink.data_cleaning.table import Table
from hmpps_cpr_splink.cpr_splink.model_cleaning.clean import (
    columns_basic,
    columns_reshaping,
    columns_simple_select,
)
from hmpps_cpr_splink.cpr_splink.model_cleaning.term_frequencies import lookup_term_frequencies
from hmpps_cpr_splink.tests.utils.table_assertions import check_data


def filter_columns(columns, columns_to_keep):
    return [
        c
        for c in columns
        if c.column_name in columns_to_keep or c.alias in columns_to_keep
    ]


DOB_COLUMNS = ["date_of_birth", "date_of_birth_arr", "date_of_birth_alias_arr"]


# DOB test setup
dob_columns_basic = filter_columns(columns_basic, DOB_COLUMNS)
dob_columns_reshaping = filter_columns(columns_reshaping, DOB_COLUMNS)

t_dob_basic_cleaned = Table("cleaned_1", *dob_columns_basic, from_table="input_table")
t_dob_enhanced = Table(
    "cleaned_2", *dob_columns_reshaping, from_table=t_dob_basic_cleaned
)


@check_data(
    "test_individual_columns/test_dob_arr_end_to_end.yaml",
    t_dob_enhanced.select_statement_with_lineage,
    expected_output_table="cleaned_2",
)
def test_dob_processing_end_to_end(): ...


NAME_COLUMNS = [
    "first_name",
    "middle_names",
    "last_name",
    "first_name_alias_arr",
    "last_name_aliases",
    "name_1_std",
    "name_2_std",
    "name_3_std",
    "last_name_std",
    "first_and_last_name_std",
    "forename_std_arr",
    "last_name_std_arr",
    "names_split",
]

name_columns_basic = filter_columns(columns_basic, NAME_COLUMNS)
name_columns_reshaping = filter_columns(columns_reshaping, NAME_COLUMNS)
name_columns_simple_select = filter_columns(columns_simple_select, NAME_COLUMNS)
t_name_basic_cleaned = Table("cleaned_1", *name_columns_basic, from_table="input_table")
t_name_enhanced = Table(
    "cleaned_2", *name_columns_reshaping, from_table=t_name_basic_cleaned
)
t_name_simple_select = Table(
    "cleaned_3", *name_columns_simple_select, from_table=t_name_enhanced
)


@check_data(
    "test_individual_columns/test_names_end_to_end.yaml",
    t_name_simple_select.select_statement_with_lineage,
    expected_output_table="cleaned_2",
)
def test_name_processing_end_to_end(): ...


SENTENCE_DATE_COLUMNS = ["sentence_date_arr", "sentence_date_single"]

sentence_date_columns_basic = filter_columns(columns_basic, SENTENCE_DATE_COLUMNS)
sentence_date_columns_reshaping = filter_columns(
    columns_reshaping, SENTENCE_DATE_COLUMNS
)

t_sentence_date_basic = Table(
    "cleaned_1", *sentence_date_columns_basic, from_table="input_table"
)
t_sentence_date_enhanced = Table(
    "cleaned_2", *sentence_date_columns_reshaping, from_table=t_sentence_date_basic
)


@check_data(
    "test_individual_columns/test_sentence_dates_end_to_end.yaml",
    t_sentence_date_enhanced.select_statement_with_lineage,
    expected_output_table="cleaned_2",
)
def test_sentence_date_processing_end_to_end(): ...


POSTCODE_COLUMNS = [
    "id",
    "postcode_arr",
    "postcode_outcode_arr",
    "postcode_arr_with_freq",
]

postcode_columns_basic = filter_columns(columns_basic, POSTCODE_COLUMNS)
postcode_columns_reshaping = filter_columns(columns_reshaping, POSTCODE_COLUMNS)

t_postcode_basic = Table("cleaned_1", *postcode_columns_basic, from_table="input_table")
t_postcode_enhanced = Table(
    "cleaned_2", *postcode_columns_reshaping, from_table=t_postcode_basic
)

t_postcode_with_freq = lookup_term_frequencies(
    "postcode_arr", "tf_postcodes", t_postcode_enhanced
)
t_postcode_final = Table(
    "cleaned_2",
    f"{t_postcode_enhanced}.*",
    "agg_table_postcode_arr.postcode_arr_with_freq",
    from_table=t_postcode_with_freq,
    post_from_clauses=(
        f"RIGHT JOIN {t_postcode_enhanced} ON "
        f"{t_postcode_enhanced}.id = {t_postcode_with_freq}.id"
    ),
)


@check_data(
    "test_individual_columns/test_postcodes_end_to_end.yaml",
    t_postcode_final.select_statement_with_lineage,
    expected_output_table="cleaned_2",
)
def test_postcode_processing_end_to_end(): ...
