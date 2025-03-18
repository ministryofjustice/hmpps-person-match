from hmpps_cpr_splink.cpr_splink.data_cleaning.transformation import (
    CONCAT_WS_FIRST_LAST_NAME_STD,
    CONCAT_WS_FIRST_MIDDLE_LAST_NAME,
    LIST_APPEND_DOB_FROM_SCALAR_COLUMN,
    LIST_DISTINCT,
    LIST_FILTER_NULLS,
    LIST_FILTER_PROBLEM_CROS,
    LIST_FILTER_PROBLEM_DOBS,
    LIST_FILTER_PROBLEM_POSTCODES,
    LIST_FILTER_REGEXP_MATCHES_POSTCODE,
    LIST_SORT,
    LIST_TRANSFORM_NAME_CLEANING,
    LIST_TRANSFORM_REMOVE_ALL_SPACES,
    LIST_TRANSFORM_TO_OUTCODE_ONLY,
    LIST_TRANSFORM_TRIM_AND_NULLIF_IF_EMPTY,
    LIST_TRANSFORM_UPPER,
    NAME_CLEANING,
    REGEX_SPLIT_TO_ARRAY,
    SCALAR_DOB_FROM_ARRAY_IF_DOB_NULL,
    SCALAR_REMOVE_PROBLEM_DOBS,
    TRIM_AND_NULLIF_IF_EMPTY,
    UPPER,
    ZERO_LENGTH_ARRAY_TO_NULL,
    TransformedColumn,
    array_concat_distinct,
    case_when_array_length_greater_equal_or_null,
    list_filter_out_strings_of_length_lt,
)


def get_column_from_array(columns: list[TransformedColumn], column_name: str) -> TransformedColumn:
    return next([col for col in columns if col.column_name == column_name])


# first pass at cleaning
# simple transformations on a per-column basis
POSTCODE_BASIC = TransformedColumn(
    "postcodes",
    [
        LIST_TRANSFORM_UPPER,
        LIST_TRANSFORM_REMOVE_ALL_SPACES,
        LIST_FILTER_PROBLEM_POSTCODES,
        LIST_FILTER_REGEXP_MATCHES_POSTCODE,
        LIST_FILTER_NULLS,
        LIST_DISTINCT,
        LIST_SORT,
        ZERO_LENGTH_ARRAY_TO_NULL,
    ],
    "VARCHAR[]",
    alias="postcode_arr",
)


columns_basic = [
    TransformedColumn("match_id", column_type="VARCHAR"),
    TransformedColumn("source_system", [UPPER], "VARCHAR"),
    TransformedColumn("first_name", [UPPER, NAME_CLEANING], "VARCHAR"),
    TransformedColumn("middle_names", [UPPER, NAME_CLEANING], "VARCHAR"),
    TransformedColumn("last_name", [UPPER, NAME_CLEANING], "VARCHAR"),
    TransformedColumn(
        "date_of_birth",
        [SCALAR_REMOVE_PROBLEM_DOBS],
        column_type="DATE",
    ),
    TransformedColumn(
        "first_name_aliases",
        [LIST_TRANSFORM_UPPER, LIST_TRANSFORM_NAME_CLEANING],
        "VARCHAR[]",
        alias="first_name_alias_arr",
    ),
    TransformedColumn(
        "last_name_aliases",
        [LIST_TRANSFORM_UPPER, LIST_TRANSFORM_NAME_CLEANING],
        "VARCHAR[]",
        alias="last_name_alias_arr",
    ),
    TransformedColumn(
        "date_of_birth_aliases",
        [LIST_FILTER_PROBLEM_DOBS],
        column_type="DATE[]",
        alias="date_of_birth_arr",
    ),
    POSTCODE_BASIC,
    TransformedColumn("crn", [TRIM_AND_NULLIF_IF_EMPTY], column_type="VARCHAR"),
    TransformedColumn("prison_number", [TRIM_AND_NULLIF_IF_EMPTY], column_type="VARCHAR"),
    TransformedColumn(
        "cros",
        [LIST_TRANSFORM_UPPER, LIST_DISTINCT, LIST_FILTER_PROBLEM_CROS, LIST_SORT],
        "VARCHAR[]",
        alias="cro_arr",
    ),
    TransformedColumn(
        "pncs",
        [LIST_TRANSFORM_UPPER, LIST_DISTINCT, LIST_SORT],
        "VARCHAR[]",
        alias="pnc_arr",
    ),
    TransformedColumn(
        "sentence_dates",
        [
            LIST_FILTER_PROBLEM_DOBS,
            LIST_DISTINCT,
            LIST_SORT,
        ],  # TODO: Remove?  Problem dob vs. sentence dates?
        alias="sentence_date_arr",
    ),
]


# a second cleaning pass now moves these into a shape closer to what we will use
TIDY_NULLS_FROM_ARRAY = [
    LIST_TRANSFORM_TRIM_AND_NULLIF_IF_EMPTY,
    LIST_DISTINCT,
    LIST_SORT,
    ZERO_LENGTH_ARRAY_TO_NULL,
]
columns_reshaping = [
    TransformedColumn("match_id"),
    TransformedColumn("source_system"),
    TransformedColumn("sentence_date_arr"),
    TransformedColumn("postcode_arr"),
    TransformedColumn("crn"),
    TransformedColumn("prison_number"),
    TransformedColumn(
        CONCAT_WS_FIRST_MIDDLE_LAST_NAME,
        [
            REGEX_SPLIT_TO_ARRAY,
            list_filter_out_strings_of_length_lt(2),
        ],
        alias="names_split",
    ),
    TransformedColumn(
        "names_split",
        [case_when_array_length_greater_equal_or_null("", 2, "names_split[1]")],
        alias="name_1_std",
    ),
    TransformedColumn(
        "names_split",
        [case_when_array_length_greater_equal_or_null("", 3, "names_split[2]")],
        alias="name_2_std",
    ),
    TransformedColumn(
        "names_split",
        [case_when_array_length_greater_equal_or_null("", 4, "names_split[3]")],
        alias="name_3_std",
    ),
    TransformedColumn(
        "names_split[-1]",
        alias="last_name_std",
    ),
    TransformedColumn(
        array_concat_distinct("[name_1_std]", "first_name_alias_arr"),
        TIDY_NULLS_FROM_ARRAY,
        alias="forename_std_arr",
    ),
    TransformedColumn(
        array_concat_distinct("names_split[-1:]", "last_name_alias_arr"),
        TIDY_NULLS_FROM_ARRAY,
        alias="last_name_std_arr",
    ),
    TransformedColumn(
        "source_system",
        alias="source_dataset",
    ),
    TransformedColumn(
        CONCAT_WS_FIRST_LAST_NAME_STD,
        [TRIM_AND_NULLIF_IF_EMPTY],
        alias="first_and_last_name_std",
    ),
    TransformedColumn(
        "date_of_birth_arr",
        [
            LIST_APPEND_DOB_FROM_SCALAR_COLUMN,
            LIST_DISTINCT,
            LIST_FILTER_NULLS,
            LIST_SORT,
            ZERO_LENGTH_ARRAY_TO_NULL,
        ],
        column_type="DATE[]",
        alias="date_of_birth_arr",
    ),
    TransformedColumn(
        "date_of_birth",
        [SCALAR_DOB_FROM_ARRAY_IF_DOB_NULL],
        column_type="DATE[]",
        alias="date_of_birth",
    ),
    TransformedColumn(
        "postcode_arr",
        [
            LIST_TRANSFORM_TO_OUTCODE_ONLY,
            LIST_DISTINCT,
            LIST_SORT,
            ZERO_LENGTH_ARRAY_TO_NULL,
        ],
        alias="postcode_outcode_arr",
    ),
    TransformedColumn(
        "sentence_date_arr[-1]",
        alias="sentence_date_single",
    ),
    TransformedColumn(
        "cro_arr[1]",
        alias="cro_single",
    ),
    TransformedColumn(
        "pnc_arr[1]",
        alias="pnc_single",
    ),
    TransformedColumn(
        "postcode_arr[1]",
        alias="postcode_first",
    ),
    TransformedColumn(
        "postcode_arr[2]",
        alias="postcode_second",
    ),
    TransformedColumn(
        "postcode_arr[-1]",
        alias="postcode_last",
    ),
    TransformedColumn(
        "postcode_outcode_arr[1]",
        alias="postcode_outcode_first",
    ),
    TransformedColumn(
        "postcode_outcode_arr[-1]",
        alias="postcode_outcode_last",
    ),
    TransformedColumn(
        "date_of_birth_arr[-1]",
        alias="date_of_birth_last",
    ),
    TransformedColumn(
        "forename_std_arr[1]",
        alias="forename_first",
    ),
    TransformedColumn(
        "forename_std_arr[-1]",
        alias="forename_last",
    ),
    TransformedColumn(
        "last_name_std_arr[1]",
        alias="last_name_first",
    ),
    TransformedColumn(
        "last_name_std_arr[-1]",
        alias="last_name_last",
    ),
    TransformedColumn(
        "sentence_date_arr[-1]",
        alias="sentence_date_first",
    ),
    TransformedColumn(
        "sentence_date_arr[-1]",
        alias="sentence_date_last",
    ),
]

columns_simple_select = [
    # core info
    TransformedColumn("match_id", column_type="VARCHAR"),
    TransformedColumn("source_system", column_type="VARCHAR"),
    # names
    TransformedColumn("name_1_std", column_type="VARCHAR"),
    TransformedColumn("name_2_std", column_type="VARCHAR"),
    TransformedColumn("name_3_std", column_type="VARCHAR"),
    TransformedColumn("last_name_std", column_type="VARCHAR"),
    TransformedColumn("first_and_last_name_std", column_type="VARCHAR"),
    TransformedColumn("forename_std_arr", column_type="VARCHAR[]"),
    TransformedColumn("forename_first", column_type="VARCHAR"),
    TransformedColumn("forename_last", column_type="VARCHAR"),
    TransformedColumn("last_name_std_arr", column_type="VARCHAR[]"),
    TransformedColumn("last_name_first", column_type="VARCHAR"),
    TransformedColumn("last_name_last", column_type="VARCHAR"),
    # dates
    TransformedColumn("date_of_birth", column_type="DATE"),
    TransformedColumn("date_of_birth_arr", column_type="DATE[]"),
    TransformedColumn("date_of_birth_last", column_type="DATE"),
    TransformedColumn("sentence_date_single", column_type="DATE"),
    TransformedColumn("sentence_date_arr", column_type="DATE[]"),
    TransformedColumn("sentence_date_first", column_type="DATE"),
    TransformedColumn("sentence_date_last", column_type="DATE"),
    # location
    TransformedColumn("postcode_arr", column_type="VARCHAR[]"),
    TransformedColumn("postcode_first", column_type="VARCHAR"),
    TransformedColumn("postcode_second", column_type="VARCHAR"),
    TransformedColumn("postcode_last", column_type="VARCHAR"),
    TransformedColumn("postcode_outcode_arr", column_type="VARCHAR[]"),
    TransformedColumn("postcode_outcode_first", column_type="VARCHAR"),
    TransformedColumn("postcode_outcode_last", column_type="VARCHAR"),
    # identifiers
    TransformedColumn("cro_single", column_type="VARCHAR"),
    TransformedColumn("pnc_single", column_type="VARCHAR"),
    TransformedColumn("crn", column_type="VARCHAR"),
    TransformedColumn("prison_number", column_type="VARCHAR"),
]
