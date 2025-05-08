from hmpps_cpr_splink.cpr_splink.data_cleaning.transformations.chainable_transformation import ChainableTransformation
from hmpps_cpr_splink.cpr_splink.data_cleaning.transformations.filter.filter_date_values import FilterByDateValues
from hmpps_cpr_splink.cpr_splink.data_cleaning.transformations.filter.filter_string_values import FilterByStringValues
from hmpps_cpr_splink.cpr_splink.data_cleaning.transformations.non_chainable_transformation import (
    NonChainableTransformation,
)

REGEX_REPLACE_ALL_SPACES = ChainableTransformation(
    "REGEXP_REPLACE('\\s', '', 'g')",
)

REGEX_FIX_MC_WITH_SPACES = ChainableTransformation(
    "REGEXP_REPLACE('\\bMC\\s+', 'MC', 'g')",
)


REGEX_REPLACE_NOT_ENTERED = ChainableTransformation(
    r"REGEXP_REPLACE('NOT\s+ENTERED', '', 'g')",
)

# https://stackoverflow.com/a/51885364/1779128
REGEXP_MATCHES_POSTCODE = ChainableTransformation(
    r"REGEXP_MATCHES('^([A-Z][A-HJ-Y]?\d[A-Z\d]? ?\d[A-Z]{2}|GIR ?0A{2})$')",
)

# TODO: template this pattern:
LIST_TRANSFORM_REMOVE_ALL_SPACES = ChainableTransformation(
    f"LIST_TRANSFORM(x -> x.{REGEX_REPLACE_ALL_SPACES})",
    # REGEX_REPLACE_ALL_SPACES.description + " in each array element"
)


UPPER = ChainableTransformation("UPPER()")
LIST_TRANSFORM_UPPER = ChainableTransformation("LIST_TRANSFORM(x -> x.UPPER())")

REGEXP_MATCHES_EMAIL = ChainableTransformation(r"REGEXP_MATCHES('^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')")
LIST_FILTER_REGEXP_MATCHES_EMAIL = ChainableTransformation(f"LIST_FILTER(x -> x.{REGEXP_MATCHES_EMAIL})")

LIST_FILTER_REGEXP_MATCHES_POSTCODE = ChainableTransformation(f"LIST_FILTER(x -> x.{REGEXP_MATCHES_POSTCODE})")

TRIM_AND_NULLIF_IF_EMPTY = ChainableTransformation("TRIM().NULLIF('')")

LIST_TRANSFORM_TRIM_AND_NULLIF_IF_EMPTY = ChainableTransformation(f"LIST_TRANSFORM(x -> x.{TRIM_AND_NULLIF_IF_EMPTY})")

LIST_FILTER_NULLS = ChainableTransformation("LIST_FILTER(x -> x IS NOT NULL)")

ZERO_LENGTH_ARRAY_TO_NULL = ChainableTransformation("NULLIF(ARRAY[])")

# These are problematic in Delius
_DATES_TO_REMOVE = ["1970-01-01", "1900-01-01"]
LIST_FILTER_PROBLEM_DOBS = FilterByDateValues(*_DATES_TO_REMOVE)

bad_dates_list = ", ".join(f"DATE '{date}'" for date in _DATES_TO_REMOVE)
nullify_dates = f"""
CASE
    WHEN date_of_birth in ({bad_dates_list}) THEN NULL
    ELSE date_of_birth
END
"""
SCALAR_REMOVE_PROBLEM_DOBS = NonChainableTransformation(nullify_dates)

dob_from_array_if_dob_null = """
CASE
    WHEN
        date_of_birth IS NULL AND array_length(date_of_birth_arr) > 0
        THEN date_of_birth_arr[1]
    ELSE date_of_birth
END
"""
SCALAR_DOB_FROM_ARRAY_IF_DOB_NULL = NonChainableTransformation(dob_from_array_if_dob_null)

LIST_APPEND_DOB_FROM_SCALAR_COLUMN = ChainableTransformation("LIST_APPEND(date_of_birth)")

LIST_FILTER_PROBLEM_CROS = FilterByStringValues("000000/00Z")
LIST_FILTER_PROBLEM_POSTCODES = FilterByStringValues("NF11NF", "NF11FA")

LIST_FILTER_EXCLUDE_1ST_JAN_DATES = ChainableTransformation(
    "LIST_FILTER(x -> CAST(x AS VARCHAR).SUBSTR(6, 5) != '01-01')",
)

NAME_CLEANING_REPLACEMENTS = [
    ("MIG_ERROR_", ""),
    ("NO_SHOW_", ""),
    ("DUPLICATE_", ""),
]

CLEAN_NAMES_BY_REPLACEMENT = ".".join(f"REPLACE('{old}', '{new}')" for old, new in NAME_CLEANING_REPLACEMENTS)

REGEX_SPLIT_TO_ARRAY = ChainableTransformation("REGEXP_SPLIT_TO_ARRAY('\\s+')")


CLEAN_NAME_PUNCTUATION = ChainableTransformation("REPLACE('-', ' ').REPLACE('''', '')")

LIST_SORT = ChainableTransformation("LIST_SORT()")
LIST_DISTINCT = ChainableTransformation("LIST_DISTINCT()")


NAME_CLEANING = ChainableTransformation(f"""
{CLEAN_NAMES_BY_REPLACEMENT}
.{CLEAN_NAME_PUNCTUATION}
.{REGEX_FIX_MC_WITH_SPACES}
.{REGEX_REPLACE_NOT_ENTERED}
""")

LIST_TRANSFORM_NAME_CLEANING = ChainableTransformation(f"LIST_TRANSFORM(x -> x.{NAME_CLEANING})")

LIST_TRANSFORM_TO_OUTCODE_ONLY = ChainableTransformation("LIST_TRANSFORM(x -> SUBSTR(x, 1, LENGTH(x) - 3))")

CONCAT_WS_FIRST_MIDDLE_LAST_NAME = NonChainableTransformation("CONCAT_WS(' ', first_name, middle_names, last_name)")

CONCAT_WS_FIRST_LAST_NAME_STD = NonChainableTransformation("CONCAT_WS(' ', name_1_std, last_name_std)")
