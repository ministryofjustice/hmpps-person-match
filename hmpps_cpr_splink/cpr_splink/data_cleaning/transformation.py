from dataclasses import dataclass, field


@dataclass
class Transformation:
    expression: str

    def __str__(self):
        return self.expression

    def full_expression(self, *args):
        return self.expression


class ChainableTransformation(Transformation):
    expression: str

    def full_expression(self, expr: str):
        return f"{expr}\n  .{self.expression}"


class NonChainableTransformation(Transformation): ...


@dataclass
class TransformedColumn:
    column_name: str
    sql_transformations: list[str] = field(default_factory=list)
    column_type: str = None
    alias: str = None

    @property
    def as_column(self):
        return self.alias or self.column_name

    @property
    def expression(self):
        expr = f"{self.column_name}"
        for transformation in self.sql_transformations:
            # print(transformation)
            expr = transformation.full_expression(expr)
        return expr

    @property
    def select_expression(self):
        return f"{self.expression} AS {self.as_column}"

    def __str__(self):
        return self.expression


REGEX_REPLACE_ALL_SPACES = ChainableTransformation(
    "REGEXP_REPLACE('\\s', '', 'g')",
    # "Remove all spaces"
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


def list_filter_remove_specific_str_values(*values: str):
    values_str = ", ".join(f"'{value}'" for value in values)
    return ChainableTransformation(
        f"LIST_FILTER(x -> x not in ({values_str}))",
        # f"Remove values from array: {values_str}",
    )


def list_filter_remove_specific_dates(*dates: str):
    dates_str = ", ".join(f"DATE '{date}'" for date in dates)
    return ChainableTransformation(f"LIST_FILTER(x -> CAST(x AS DATE) not in ({dates_str}))")


def list_filter_out_strings_of_length_lt(length: int):
    return ChainableTransformation(f"LIST_FILTER(x -> LENGTH(x) >= {length})")


REGEXP_MATCHES_EMAIL = ChainableTransformation(r"REGEXP_MATCHES('^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')")
LIST_FILTER_REGEXP_MATCHES_EMAIL = ChainableTransformation(f"LIST_FILTER(x -> x.{REGEXP_MATCHES_EMAIL})")

LIST_FILTER_REGEXP_MATCHES_POSTCODE = ChainableTransformation(f"LIST_FILTER(x -> x.{REGEXP_MATCHES_POSTCODE})")

TRIM_AND_NULLIF_IF_EMPTY = ChainableTransformation("TRIM().NULLIF('')")

LIST_TRANSFORM_TRIM_AND_NULLIF_IF_EMPTY = ChainableTransformation(f"LIST_TRANSFORM(x -> x.{TRIM_AND_NULLIF_IF_EMPTY})")

LIST_FILTER_NULLS = ChainableTransformation("LIST_FILTER(x -> x IS NOT NULL)")

ZERO_LENGTH_ARRAY_TO_NULL = ChainableTransformation("NULLIF(ARRAY[])")


def list_append_from_scalar_column(column_name: str) -> str:
    return ChainableTransformation(f"LIST_APPEND({column_name})")


# These are problematic in Delius
_DATES_TO_REMOVE = ["1970-01-01", "1900-01-01"]
LIST_FILTER_PROBLEM_DOBS = list_filter_remove_specific_dates(*_DATES_TO_REMOVE)

bad_dates_list = ", ".join(f"DATE '{date}'" for date in _DATES_TO_REMOVE)
nullify_dates = f"""
CASE
WHEN date_of_birth in ({bad_dates_list}) then NULL
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

LIST_APPEND_DOB_FROM_SCALAR_COLUMN = list_append_from_scalar_column("date_of_birth")


LIST_FILTER_PROBLEM_CROS = list_filter_remove_specific_str_values("000000/00Z")
LIST_FILTER_PROBLEM_POSTCODES = list_filter_remove_specific_str_values("NF11NF", "NF11FA")

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


@dataclass
class case_when_array_length_greater_equal_or_null(NonChainableTransformation):  # noqa: N801 (reasonable exception)
    threshold: int
    then_clause: str

    def full_expression(self, column: str):
        return f"CASE\n    WHEN array_length({column}) >= {self.threshold} THEN {self.then_clause}\n    ELSE NULL\nEND"


# TODO: get rid of awkward expression at start
case_when_array_length_greater_equal_or_null("", 1, "names_split[1]")


def array_concat_distinct(*args):
    array_str = ", ".join(args)
    return f"array_distinct(array_concat({array_str}))"
