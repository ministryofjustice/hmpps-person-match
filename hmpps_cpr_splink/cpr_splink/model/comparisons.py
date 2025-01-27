# ruff: noqa: E501  (disable line length for this module)
import splink.comparison_level_library as cll
import splink.comparison_library as cl

name_comparison = cl.CustomComparison(
    output_column_name="name_comparison",
    comparison_levels=[
        cll.And(cll.NullLevel("name_1_std"), cll.NullLevel("last_name_std")).configure(
            is_null_level=True
        ),
        cll.ExactMatchLevel("first_and_last_name_std").configure(
            tf_adjustment_column="first_and_last_name_std"
        ),
        cll.JaroWinklerLevel(
            "first_and_last_name_std", distance_threshold=0.9
        ).configure(
            tf_adjustment_column="first_and_last_name_std", tf_adjustment_weight=0.7
        ),
        cll.ColumnsReversedLevel("name_1_std", "last_name_std", symmetrical=True),
        cll.And(
            cll.ArrayIntersectLevel("forename_std_arr", 1),
            cll.ArrayIntersectLevel("last_name_std_arr", 1),
        ),
        cll.And(
            cll.JaroWinklerLevel("name_1_std", distance_threshold=0.7),
            cll.JaroWinklerLevel("last_name_std", distance_threshold=0.7),
        ),
        cll.ExactMatchLevel("name_1_std").configure(tf_adjustment_column="name_1_std"),
        cll.ExactMatchLevel("last_name_std").configure(
            tf_adjustment_column="last_name_std"
        ),
        cll.ElseLevel(),
    ],
)

name_2_comparison = cl.JaroWinklerAtThresholds(
    "name_2_std", score_threshold_or_thresholds=[0.85]
).configure(term_frequency_adjustments=True)

# Setence date doesn't have much skew so TF adjustments not necesasry
intersection_sql = (
    'array_length(list_intersect("sentence_date_arr_l", "sentence_date_arr_r")) >= 1'
)
few_elements_sql = 'len("sentence_date_arr_l") * len("sentence_date_arr_r") <= 9'
date_diff_sql = """
    abs(datediff('day', sentence_date_arr_l[1], sentence_date_arr_r[1])) <= 14
    or
    abs(datediff('day', sentence_date_arr_l[-1], sentence_date_arr_r[-1])) <= 14
"""

sentence_date_comparison = cl.CustomComparison(
    output_column_name="sentence_date_arr",
    comparison_levels=[
        cll.NullLevel("sentence_date_arr"),
        {
            "sql_condition": f"{intersection_sql} and {few_elements_sql}",
            "label_for_charts": "Array intersection size >= 1, few elements",
        },
        {
            "sql_condition": intersection_sql,
            "label_for_charts": "Array intersection size >= 1, many elements",
        },
        {
            "sql_condition": date_diff_sql,
            "label_for_charts": "First or last elements within 2 weeks",
        },
        cll.ElseLevel(),
    ],
)

# Don't need tf adjustments on arrays because we removed the 01-01 values
# from the arr so the distribution of frequencies is not very skewed
dob_intersection_sql = """
array_length(
    list_intersect("date_of_birth_arr_l", "date_of_birth_arr_r")
)
"""
date_of_birth_comparison = cl.CustomComparison(
    output_column_name="date_of_birth_arr",
    comparison_levels=[
        # date_of_birth is guaranteed to be in date_of_birth_arr so it's fine to
        # use date_of_birth_arr as the null column
        cll.NullLevel("date_of_birth_arr"),
        cll.ExactMatchLevel("date_of_birth").configure(
            tf_adjustment_column="date_of_birth",
        ),
        {
            "sql_condition": f"{dob_intersection_sql} >= 1",
            "label_for_charts": "Array intersection size >= 1",
        },
        cll.DamerauLevenshteinLevel(
            "cast(date_of_birth as varchar)", distance_threshold=1
        ),
        cll.AbsoluteDateDifferenceLevel(
            "date_of_birth", threshold=5, metric="year", input_is_string=False
        ),
        cll.ElseLevel(),
    ],
)

# See https://github.com/RobinL/uk_address_matcher/issues/11
tf_product_sql = """
    list_reduce(
        list_prepend(
            1.0,
            list_transform(
                postcode_arr_with_freq_l,
                x -> CASE
                        WHEN array_contains(
                            list_transform(postcode_arr_with_freq_r, y -> y.value),
                            x.value
                        )
                        THEN x.rel_freq
                        ELSE 1.0
                    END
            )
        ),
        (p, q) -> p * q
    )"""

postcode_comparison = cl.CustomComparison(
    output_column_name="postcode_arr",
    comparison_levels=[
        cll.NullLevel("postcode_arr"),
        {
            "sql_condition": f"{tf_product_sql} < 0.00001",
            "label_for_charts": "tf arr mul < 0.00001",
        },
        {
            "sql_condition": f"{tf_product_sql} < 0.0001",
            "label_for_charts": "tf arr mul < 0.0001",
        },
        {
            "sql_condition": f"{tf_product_sql} < 0.001",
            "label_for_charts": "tf arr mul < 0.001",
        },
        cll.ArrayIntersectLevel("postcode_outcode_arr", 1),
        cll.ElseLevel(),
    ],
)

cro_comparison = cl.LevenshteinAtThresholds(
    "cro_single", distance_threshold_or_thresholds=1
).configure(
    term_frequency_adjustments=True,
)


pnc_comparison = cl.LevenshteinAtThresholds(
    "pnc_single", distance_threshold_or_thresholds=1
).configure(
    term_frequency_adjustments=True,
)
