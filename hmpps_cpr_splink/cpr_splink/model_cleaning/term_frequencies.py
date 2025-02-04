from ..data_cleaning.table import Table
from .clean import TransformedColumn


def calculate_array_element_frequencies_table(
    input_array_col_name: str,
    output_col_name: str,
    source_table: str | Table,
) -> Table:
    """Calculate frequency distribution of elements in an array column.

    Args:
        input_array_col_name: Name of array column to analyze
        output_col_name: Name for the output column containing individual values
        source_table: Table containing the array column

    Returns:
        Table with value frequencies, ordered by descending frequency

    Example:
        Given source table:
        | id | names_arr           |
        |----|---------------------|
        | 1  | [John, John, Mary]  |
        | 2  | [Bob, Mary]         |

        freqs = calculate_array_element_frequencies_table("names_arr", "name", source)

        Result:
        | name | tf_name |
        |------|---------|
        | John | 0.400   |
        | Mary | 0.400   |
        | Bob  | 0.200   |
    """
    exploded_tn = f"exploded_{output_col_name}"
    tf_tn = f"tf_{output_col_name}"

    t_exploded = Table(
        exploded_tn,
        "id",
        TransformedColumn(f"unnest({input_array_col_name})", alias=output_col_name),
        from_table=source_table,
    )

    t_term_freqs = Table(
        tf_tn,
        TransformedColumn(output_col_name, alias="value"),
        TransformedColumn("COUNT(*) / SUM(COUNT(*)) OVER ()", alias="rel_freq"),
        from_table=t_exploded,
        post_from_clauses=("GROUP BY value\nORDER BY rel_freq DESC"),
    )

    return t_term_freqs


def lookup_term_frequencies(input_array_col_name: str, tf_table_name: str, from_table):
    """Look up frequency values for elements in an array column from a term frequency
    table.

    Args:
        input_array_col_name: Name of array column to analyze
        tf_table_name: Name of table containing term frequencies to join against
        from_table: Source table containing the array column

    Returns:
        Table with array of structs containing original values and their frequencies

    Example:
        Given cleanedsource table:
        | id | names_arr          |
        |----|-------------------|
        | 1  | [john, mary]      |
        | 2  | [bob, mary]       |

        And cleaned tf_table:
        | value | rel_freq |
        |-------|----------|
        | john  | 0.400    |
        | mary  | 0.400    |
        | bob   | 0.200    |

        result = lookup_term_frequencies("names_arr", "tf_table", source)

        Result:
        | id | names_arr_with_freq                                              |
        |----|---------------------------------------------------------------|
        | 1  | [{value: john, rel_freq: 0.4}, {value: mary, rel_freq: 0.4}]  |
        | 2  | [{value: bob, rel_freq: 0.2}, {value: mary, rel_freq: 0.4}]   |
    """
    exploded_name = f"exploded_{input_array_col_name}"
    joined_name = f"joined_{input_array_col_name}"
    agg_table_name = f"agg_table_{input_array_col_name}"

    t_exploded = Table(
        exploded_name,
        "match_id",
        TransformedColumn(f"UNNEST({input_array_col_name})", alias="value"),
        from_table=from_table,
    )

    t_joined = Table(
        joined_name,
        f"{t_exploded}.match_id",
        f"{t_exploded}.value",
        TransformedColumn("COALESCE(tf.rel_freq, NULL)", alias="rel_freq"),
        from_table=t_exploded,
        post_from_clauses=(f"LEFT JOIN {tf_table_name} tf ON {t_exploded}.value = tf.value"),
    )

    t_aggregated = Table(
        agg_table_name,
        "match_id",
        TransformedColumn(
            "array_agg(struct_pack(value := value, rel_freq := rel_freq))",
            alias=f"{input_array_col_name}_with_freq",
        ),
        from_table=t_joined,
        post_from_clauses="GROUP BY match_id",
    )

    return t_aggregated
