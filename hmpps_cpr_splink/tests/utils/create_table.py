from collections import OrderedDict

import duckdb


# TODO: more robust
def value_to_sql_literal(value: str | list[str] | None) -> str:
    """
    Takes any literal and translates it into a literal suitable
    for use in a SQL expression
    """
    if isinstance(value, str):
        escaped_value = value.replace("'", "''")
        return f"'{escaped_value}'"
    if value is None:
        return "NULL"
    if isinstance(value, list):
        return "[" + ", ".join(map(value_to_sql_literal, value)) + "]"
    return str(value)


def load_frame(
    con: duckdb.DuckDBPyConnection,
    rows: list[dict[str, str]],
    table_schema: dict[str, str],
    table_name: str,
) -> None:
    """
    Create (or replace) a table `table_name` with schema `table_schema`.

    Insert into it values as listed in `rows`, in record format (i.e. a list of dicts).
    """

    # TODO: restore ??
    # get the names of the columns
    column_metadata = OrderedDict(
        {
            column_name: {
                "type": column_type,
                "required": True,
            }
            for column_name, column_type in table_schema.items()
        },
    )
    # insert values from test data - any columns appearing
    # that we don't need are just None
    # let someone else handle converting to NULL
    values_to_insert = [
        [row[column_name] if column_data["required"] else None for column_name, column_data in column_metadata.items()]
        for row in rows
    ]
    column_spec_str = ", ".join(
        f"{column_name} {column_data['type']}" for column_name, column_data in column_metadata.items()
    )

    values_str = (
        "("
        + "), (".join(
            [", ".join(value_to_sql_literal(value) for value in row_values) for row_values in values_to_insert],
        )
        + ")"
    )
    sql_create_table = f"CREATE OR REPLACE TABLE {table_name} ({column_spec_str})"

    con.sql(sql_create_table)

    # It's valid to have some zero-row input tables, eg to check left join vs inner join
    if len(rows) > 0:
        sql_insert_values = f"INSERT INTO {table_name} VALUES {values_str}"
        con.sql(sql_insert_values)
