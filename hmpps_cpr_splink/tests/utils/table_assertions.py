from typing import Any, Callable

import duckdb
from pytest import mark
from yaml import safe_load

from .create_table import load_frame

_TEST_DATA_DIR = "tests/data/"


def cols_from_rows(rows: list[dict[str, Any]]) -> list[str]:
    """
    List of column names, from a list of records in key: value format.
    """
    return list({k for r in rows for k in r.keys()})


def assert_tables_equal(
    con: duckdb.DuckDBPyConnection,
    expected_relation: str,
    actual_relation: str,
    cols_to_check: list[str],
) -> None:
    """
    Compares two tables, `expected_relation` and `actual_relation`.

    Checks that the set of values in the columns listed in `cols_to_check`
    are the same in both relations (order ignored).

    If they differ, mismatching columns will be displayed, clearly showing
    which values were expected but not present, and which were present
    but not expected.
    An `AssertionError` is also raised.
    """
    # TODO: restore in some form?
    # want consistent ordering of array columns so we can compare directly
    # TODO: sometimes order matters. So handle
    # cols_to_check = [
    #     column_name
    #     # if not is_list_type(OUTPUT_SCHEMA.get(column_name, ""))
    #     # else order_array_column(column_name)
    #     for column_name in cols_to_check
    # ]

    col_string = ", ".join(cols_to_check)
    sql_set_diff = "SELECT {cols} FROM {t1} EXCEPT ALL SELECT {cols} FROM {t2}"
    sql_expected_except_actual = sql_set_diff.format(
        cols=col_string, t1=expected_relation, t2=actual_relation
    )
    sql_actual_except_expected = sql_set_diff.format(
        cols=col_string, t1=actual_relation, t2=expected_relation
    )
    sql = f"""
    WITH expected_not_found AS ({sql_expected_except_actual}),
         found_not_expected AS ({sql_actual_except_expected})
    SELECT
        *, 'expected but not found' AS _test_row_status,
    FROM expected_not_found
    UNION ALL
    SELECT
        *, 'found but not expected' AS _test_row_status,
    FROM found_not_expected
    """
    full_sql = sql.format(cols=col_string, t1=expected_relation, t2=actual_relation)

    res = con.sql(full_sql)

    result_rows = res.fetchall()
    try:
        assert len(result_rows) == 0
    except AssertionError as e:
        # show the full diff when it's a schema difference not a data difference
        if actual_relation == "schema_compare_actual":
            res.show()
            raise e

        # more focussed view - only mismatched columns
        # This only works when the expected and actual tables have a single row
        # and so is only suitable for the data check, not the schema check
        col_check_template = (
            "CASE WHEN e.{column_name} IS DISTINCT FROM a.{column_name} "
            "THEN '{column_name}' "
            "ELSE NULL END AS {column_name}"
        )
        col_check_sql = ",\n        ".join(
            col_check_template.format(column_name=column_name)
            for column_name in cols_to_check
        )

        sql = f"""
        WITH mismatched_columns AS (
            SELECT
            {col_check_sql}
            FROM
                {actual_relation} a
            FULL JOIN
                {expected_relation} e
            ON 1=1
        )
        SELECT
            list_distinct(ARRAY[{col_string}]) AS differing_columns_array
        FROM
            mismatched_columns
        """

        mismatched_columns = con.sql(sql).fetchall()[0][0]
        sql = (
            f"SELECT {', '.join(mismatched_columns)}, "
            f"_test_row_status from ({full_sql})"
        )
        con.sql(sql).show(max_width=1000)

        raise e


def check_output_matches_expected(
    test_data: dict[str, list[dict[str, Any]]],
    con: duckdb.DuckDBPyConnection,
    sql: str,
    schemas: dict[str, dict[str, str]],
    expected_output_table: str = "expected_output_table",
):
    """
    Creates tables as specified in schemas, and loads in the corresponding data,
    as specified in test_data.

    The query 'sql' is run, and the result compared to the data loaded into the table
    specified by 'expected_output_table'.
    Types are also compared separately.

    If either are found to be different to expected, clear output of
    the difference is displayed,
    and an `AssertionError` is raised.
    """
    # extract the test data, create tables for them, and load the data in:
    for table_name, schema in schemas.items():
        # if schema is a string instead of dict, then it should be a yaml file name
        # containing the schema
        if isinstance(schema, str):
            schema = load_yaml_file(f"tables/{schema}")
            schemas[table_name] = schema
        try:
            rows = test_data[table_name]
        except KeyError as e:
            print(f"Didn't find any test data for specified table: '{table_name}'")  # noqa: T201
            raise e
        load_frame(con, rows, schema, table_name)

    # run the actual SQL we are testing:
    con.sql(f"CREATE OR REPLACE VIEW actual_output_table AS {sql}")

    # # TODO: we are already calculating this, so streamline
    cols_to_check = cols_from_rows(test_data[expected_output_table])

    assert_tables_equal(
        con, expected_output_table, "actual_output_table", cols_to_check
    )

    # now check the types are correct
    expected_output_schema = schemas[expected_output_table]

    # create a table in suitable format with the types we expect
    columns_table_schema = {"column_name": "VARCHAR", "data_type": "VARCHAR"}
    colname_data = [
        {"column_name": col_name, "data_type": col_type}
        for col_name, col_type in expected_output_schema.items()
    ]
    load_frame(con, colname_data, columns_table_schema, "schema_compare_expected")
    # compare this to the relevant subset of information_schema.columns
    # containing data on the types of our output table
    con.sql(
        "CREATE OR REPLACE VIEW schema_compare_actual AS "
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name='actual_output_table'"
    )
    assert_tables_equal(
        con,
        "schema_compare_expected",
        "schema_compare_actual",
        columns_table_schema.keys(),
    )
    return


def load_yaml_file(file_name: str):
    with open(f"{_TEST_DATA_DIR}/{file_name}", "r") as f:
        data = safe_load(f)
    return data


def check_data(
    data_file: str, sql: str, expected_output_table: str = "expected_output_table"
) -> Callable[[None], None]:
    """
    Decorator for a test function (whose contents are irrelevant).

    Specify some table data, and some SQL to run.
    After the SQL is run, we compare `expected_output_table`
    (as specified in the data_file) to the real table resulting from running the SQL.
    Compares values and types separately,
    with clear display of the difference on failure.

    Examples:
        ```py
        @check_data(
            "test_method_chaining_list_transform_remove_spaces.yaml",
            sql_list_transform_remove_spaces,
        )
        def test_list_transform_remove_all_spaces(): ...
        ```
    Args:
        data_file (str): Filename of yaml file in tests/data containing test data.
            Either specified as input/output values for single-column transforms,
            or can be multi-row/table for more complex queries
        sql (str): SQL query to be run. A relation will be made of query result,
            which is then compared (types and values) to an expected table
        expected_output_table (str, optional): In the data_file, which of the specified
            tables is the one to compare the query output against?
            Only relevant for the fuller data format - for single column transforms
            the default value will take care of things.

    Returns:
        Callable: The result of this function is a decorator to apply to test functions
    """
    con = duckdb.connect()

    data = load_yaml_file(data_file)

    # if we are using a shorthand format, convert it to long format
    if data.get("format", "long") == "short":
        # convert schema format
        data["schema"]["input_table"] = {
            "input_column": data["schema"]["input_column_type"]
        }
        data["schema"]["expected_output_table"] = {
            "output_column": data["schema"]["output_column_type"]
        }
        del data["schema"]["input_column_type"]
        del data["schema"]["output_column_type"]
        # and then the test data itself
        long_rows = []
        for input_output_row in data["data"]:
            input_value = input_output_row["input_value"]
            output_value = input_output_row["output_value"]
            long_rows.append(
                {
                    "input_table": [{"input_column": input_value}],
                    "expected_output_table": [{"output_column": output_value}],
                }
            )
        data["data"] = long_rows

    schemas: dict[str, dict[str, str]] = data["schema"]

    test_data: list[dict[str, list[dict[str, Any]]]] = data["data"]

    # wrapper runs check_output_matches_expected for each dataset in test_data
    # run each as a separate test instance using pytest.mark.parameterize
    def decorator(test_function):
        def check_function_wrapper(test_data_set):
            return check_output_matches_expected(
                test_data_set,
                con,
                sql,
                schemas=schemas,
                expected_output_table=expected_output_table,
            )

        return mark.parametrize("test_data_set", test_data)(check_function_wrapper)

    return decorator
