import duckdb


def create_table_from_records(
    con: duckdb.DuckDBPyConnection,
    records: list,
    table_name: str,
    schema: list[tuple[str, str]],
):
    """Creates a DuckDB table from a list of records using the provided schema.

    Args:
        con: DuckDB connection
        records: List of dataclass/TypedDict instances
        table_name: Name of the table to create
        schema: List of (column_name, duckdb_type) tuples
    """
    # See https://github.com/RobinL/benchmark_duckdb_row_creation
    # for benchmarking/performance testing of different approaches
    columns_sql = ",\n".join(f"{col} {type_}" for col, type_ in schema)

    create_sql = f"CREATE TABLE\n{table_name} ({columns_sql})"

    con.execute(create_sql)

    def extract_record_values(record, schema):
        return [record[col] for col, _ in schema]

    values = [extract_record_values(record, schema) for record in records]

    placeholders = ",".join("?" for _ in schema)
    # table name is fixed by us, and this is executed in duckdb
    insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"  # noqa: S608
    con.executemany(insert_sql, values)
    return table_name
