# ruff: noqa: T201
# Simple script to illustrate how to query the RDS database directly

import time

import psycopg2

# Change to prod if you want to query the production database
from secret_data.secrets_file import (
    postgres_connection_string_preprod as postgres_connection_string,
)


def execute_postgres_query_direct(sql):
    start_time = time.time()

    with psycopg2.connect(postgres_connection_string) as conn, conn.cursor() as cur:
        cur.execute(sql)
        result = cur.fetchall()

    end_time = time.time()
    # 0.1330 seconds is length of time for most trivial query
    execution_time = end_time - start_time - 0.1330
    print(f"Query execution time: {execution_time:.4f} seconds")

    return result


sql = """
select *
from personrecordservice.person
limit 1
"""
execute_postgres_query_direct(sql)

# Get a list of the tables in the personrecordservice schema
sql = """
select table_name
from information_schema.tables
where table_schema = 'personrecordservice'
"""
execute_postgres_query_direct(sql)

sql = """
select count(*)/1000000
from personrecordservice.sentence_info
limit 1
"""
execute_postgres_query_direct(sql)
