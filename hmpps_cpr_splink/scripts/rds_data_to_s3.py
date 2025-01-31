# ruff: noqa: T201
# This script downloads all the data from the RDS database and uploads it to S3
# Run using `avd python rds_data_to_s3.py`

import os
import shutil

import boto3
import duckdb
import pandas as pd

# fmt: off
# The problem here is that the secret_data folder is present locally but not in github
# which messes up auto sorting of imports, causing ruff to fail on github actions
from secret_data.secrets_file import (  # noqa: I001
    postgres_connection_string_prod as postgres_connection_string,
)

# fmt: on

MAIN_DIR = "../secret_data"

OUTPUT_BUCKET = "alpha-data-linking"
OUTPUT_PREFIX = "core_person_record/v4/data/"

DELETE_FROM_LOCAL_AFTER_S3_UPLOAD = True

pd.options.display.max_colwidth = 1000


def delete_all_objects_at_path(bucket_name, prefix):
    s3 = boto3.client("s3")
    total_objects = 0

    # First, count total objects
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if "Contents" in page:
            total_objects += len(page["Contents"])

        if total_objects > 500:
            raise ValueError(
                f"Found more than 1000 objects at {bucket_name}/{prefix}. "
                "Aborting deletion for safety."
            )

    if total_objects == 0:
        print("No objects found at the specified path")
        return

    # Now proceed with deletion since we know it's safe
    total_deleted = 0
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    while True:
        if "Contents" not in response:
            break

        keys_to_delete = [{"Key": obj["Key"]} for obj in response["Contents"]]
        s3.delete_objects(Bucket=bucket_name, Delete={"Objects": keys_to_delete})
        total_deleted += len(keys_to_delete)

        if not response.get("IsTruncated"):
            break

        response = s3.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix,
            ContinuationToken=response["NextContinuationToken"],
        )

    print(f"Deleted {total_deleted} objects from {bucket_name}/{prefix}")

MAX_DIR_DEPTH = 3
MAX_TOTAL_DIRS = 20

def is_directory_structure_safe_to_delete(directory):
    abs_dir = os.path.abspath(directory)
    abs_main_dir = os.path.abspath(MAIN_DIR)

    if not abs_dir.startswith(abs_main_dir):
        return False, (
            f"The directory {abs_dir} is not within the MAIN_DIR {abs_main_dir}"
        )

    total_dirs = 0
    max_depth_reached = 0

    for root, dirs, _ in os.walk(abs_dir):
        depth = os.path.relpath(root, abs_dir).count(os.sep)
        max_depth_reached = max(max_depth_reached, depth)

        if max_depth_reached > MAX_DIR_DEPTH:
            return False, (
                f"Directory depth {max_depth_reached} exceeds the maximum "
                f"allowed depth of {MAX_DIR_DEPTH}"
            )

        total_dirs += len(dirs)
        if total_dirs > MAX_TOTAL_DIRS:
            return False, (
                f"Total number of subdirectories ({total_dirs}) exceeds the maximum "
                f"allowed of {MAX_TOTAL_DIRS}"
            )

    return True, "Directory structure is safe to delete"


def clean_parquet_files(directory):
    abs_directory = os.path.abspath(directory)

    is_safe, reason = is_directory_structure_safe_to_delete(abs_directory)
    if not is_safe:
        raise ValueError(
            f"Refusing to delete directory with unexpected structure: "
            f"{abs_directory}. Reason: {reason}"
        )

    if os.path.exists(abs_directory):
        shutil.rmtree(abs_directory)
    os.makedirs(abs_directory)


con = duckdb.connect()
con.execute(
    f"ATTACH '{postgres_connection_string}' AS postgres_db (TYPE POSTGRES, READ_ONLY)"
)
con.execute("SET pg_debug_show_queries=false")


if not os.path.exists(MAIN_DIR):
    os.makedirs(MAIN_DIR)

tables_df = con.execute("SHOW ALL TABLES;").df()

page_size = 250_000

sql = """
SELECT * FROM
postgres_query('postgres_db',
'SELECT
    source_system,
    count(*) as count
FROM
    personrecordservice.person
GROUP BY
    source_system
')
"""

con.sql(sql).show(max_width=1000)

SCHEMA_NAME = "personrecordservice"


TABLES = [
    "address",
    "contact",
    "person",
    "pseudonym",
    "sentence_info",
    "reference",
]

for table in TABLES:
    table_dir = os.path.join(MAIN_DIR, "raw", table)
    if not os.path.exists(table_dir):
        os.makedirs(table_dir)
    clean_parquet_files(table_dir)

    total_records_query = f"SELECT COUNT(*) FROM postgres_db.{SCHEMA_NAME}.{table}"
    total_records = con.execute(total_records_query).fetchone()[0]

    for offset in range(0, total_records, page_size):
        sql = f"""
        COPY (
            WITH my_table AS (
                SELECT *
                FROM postgres_query('postgres_db',
                    'SELECT *
                     FROM {SCHEMA_NAME}.{table}
                     ORDER BY 1
                     LIMIT {page_size} OFFSET {offset}')
            )
            SELECT * FROM my_table
        )
        TO '{table_dir}/{table}_{offset // page_size + 1}.parquet'
        (FORMAT 'parquet');
        """

        con.execute(sql)
        print(
            f"Exported {table} - chunk {(offset // page_size) + 1} "
            f"to {table_dir}/{table}_{offset // page_size + 1}.parquet"
        )


def is_s3_path_safe_to_delete(bucket_name, prefix):
    s3 = boto3.client("s3")
    objects_to_check = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    if "Contents" not in objects_to_check:
        return True, "Path is empty or does not exist in S3"

    total_dirs = 0
    max_depth_reached = 0

    for obj in objects_to_check["Contents"]:
        relative_path = obj["Key"].replace(prefix, "").strip("/")
        depth = relative_path.count("/")
        max_depth_reached = max(max_depth_reached, depth)

        if max_depth_reached > 3:
            return False, (
                f"Directory depth {max_depth_reached} exceeds the maximum "
                "allowed depth of 3"
            )
        if relative_path.endswith("/"):
            total_dirs += 1
        if total_dirs > 20:
            return False, (
                f"Total number of subdirectories ({total_dirs}) exceeds the maximum "
                "allowed of 20"
            )

    return True, "S3 path structure is safe to delete"


def upload_to_s3(local_path, bucket_name, prefix):
    safe, reason = is_s3_path_safe_to_delete(bucket_name, prefix)
    if not safe:
        raise ValueError(
            f"Refusing to delete S3 path with unexpected structure: "
            f"{bucket_name}/{prefix}. Reason: {reason}"
        )

    # Delete existing data
    delete_all_objects_at_path(bucket_name, prefix)

    # Use aws CLI sync command to upload data
    sync_command = f"aws s3 sync {local_path} s3://{bucket_name}/{prefix}"
    os.system(sync_command)

    if DELETE_FROM_LOCAL_AFTER_S3_UPLOAD:
        shutil.rmtree(local_path)
        print(f"Cleaned up local directory: {local_path}")


LOCAL_RAW_PATH = os.path.join(MAIN_DIR, "raw")

upload_to_s3(LOCAL_RAW_PATH, OUTPUT_BUCKET, f"{OUTPUT_PREFIX}raw/")
