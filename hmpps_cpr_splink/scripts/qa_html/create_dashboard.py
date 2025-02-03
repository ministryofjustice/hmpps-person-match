# ruff: noqa: T201
import json
import os
import subprocess

import duckdb
from jinja2 import Template
from splink import DuckDBAPI, Linker
from splink.internals.comparison_vector_distribution import (
    comparison_vector_distribution_sql,
)
from splink.internals.pipeline import CTEPipeline
from splink.internals.splink_comparison_viewer import comparison_viewer_table_sqls
from splink.internals.waterfall_chart import record_to_waterfall_data

# Get relevant comparisons
con = duckdb.connect()


path = "secret_data/cleaned/predictions"
# path = "secret_data/cleaned/fake_predictions"

sql = f"""
create or replace table df_predictions as
select *
from read_parquet('{path}/*.parquet')
where
-- Filter out matches on an ID, full name and DOB.  No point in manually reviewing these
-- since they're all definite matches.
not (
    (gamma_cro_single = 2  or gamma_pnc_single = 2)
    and gamma_name_comparison = 7 and gamma_date_of_birth_arr = 4
)

"""
con.execute(sql)
con.table("df_predictions").show(max_width=10000)
c = con.table("df_predictions").count("*").fetchall()[0][0]
print(f"Number of records: {c:,.0f}")

# This is just needed because to create a linker we need to pass it some data,
# i.e. this data is really only being used for its schema
path = "secret_data/cleaned/cleaned"
sql = f"""
create or replace table df_training as
select *
from read_parquet('{path}/*.parquet')
limit 10
"""
con.execute(sql)


db_api = DuckDBAPI(con)
linker = Linker("df_training", "model_2025_01_13_5e08.json", db_api)
df_pred = linker.table_management.register_table_predict("df_predictions")

pipeline = CTEPipeline([df_pred])
sql = comparison_vector_distribution_sql(linker)
pipeline.enqueue_sql(sql, "__splink__df_comparison_vector_distribution")

df_gamma_dist = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)
df_gamma_dist.as_duckdbpyrelation().show(max_width=10000)

pipeline = CTEPipeline([df_pred, df_gamma_dist])
sqls = comparison_viewer_table_sqls(linker, example_rows_per_category=5)
pipeline.enqueue_list_of_sqls(sqls)


df_examples = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)
df_examples.as_duckdbpyrelation().sort("count_rows_in_comparison_vector_group").show(max_width=10000)

# Munge data into the right format for the labelling tool and render

# Now we want to output (say) 5 example rows for each gamma pattern
# Want to filter out any gamma patterns that have fewer than n examples
min_examples = 50
num_examples = 3


sql_filtered_examples = f"""
select gam_concat, * exclude (gam_concat)
from {df_examples.physical_name}
where count_rows_in_comparison_vector_group >= {min_examples}
and row_example_index <= {num_examples}
order by random()
"""
records_to_waterfall = db_api._con.sql(sql_filtered_examples)
con.register("records_to_waterfall", records_to_waterfall)
records_to_waterfall.show(max_width=10000)
# Get schema info
schema_info = db_api._con.sql("DESCRIBE records_to_waterfall").df()

# Build column list for SQL query
columns = []
for row in schema_info.itertuples():
    col_name = row.column_name
    col_type = row.column_type

    # Handle date columns
    if col_type == "DATE":
        columns.append(f"strftime({col_name}, '%Y-%m-%d') as {col_name}")
    # Handle array of dates
    elif col_type == "DATE[]":
        columns.append(f"array_transform({col_name}, x -> strftime(x, '%Y-%m-%d')) as {col_name}")
    # Keep other columns as-is
    else:
        columns.append(col_name)

# Build and execute SQL
sql_format_dates = "select\n    " + ",\n    ".join(columns) + "\nfrom records_to_waterfall"
records_to_waterfall_formatted = db_api._con.sql(sql_format_dates)


def extract_comparison_columns(record):
    # Find all columns that end with _l
    left_cols = [
        col
        for col in record
        if col.endswith("_l")
        and not col.startswith("tf_")
        and not col.startswith("bf_")
        and not col.startswith("postcode_arr_with_")
        and not col.startswith("first_and_last_name_std")
    ]

    # Create structured data for the template
    comparisons = []
    for col in left_cols:
        base_name = col[:-2]  # Remove _l suffix
        right_col = f"{base_name}_r"

        if right_col in record:
            left_value = record[col]
            right_value = record[right_col]

            comparisons.append(
                {
                    "field": base_name,
                    "left_value": left_value,
                    "right_value": right_value,
                }
            )

    return comparisons


records_to_waterfall_formatted.show(max_width=10000)


data_as_dicts = json.loads(records_to_waterfall_formatted.df().to_json(orient="records"))

extract_comparison_columns(data_as_dicts[0])

example_waterfall = linker.visualisations.waterfall_chart([data_as_dicts[0]])

spec = example_waterfall.to_dict()

spec["datasets"] = {}
spec["data"] = {"name": "data-1"}
del spec["params"][0]["bind"]
del spec["transform"][0]


final_data = []
for record in data_as_dicts:
    output_record = {}

    # Process the row data using extract_comparison_columns
    output_record["for_table"] = extract_comparison_columns(record)

    # Get waterfall data as before
    as_waterfall = record_to_waterfall_data(record, linker._settings_obj, hide_details=False)

    # Truncate values in as_waterfall if longer than 40 characters
    for item in as_waterfall:
        if isinstance(item["value_l"], str) and len(item["value_l"]) > 40:
            item["value_l"] = item["value_l"][:40] + "..."
        if isinstance(item["value_r"], str) and len(item["value_r"]) > 40:
            item["value_r"] = item["value_r"][:40] + "..."

    output_record["as_waterfall"] = as_waterfall

    # Add additional metadata that might be useful
    # Add additional metadata that might be useful
    output_record["match_probability"] = record["match_probability"]
    output_record["match_weight"] = record["match_weight"]
    output_record["rec_comparison_id"] = record["rec_comparison_id"]

    output_record["gamma_concat"] = record["gam_concat"]
    output_record["count_rows_in_comparison_vector_group"] = record["count_rows_in_comparison_vector_group"]

    final_data.append(output_record)


def output_html(final_data, spec_to_interpolate_into_template, output_path):
    # Get the template path relative to this file
    template_path = os.path.join(os.path.dirname(__file__), "template.j2")

    # Read the template
    with open(template_path) as f:
        template = Template(f.read())

    final_data_json = json.dumps(final_data)

    spec_json = json.dumps(spec_to_interpolate_into_template)

    # Render the template with our data
    html_content = template.render(data_to_display=final_data_json, spec_json=spec_json)

    # Write the rendered HTML to the output file
    with open(output_path, "w") as f:
        f.write(html_content)


os.makedirs("secret_data/cleaned/qa_html", exist_ok=True)


BATCH_SIZE = 50


def get_git_commit_hash():
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"]).decode("utf-8").strip()
    except subprocess.CalledProcessError:
        return "unknown"


GIT_COMMIT_HASH = get_git_commit_hash()


def batch_and_output_html(final_data, spec, output_base_dir):
    os.makedirs(output_base_dir, exist_ok=True)

    # Split final_data into batches
    for batch_start in range(0, len(final_data), BATCH_SIZE):
        batch_end = batch_start + BATCH_SIZE
        batch = final_data[batch_start:batch_end]

        # Create filename for this batch
        batch_filename = f"labelling_records_{GIT_COMMIT_HASH}_{batch_start}_{batch_end}.html"

        # Add the filename to each record in the batch
        for record in batch:
            record["batch_filename"] = batch_filename

        # Create full output path
        output_path = os.path.join(output_base_dir, batch_filename)

        # Output the HTML for this batch
        output_html(batch, spec, output_path)

        print(f"Created {output_path} with {len(batch)} records")


batch_and_output_html(final_data, spec, "secret_data/cleaned/qa_html")
