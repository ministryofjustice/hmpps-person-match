# ruff: noqa: T201
import duckdb
from splink import DuckDBAPI, Linker, SettingsCreator, block_on
from splink.blocking_rule_library import CustomRule
from splink.internals.blocking_analysis import (
    cumulative_comparisons_to_be_scored_from_blocking_rules_data,
    n_largest_blocks,
)

from scripts.qa.blocking_performance import blocking_performance_chart

con_training = duckdb.connect()

sql = """
create or replace table df_training as
select *
from read_parquet('secret_data/cleaned/cleaned/*.parquet')
-- where substr(first_and_last_name_std,1,1) = 'A'
-- OR substr(last_name_std,1,1) = 'A'
"""

con_training.execute(sql)

con_training.table("df_training").show(max_width=1000)


db_api = DuckDBAPI(con_training)


model_name = "model_2025_01_13_5e08.json"


new_settings = SettingsCreator.from_path_or_dict(model_name)
new_settings.blocking_rules_to_generate_predictions = [
    block_on("pnc_single"),
    block_on("cro_single"),
    block_on("date_of_birth", "postcode_arr[1]"),
    block_on("date_of_birth", "postcode_outcode_arr[1]", "substr(name_1_std, 1, 2)"),
    block_on(
        "date_of_birth_arr[-1]",
        "postcode_outcode_arr[-1]",
        "substr(last_name_std, 1, 2)",
    ),
    block_on("forename_std_arr[1]", "last_name_std_arr[1]", "postcode_arr[1]"),
    block_on("date_of_birth", "postcode_arr[-1]"),
    CustomRule(
        "l.date_of_birth = r.date_of_birth and l.postcode_arr[1] = r.postcode_arr[2]"
    ),
    block_on("sentence_date_arr[1]", "date_of_birth"),
    block_on("forename_std_arr[-1]", "last_name_std_arr[-1]", "date_of_birth"),
    block_on("forename_std_arr[1]", "last_name_std_arr[-1]", "date_of_birth"),
    block_on("first_and_last_name_std", "name_2_std"),
    block_on(
        "substr(name_1_std, 1, 2)", "substr(last_name_std, 1, 2)", "date_of_birth"
    ),
    block_on(
        "substr(name_1_std, 1, 2)", "substr(last_name_std, 1, 2)", "postcode_arr[1]"
    ),
    block_on(
        "substr(name_1_std, 1, 2)", "substr(last_name_std, 1, 2)", "postcode_arr[-1]"
    ),
    block_on(
        "substr(name_1_std, 1, 2)",
        "substr(last_name_std, 1, 2)",
        "sentence_date_arr[-1]",
    ),
    CustomRule(
        (
            "l.name_1_std = r.last_name_std and l.last_name_std = r.name_1_std "
            "and l.date_of_birth = r.date_of_birth"
        )
    ),
]


linker = Linker("df_training", new_settings, db_api)

for br in new_settings.blocking_rules_to_generate_predictions:
    c = n_largest_blocks(
        table_or_tables="df_training",
        blocking_rule=br,
        db_api=db_api,
        link_type=linker._settings_obj._link_type,
        n_largest=2,
    )
    print("--")
    print(f"Blocking rule: {br}")

    c.as_duckdbpyrelation().show(max_width=1000)

# Comparisons per blocking rule
counts = cumulative_comparisons_to_be_scored_from_blocking_rules_data(
    table_or_tables="df_training",
    blocking_rules=new_settings.blocking_rules_to_generate_predictions,
    db_api=db_api,
    link_type=linker._settings_obj._link_type,
    unique_id_column_name=linker._settings_obj.column_info_settings.unique_id_column_name,
    source_dataset_column_name=linker._settings_obj.column_info_settings.source_dataset_column_name,
)
total_comparisons = counts["row_count"].sum()
print(f"Total comparisons: {total_comparisons:,}")
# linker.misc.save_model_to_json(model_name, overwrite=True)
df_predict = linker.inference.predict(threshold_match_weight=20.0)
c = df_predict.as_duckdbpyrelation().count("*").fetchone()[0]
print(f"Number of predictions: {c:,}")


clustered = linker.clustering.cluster_pairwise_predictions_at_threshold(
    df_predict, threshold_match_weight=20.0
)
clustered.as_duckdbpyrelation().show(max_width=1000)
cluster_ddb = clustered.as_duckdbpyrelation()
cluster_ddb.show(max_width=1000)

sql = """
select count(*) as c, count (distinct cluster_id) as num_clusters
from cluster_ddb
"""
summary_clusters = con_training.sql(sql)
summary_clusters.show(max_width=1000)
num_input_records, distinct_cluster_count = summary_clusters.fetchone()


# Format the summary message
print("\nSummary:")

print(
    f"{c:,} predictions, resulting in {distinct_cluster_count:,} "
    f"distinct clusters from {num_input_records:,} input records"
)
print(f"Total comparisons: {total_comparisons:,}")

blocking_performance_chart(linker, df_predict)
