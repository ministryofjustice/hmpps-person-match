# noqa: INP001
import os
from datetime import datetime

import duckdb
from splink import DuckDBAPI, Linker

from hmpps_cpr_splink.cpr_splink.data_cleaning.join_raw_tables import join_raw_tables_sql
from hmpps_cpr_splink.cpr_splink.model_cleaning import clean_whole_joined_table
from hmpps_cpr_splink.cpr_splink.model_cleaning.tables import create_postcode_tf_from_cpr_joined

joined_data_exists = False
test_con = duckdb.connect()


try:
    test_con.execute("SELECT * FROM read_parquet('secret_data/cleaned/joined/*.parquet') LIMIT 1")
    joined_data_exists = True
except duckdb.IOException:
    joined_data_exists = False
finally:
    test_con.close()

if not joined_data_exists:
    con_join = duckdb.connect()
    sql = join_raw_tables_sql(
        dict(
            person_in="read_parquet('secret_data/raw/person/*.parquet')",
            pseudonym_in="read_parquet('secret_data/raw/pseudonym/*.parquet')",
            address_in="read_parquet('secret_data/raw/address/*.parquet')",
            reference_in="read_parquet('secret_data/raw/reference/*.parquet')",
            sentence_info_in="read_parquet('secret_data/raw/sentence_info/*.parquet')",
        ),
    )

    df = con_join.sql(sql)
    os.makedirs("secret_data/cleaned/joined/", exist_ok=True)

    con_join.execute(
        """
        COPY df
        TO './secret_data/cleaned/joined/'
        (FORMAT PARQUET, PER_THREAD_OUTPUT, ROW_GROUP_SIZE 100000);
    """,
    )
    con_join.close()
else:
    print("Joined data files already exist, skipping processing")  # NOQA: T201

cleaned_data_exists = False
test_con = duckdb.connect()

try:
    test_con.execute("SELECT * FROM read_parquet('secret_data/cleaned/cleaned/*.parquet') LIMIT 1")
    cleaned_data_exists = True
except duckdb.IOException:
    cleaned_data_exists = False
finally:
    test_con.close()

if not cleaned_data_exists:
    con_clean = duckdb.connect()

    sql = """
    create or replace table df_joined as
    select *
    from read_parquet('secret_data/cleaned/joined/*.parquet')
    """
    con_clean.execute(sql)

    sql = create_postcode_tf_from_cpr_joined("df_joined").create_table_sql
    con_clean.sql(sql)

    sql = clean_whole_joined_table("df_joined", "tf_postcodes").create_table_sql
    con_clean.sql(sql)

    os.makedirs("secret_data/cleaned/cleaned/", exist_ok=True)

    con_clean.execute(
        """
        COPY df_cleaned_with_arr_freq
        TO './secret_data/cleaned/cleaned/'
        (FORMAT PARQUET, PER_THREAD_OUTPUT, ROW_GROUP_SIZE 100000);
    """,
    )
    con_clean.close()
else:
    print("Cleaned data files already exist, skipping processing")  # NOQA: T201


con_training = duckdb.connect()

sql = """
create or replace table df_training as
select *
from read_parquet('secret_data/cleaned/cleaned/*.parquet')

"""

con_training.execute(sql)


db_api = DuckDBAPI(con_training)


train_u_size = 1e9
date_str = datetime.now().astimezone().strftime("%Y_%m_%d")
train_u_size_str = f"{train_u_size:.0e}".replace("+", "")
model_name = f"model_{date_str}_{train_u_size_str}.json"

linker = Linker("df_training", "model_2025_01_13_5e08.json", db_api)

df_predict = linker.inference.predict(threshold_match_weight=20.0)

con_training.execute(
    f"""
    COPY {df_predict.physical_name}
    TO './secret_data/cleaned/predictions/'
    (FORMAT PARQUET, PER_THREAD_OUTPUT, ROW_GROUP_SIZE 100000);
""",
)


linker.visualisations.match_weights_histogram(df_predict)
linker.visualisations.comparison_viewer_dashboard(df_predict, "secret_data/cleaned/comparison_viewer/cv.html")

clustered = linker.clustering.cluster_pairwise_predictions_at_threshold(df_predict, threshold_match_weight=20.0)
clustered.as_duckdbpyrelation().show(max_width=1000)
cluster_ddb = clustered.as_duckdbpyrelation()


con_training.execute(
    f"""
    COPY {clustered.physical_name}
    TO './secret_data/cleaned/clustered/'
    (FORMAT PARQUET, PER_THREAD_OUTPUT, ROW_GROUP_SIZE 100000);
""",
)
