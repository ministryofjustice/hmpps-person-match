from datetime import datetime  # noqa: INP001

import duckdb
from IPython.display import display
from splink import DuckDBAPI

from hmpps_cpr_splink.cpr_splink.data_cleaning.table import Table
from hmpps_cpr_splink.cpr_splink.model import ModelTraining
from hmpps_cpr_splink.cpr_splink.model_cleaning.term_frequencies import (
    lookup_term_frequencies,
)

con = duckdb.connect()

sql = """
create or replace table personmatch_person as
select *
from read_parquet('hmpps_cpr_splink/secret_data/personmatch/raw/person/*.parquet')

"""
con.execute(sql)

con.table("personmatch_person").show(max_width=10000)


# Note that the cleaned data doesn't have postcode frequencies so we need to
# derive this as part of training
pc_distinct_unnested = Table(
    "pc_unnested",
    "UNNEST(postcode_arr) AS value",
    from_table="personmatch_person",
)


pc_frequencies = Table(
    "tf_postcodes",
    "value",
    "COUNT(*) as freq",
    "COUNT(*) * 1.0 / (SELECT COUNT(*) FROM pc_unnested) as rel_freq",
    from_table=pc_distinct_unnested,
    post_from_clauses="GROUP BY value",
)


con.execute(pc_frequencies.create_table_sql)


t_postcode_with_freq = lookup_term_frequencies(
    "postcode_arr",
    "tf_postcodes",
    "personmatch_person",
)


t_postcode_final = Table(
    "cleaned_with_postcode_tfs",
    "personmatch_person.*",
    "agg_table_postcode_arr.postcode_arr_with_freq",
    from_table=t_postcode_with_freq,
    post_from_clauses=(
        f"RIGHT JOIN personmatch_person ON personmatch_person.match_id = {t_postcode_with_freq}.match_id"
    ),
)


con.execute(t_postcode_final.create_table_sql)
con.table("cleaned_with_postcode_tfs").show(max_width=10000)

assert (  # noqa: S101
    con.table("cleaned_with_postcode_tfs").count("*").fetchone()[0]
    == con.table("personmatch_person").count("*").fetchone()[0]
), "The number of rows in the cleaned table should match the original table"

db_api = DuckDBAPI(con)


train_u_size = 1e9
date_str = datetime.now().strftime("%Y_%m_%d")  # noqa: DTZ005
train_u_size_str = f"{train_u_size:.0e}".replace("+", "")
model_name = f"model_{date_str}_{train_u_size_str}.json"

linker = ModelTraining(train_u_size=train_u_size).train("cleaned_with_postcode_tfs", db_api)

display(linker.visualisations.match_weights_chart())

linker.misc.save_model_to_json("hmpps_cpr_splink/cpr_splink/model/model_files/" + model_name, overwrite=True)
