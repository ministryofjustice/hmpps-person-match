import time

import duckdb
from sqlalchemy import URL

con = duckdb.connect()

database_url = URL.create(
    drivername="postgresql",
    username="root",
    password="dev",  # noqa: S106
    host="localhost",
    port="5432",
    database="postgres",
)

con.execute(f"ATTACH '{database_url.render_as_string(hide_password=False)}' AS pg_db (TYPE POSTGRES);")

con.execute("TRUNCATE TABLE pg_db.personmatch.person")

t1 = time.time()
# can't parse dates as-is, so do some fiddling
sql = "CREATE TABLE temp_data AS SELECT * FROM read_csv('dev/data/people.csv', auto_detect=true);"
con.execute(sql)

t2 = time.time()

con.table("temp_data").show(max_width=100000)

print(f"Read csv: {t2 - t1}")

columns = {
    "id",
    "match_id",
    "name_1_std",
    "name_2_std",
    "name_3_std",
    "last_name_std",
    "first_and_last_name_std",
    "forename_std_arr",
    "last_name_std_arr",
    "sentence_date_arr",
    "date_of_birth",
    "date_of_birth_arr",
    "postcode_arr",
    "postcode_outcode_arr",
    "cro_single",
    "pnc_single",
    "source_system",
    "source_system_id",
    # derived columns
    "sentence_date_first",
    "sentence_date_last",
    "postcode_first",
    "postcode_second",
    "postcode_last",
    "postcode_outcode_first",
    "postcode_outcode_last",
    "date_of_birth_last",
    "forename_first",
    "forename_last",
    "last_name_first",
    "last_name_last",
}
col_string = ", ".join(columns)

select_columns = []
array_columns = {
    "forename_std_arr",
    "last_name_std_arr",
    "sentence_date_arr",
    "date_of_birth_arr",
    "postcode_arr",
    "postcode_outcode_arr",
}

for col in columns:
    if col in array_columns:
        # For array columns, add a cast to VARCHAR[]
        # This tells DuckDB's postgres extension to format it correctly for Postgres
        select_columns.append(f"{col}::VARCHAR[]")
    else:
        select_columns.append(col)

select_col_string = ", ".join(select_columns)

con.execute(
    f"""
    INSERT INTO pg_db.personmatch.person
        ({col_string})
    SELECT
        {select_col_string}
    FROM temp_data;
""",
)
# --- END OF CHANGE ---
t3 = time.time()

print(f"Inserted in: {t3 - t2}")
