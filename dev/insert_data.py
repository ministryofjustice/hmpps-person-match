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
sql = (
    "CREATE TABLE temp_data AS SELECT * FROM read_csv("
    "'dev/data/people.csv', "
    "types={"
    "'forename_std_arr': 'VARCHAR[]', 'last_name_std_arr': 'VARCHAR[]', 'sentence_date_arr': 'DATE[]', "
    "'date_of_birth_arr': 'DATE[]', 'postcode_arr': 'VARCHAR[]', 'postcode_outcode_arr': 'VARCHAR[]'"
    "}"
    ");"
)
con.execute(sql)

t2 = time.time()

con.table("temp_data").show()

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
    "crn",
    "prison_number",
    "source_system",
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

con.execute(
    f"""
    INSERT INTO pg_db.personmatch.person
        ({col_string})
    SELECT
        {col_string}
    FROM temp_data;
""",  # NOQA: S608
)
t3 = time.time()

print(f"Inserted in: {t3 - t2}")
