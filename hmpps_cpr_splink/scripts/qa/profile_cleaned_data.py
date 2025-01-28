import duckdb
import splink.exploratory as exp
from splink import DuckDBAPI

path = "secret_data/cleaned/cleaned/*.parquet"

source_dbs = ["NOMIS", "DELIUS", "COMMON_PLATFORM", "LIBRA"]

source = source_dbs[2]

sql = f"""
create or replace view cleaned
as select * from read_parquet('{path}')
where source_system = '{source}'
"""
con = duckdb.connect(":default:")
con.execute(sql)


db_api = DuckDBAPI(con)
exp.profile_columns("cleaned", db_api, top_n=60, bottom_n=40)


sql = """
select * from cleaned
where first_and_last_name_std = ''
limit 10
"""
con.sql(sql).show()


path = "secret_data/cleaned/joined/*.parquet"

sql = f"""
create or replace view joined
as select * from read_parquet('{path}')
where source_system = 'NOMIS'
"""
con.execute(sql)

exp.profile_columns("joined", db_api, "date_of_birth", top_n=60, bottom_n=40)

path = "secret_data/raw/person/*.parquet"
sql = f"""
select currently_managed, count(*) as count from read_parquet('{path}')
group by 1
"""
con.sql(sql).show()
