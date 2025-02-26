from sqlalchemy import URL

from hmpps_person_match.db.config import Config

asyncpg_db_query_string_params = {}
if Config.DB_SSL_ENABLED:
    asyncpg_db_query_string_params["ssl"] = "verify-full"

asyncpg_database_url: URL = URL.create(
    drivername=Config.DB_ASYNCPG_DRIVER,
    username=Config.DB_USER,
    password=Config.DB_PASSWORD,
    host=Config.DB_HOST,
    port=Config.DB_PORT,
    database=Config.DB_NAME,
    query=asyncpg_db_query_string_params,
)


pg_db_query_string_params = {}
if Config.DB_SSL_ENABLED:
    pg_db_query_string_params["sslmode"] = "verify-full"

pg_database_url: URL = URL.create(
    drivername=Config.DB_PG_DRIVER,
    username=Config.DB_USER,
    password=Config.DB_PASSWORD,
    host=Config.DB_HOST,
    port=Config.DB_PORT,
    database=Config.DB_NAME,
    query=pg_db_query_string_params,
)
