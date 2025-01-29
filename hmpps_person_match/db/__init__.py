from collections.abc import AsyncGenerator

from sqlalchemy import URL, Connection
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from hmpps_person_match.db.config import Config

db_query_string_params = None
if Config.DB_SSL_ENABLED:
    db_query_string_params = {
        "sslmode": "verify-full",
    }

# Construct the database URL
database_url: URL = URL.create(
    drivername=Config.DB_DRIVER,
    username=Config.DB_USER,
    password=Config.DB_PASSWORD,
    host=Config.DB_HOST,
    port=Config.DB_PORT,
    database=Config.DB_NAME,
    query=db_query_string_params,
)

engine: AsyncEngine = create_async_engine(
    database_url,
    echo=Config.DB_LOGGING,
    pool_size=Config.DB_CON_POOL_SIZE,
    max_overflow=Config.DB_CON_POOL_MAX_OVERFLOW,
    pool_timeout=Config.DB_CON_POOL_TIMEOUT,
    pool_recycle=Config.DB_CON_POOL_RECYCLE,
    pool_pre_ping=Config.DB_CON_POOL_PRE_PING,
)

async def get_db_connection() -> AsyncGenerator[Connection]:
    """
    Get the database connection
    """
    async with engine.connect() as connection:
        yield connection
