from collections.abc import AsyncGenerator

from sqlalchemy import URL
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from hmpps_person_match.db.config import Config, DatabaseDriver


def build_database_url(driver: DatabaseDriver) -> URL:
    """
    Build database connection
    """
    db_query_string_params = {}
    if Config.DB_SSL_ENABLED:
        if driver == DatabaseDriver.ASYNC:
            db_query_string_params["ssl"] =  "verify-full"
        if driver == DatabaseDriver.SYNC:
            db_query_string_params["sslmode"] =  "verify-full"

    # Construct the database URL
    return URL.create(
        drivername=driver.value,
        username=Config.DB_USER,
        password=Config.DB_PASSWORD,
        host=Config.DB_HOST,
        port=Config.DB_PORT,
        database=Config.DB_NAME,
        query=db_query_string_params,
    )

engine: AsyncEngine = create_async_engine(
    build_database_url(DatabaseDriver.ASYNC),
    echo=Config.DB_LOGGING,
    pool_size=Config.DB_CON_POOL_SIZE,
    max_overflow=Config.DB_CON_POOL_MAX_OVERFLOW,
    pool_timeout=Config.DB_CON_POOL_TIMEOUT,
    pool_recycle=Config.DB_CON_POOL_RECYCLE,
    pool_pre_ping=Config.DB_CON_POOL_PRE_PING,
)


async def get_db_connection() -> AsyncGenerator[AsyncConnection]:
    """
    Get the database connection
    """
    async with engine.begin() as connection:
        yield connection
