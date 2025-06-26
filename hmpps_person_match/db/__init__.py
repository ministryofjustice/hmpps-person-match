from collections.abc import AsyncGenerator


from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from hmpps_person_match.db import url
from hmpps_person_match.db.config import Config

engine: AsyncEngine = create_async_engine(
    url.asyncpg_database_url,
    echo=Config.DB_LOGGING,
    pool_size=Config.DB_CON_POOL_SIZE,
    max_overflow=Config.DB_CON_POOL_MAX_OVERFLOW,
    pool_timeout=Config.DB_CON_POOL_TIMEOUT,
    pool_recycle=Config.DB_CON_POOL_RECYCLE,
    pool_pre_ping=Config.DB_CON_POOL_PRE_PING,
)



AsyncSessionLocal = async_sessionmaker(engine)


async def get_db_session() -> AsyncGenerator[AsyncSession]:
    """
    Get the database async connection
    """
    async with AsyncSessionLocal() as session, session.begin():
        yield session
