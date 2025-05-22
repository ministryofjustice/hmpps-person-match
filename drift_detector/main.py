import os
from collections.abc import AsyncGenerator

from sqlalchemy import URL, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

DRIVER_NAME = "postgresql+asyncpg"
SSL_ARGS = {
    "ssl": "verify-full",
}

person_match_pg_database_url: URL = URL.create(
    drivername=DRIVER_NAME,
    username=os.environ.get("PERSON_MATCH_DB_USERNAME"),
    password=os.environ.get("PERSON_MATCH_DB_PASSWORD"),
    host=os.environ.get("PERSON_MATCH_DB_HOST"),
    port=os.environ.get("PERSON_MATCH_DB_PORT"),
    database=os.environ.get("PERSON_MATCH_DB_NAME"),
    query=SSL_ARGS,
)

person_match_engine: AsyncEngine = create_async_engine(
    person_match_pg_database_url,
)

PersonMatchAsyncSessionLocal = async_sessionmaker(person_match_engine)

person_record_pg_database_url: URL = URL.create(
    drivername=DRIVER_NAME,
    username=os.environ.get("PERSON_RECORD_DB_USERNAME"),
    password=os.environ.get("PERSON_RECORD_DB_PASSWORD"),
    host=os.environ.get("PERSON_RECORD_DB_HOST"),
    port=os.environ.get("PERSON_RECORD_DB_PORT"),
    database=os.environ.get("PERSON_RECORD_DB_NAME"),
    query=SSL_ARGS,
)

person_record_engine: AsyncEngine = create_async_engine(
    person_record_pg_database_url,
)

PersonRecordAsyncSessionLocal = async_sessionmaker(person_record_engine)


async def get_person_match_db_session() -> AsyncGenerator[AsyncSession]:
    """
    Get the person-match RDS database async connection
    """
    async with PersonMatchAsyncSessionLocal() as session, session.begin():
        yield session


async def get_person_record_db_session() -> AsyncGenerator[AsyncSession]:
    """
    Get the person-record RDS database async connection
    """
    async with PersonRecordAsyncSessionLocal() as session, session.begin():
        yield session


async def main():
    person_match_session: AsyncSession = get_person_match_db_session()
    result = await person_match_session.execute(text("SELECT * FROM personmatch.person"))
    rows = result.fetchall()
    print(f"Person Match Count: {rows}")

    person_record_session = get_person_record_db_session()
    result = await person_record_session.execute(text("SELECT * FROM personmatch.person"))
    rows = result.fetchall()
    print(f"Person Record Count: {rows}")


if __name__ == "__main__":
    main()
