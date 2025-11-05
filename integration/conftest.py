import time
from collections.abc import AsyncGenerator, Callable
from enum import Enum

import pytest
import requests
from sqlalchemy import URL
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from integration.person_factory import PersonFactory


class Service(Enum):
    """
    Docker services
    """

    def __init__(self, host: str, port: int, health_route: str = "/") -> None:
        self.host: str = host
        self.port: int = port
        self.health_route: str = health_route

    HMPPS_PERSON_MATCH = ("localhost", 5000, "/health")


@pytest.fixture(scope="session")
def get_service() -> Callable:
    """
    Start and check service is running
    """

    def _wait_for_health(service: Service, timeout: int = 60, interval: int = 2) -> str:
        """
        Polls a health endpoint until it returns HTTP 200 OK.
        """
        start_time = time.time()
        url = f"http://{service.host}:{service.port}"
        health_url = url + service.health_route

        while time.time() - start_time < timeout:
            try:
                response = requests.get(health_url, timeout=10)
                if response.status_code == 200:
                    return url
            except requests.exceptions.RequestException:
                print(f"Waiting for {url}... for {interval} seconds")

            time.sleep(interval)  # Wait before retrying

        raise TimeoutError(f"Service did not become healthy within {timeout} seconds.")

    return _wait_for_health


@pytest.fixture(scope="session")
def person_match_url(get_service: Callable) -> None:
    """
    Start and check service is running for hmpps-person-match
    """
    return get_service(Service.HMPPS_PERSON_MATCH)


@pytest.fixture()
async def db_connection() -> AsyncGenerator[AsyncSession]:
    database_url = URL.create(
        drivername="postgresql+asyncpg",
        username="root",
        password="dev",  # noqa: S106
        host="localhost",
        port=5432,
        database="postgres",
    )

    engine: AsyncEngine = create_async_engine(
        database_url,
        pool_pre_ping=True,
    )

    AsyncSessionLocal = async_sessionmaker(engine)  # noqa: N806

    async with AsyncSessionLocal() as session:
        yield session

    await engine.dispose()


@pytest.fixture()
def pg_db_url() -> URL:
    return URL.create(
        drivername="postgresql",
        username="root",
        password="dev",  # noqa: S106
        host="localhost",
        port=5432,
        database="postgres",
    )


@pytest.fixture()
async def person_factory(db_connection: AsyncSession) -> PersonFactory:
    return PersonFactory(db_connection)
