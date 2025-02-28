import time
import uuid
from collections.abc import AsyncGenerator
from enum import Enum

import pytest
import requests
from sqlalchemy import URL
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from hmpps_cpr_splink.cpr_splink.interface import clean
from hmpps_person_match.models.person.person import Person
from hmpps_person_match.models.person.person_batch import PersonBatch
from integration.test_person import TestPerson


class Service(Enum):
    """
    Docker services
    """

    def __init__(self, host: str, port: int, health_route: str = "/"):
        self.host: str = host
        self.port: int = port
        self.health_route: str = health_route

    HMPPS_PERSON_MATCH = ("localhost", 5000, "/health")


@pytest.fixture(scope="session")
def get_service():
    """
    Start and check service is running
    """

    def _wait_for_health(service: Service, timeout=60, interval=2):
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
def person_match_url(get_service):
    """
    Start and check service is running for hmpps-person-match
    """
    return get_service(Service.HMPPS_PERSON_MATCH)


@pytest.fixture()
def match_id():
    """
    Generate UUID
    """
    return str(uuid.uuid4())


@pytest.fixture()
def create_person_data():
    """
    Create a new person data
    """

    def _create_person_json(person_data: TestPerson) -> dict:
        return person_data.model_dump(by_alias=True)

    return _create_person_json


@pytest.fixture()
async def db_connection() -> AsyncGenerator[AsyncSession]:
    database_url = URL.create(
        drivername="postgresql+asyncpg",
        username="root",
        password="dev",  # noqa: S106
        host="localhost",
        port="5432",
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
        port="5432",
        database="postgres",
    )


@pytest.fixture()
async def create_person_record(db_connection):
    async def _create_person(person: Person):
        await clean.clean_and_insert(PersonBatch(records=[person]), db_connection)

    return _create_person
