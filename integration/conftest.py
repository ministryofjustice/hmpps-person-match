import uuid
from enum import Enum

import asyncpg
import pytest
import requests


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
    def _check_service(service: Service):
        """
        Check service is running
        """
        url = f"http://{service.host}:{service.port}"
        health_url = url + service.health_route
        response = requests.get(health_url, timeout=30)
        if response.status_code == 200:
            return url
        else:
            raise Exception(f"Service {service.name} is not running: {response.status_code}")

    return _check_service

@pytest.fixture(scope="session")
def person_match_url(get_service):
    """
    Start and check service is running for hmpps-person-match
    """
    return get_service(Service.HMPPS_PERSON_MATCH)


@pytest.fixture()
async def db():
    """
    Get database connection
    """
    connection = await asyncpg.connect(
        host="localhost",
        port=5432,
        user="root",
        password="dev",  # noqa: S106
        database="postgres",
    )
    yield connection
    await connection.close()


@pytest.fixture()
def generate_uuid():
    """
    Generate UUID
    """
    return str(uuid.uuid4())
