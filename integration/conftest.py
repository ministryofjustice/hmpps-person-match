import time
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
