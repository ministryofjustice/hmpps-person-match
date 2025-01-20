import os
from enum import Enum

import pytest
from testcontainers.compose import DockerCompose


class Service(Enum):
    """
    Docker services
    """

    def __init__(self, service: str, port: int):
        self.container_name: str = service
        self.port: int = port

    HMPPS_PERSON_MATCH = ("hmpps-person-match", 5000)


@pytest.fixture(scope="session")
def docker_compose():
    """
    Manage docker containers
    """
    with DockerCompose(os.path.join(os.getcwd()), "docker-compose.yml") as compose:
        yield compose


@pytest.fixture(scope="session")
def service_url(docker_compose):
    """
    Build service URL
    """
    def _build_url(service: Service):
        client_port = docker_compose.get_service_port(service.container_name, service.port)
        return f"http://localhost:{client_port}"

    return _build_url


@pytest.fixture(scope="session")
def person_match_url(service_url) -> str:
    """
    Return url for hmpps-person-match
    """
    return service_url(Service.HMPPS_PERSON_MATCH)
