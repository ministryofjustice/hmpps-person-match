import os
from enum import Enum

import pytest
import requests
from requests.exceptions import ConnectionError as ReqConnectionError


class Service(Enum):
    """
    Docker services
    """

    def __init__(self, service: str, port: int, health_route: str = "/"):
        self.container_name: str = service
        self.port: int = port
        self.health_route: str = health_route

    HMPPS_PERSON_MATCH = ("hmpps-person-match", 5000, "/health")


@pytest.fixture(scope="session")
def docker_compose_project_name():
    return "hmpps-person-match"


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return os.path.join(str(pytestconfig.rootdir), "docker-compose.yml")


@pytest.fixture(scope="session")
def get_service(docker_ip, docker_services):
    """
    Ensure that docker service is up and responsive.
    """

    def _is_responsive(url):
        try:
            response = requests.get(url, timeout=30)
            return response.status_code == 200
        except ReqConnectionError:
            return False

    def _service_get_factory(service: Service):
        # `port_for` takes a container port and returns the corresponding host port
        port = docker_services.port_for(service.container_name, service.port)
        url = f"http://{docker_ip}:{port}"
        health_endpoint = url + service.health_route
        docker_services.wait_until_responsive(
            timeout=30.0,
            pause=0.1,
            check=lambda: _is_responsive(health_endpoint),
        )
        return url

    return _service_get_factory


@pytest.fixture(scope="session")
def person_match_url(get_service):
    """
    Start and check service is running for hmpps-person-match
    """
    return get_service(Service.HMPPS_PERSON_MATCH)
