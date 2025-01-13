import os

import pytest
from fastapi.testclient import TestClient


@pytest.fixture(scope="module")
def client(app):
    return TestClient(app)

@pytest.fixture(autouse=True)
def set_env_vars():
    os.environ["APP_BUILD_NUMBER"] = "number"
    os.environ["APP_GIT_REF"] = "ref"
    os.environ["APP_GIT_BRANCH"] = "branch"
    os.environ["OAUTH_BASE_URL"] = "http://localhost:5000"
