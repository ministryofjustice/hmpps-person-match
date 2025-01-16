import os
from enum import Enum


class EnvVars(Enum):
    OAUTH_BASE_URL_KEY = "OAUTH_BASE_URL"


def get_env_var(key: EnvVars):
    """
    Helper function to retrieve an environment variable.
    """
    env_var = os.getenv(key.value)
    if not env_var:
        raise ValueError(f"Missing environment variable: {key.value}")
    return env_var
