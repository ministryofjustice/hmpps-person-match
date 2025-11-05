import os
from enum import Enum


class EnvVars(Enum):
    OAUTH_BASE_URL_KEY = "OAUTH_BASE_URL"
    OAUTH_ISSUER_URL_KEY = "OAUTH_ISSUER_URL_KEY"


def get_env_var(key: EnvVars) -> str:
    """
    Helper function to retrieve an environment variable.
    """
    env_var = os.getenv(key.value)
    if not env_var:
        raise ValueError(f"Missing environment variable: {key.value}")
    return env_var
