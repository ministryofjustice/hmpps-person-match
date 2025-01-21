import os


class Config:
    """
    Database configuration
    """
    # The default DB parameters are set to allow you to connect to the Docker DB
    DB_USER = os.environ.get("POSTGRES_USER", "root")
    DB_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "dev")
    DB_HOST = os.environ.get("POSTGRES_HOST", "localhost")
    DB_PORT = os.environ.get("POSTGRES_PORT", "5436")
    DB_NAME = os.environ.get("POSTGRES_DB", "postgres")
