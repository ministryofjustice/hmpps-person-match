import os


class Config:
    """
    Database configuration
    """
    # The default DB parameters are set to allow you to connect to the Docker DB
    DB_USER = os.environ.get("DATABASE_USERNAME", "root")
    DB_PASSWORD = os.environ.get("DATABASE_PASSWORD", "dev")
    DB_HOST = os.environ.get("DATABASE_ENDPOINT", "localhost")
    DB_PORT = os.environ.get("POSTGRES_PORT", "5432")
    DB_NAME = os.environ.get("DATABASE_NAME", "postgres")
