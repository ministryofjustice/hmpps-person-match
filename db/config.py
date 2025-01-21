import os


class Config:
    """
    Database configuration
    """
    DB_DRIVER = "postgresql"
    DB_USER = os.environ.get("DATABASE_USERNAME", "root")
    DB_PASSWORD = os.environ.get("DATABASE_PASSWORD", "dev")
    DB_HOST = os.environ.get("DATABASE_ENDPOINT", "localhost")
    DB_PORT = os.environ.get("POSTGRES_PORT", "5432")
    DB_NAME = os.environ.get("DATABASE_NAME", "postgres")
