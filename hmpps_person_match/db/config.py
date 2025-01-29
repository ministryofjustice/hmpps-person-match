import os


class Config:
    """
    Database configuration
    """

    DB_DRIVER = "postgresql+psycopg"
    DB_USER = os.environ.get("DATABASE_USERNAME")
    DB_PASSWORD = os.environ.get("DATABASE_PASSWORD")
    DB_HOST = os.environ.get("DATABASE_HOST")
    DB_PORT = os.environ.get("DATABASE_PORT", 5432)
    DB_NAME = os.environ.get("DATABASE_NAME")
    DB_LOGGING = os.environ.get("DB_LOGGING", "False") == "True"
    DB_SSL_ENABLED = os.environ.get("DB_SSL_ENABLED", "True") == "True"
