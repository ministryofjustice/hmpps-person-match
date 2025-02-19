import os


class Config:
    """
    Database configuration
    """

    # Database connection settings
    DB_DRIVER = "postgresql+asyncpg"
    DB_USER = os.environ.get("DATABASE_USERNAME")
    DB_PASSWORD = os.environ.get("DATABASE_PASSWORD")
    DB_HOST = os.environ.get("DATABASE_HOST")
    DB_PORT = os.environ.get("DATABASE_PORT", 5432)
    DB_NAME = os.environ.get("DATABASE_NAME")
    DB_LOGGING = os.environ.get("DB_LOGGING", "False") == "True"
    DB_SSL_ENABLED = os.environ.get("DB_SSL_ENABLED", "True") == "True"

    # Database connection pool settings
    DB_CON_POOL_SIZE = 30  # Max connections in the pool
    DB_CON_POOL_MAX_OVERFLOW = 5  # Additional connections allowed beyond pool size
    DB_CON_POOL_TIMEOUT = 30  # Wait time before timeout if pool is full (seconds)
    DB_CON_POOL_RECYCLE = 180  # Wait time before connection is recycled (seconds)
    DB_CON_POOL_PRE_PING = True  # Test connections before using them
