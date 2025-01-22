from sqlalchemy import URL, create_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import Session

from hmpps_person_match.db.config import Config

db_query = None
if Config.DB_SSL_ENABLED:
    db_query = {
        "sslmode": "verify-full",
    }

# Construct the database URL
database_url: str = URL.create(
    drivername=Config.DB_DRIVER,
    username=Config.DB_USER,
    password=Config.DB_PASSWORD,
    host=Config.DB_HOST,
    port=Config.DB_PORT,
    database=Config.DB_NAME,
    query=db_query,
)

engine = create_engine(
    database_url,
    echo=Config.DB_LOGGING,
)

DatabaseSession = sessionmaker(
    autocommit=False, autoflush=False, bind=engine, class_=Session,
)

def get_db_session():
    """
    Get the database session
    """
    with DatabaseSession() as db_session:
        yield db_session
