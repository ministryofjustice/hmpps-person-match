from sqlalchemy import URL, create_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import Session

from db.config import Config

driver = "postgres"
dialect = ""
# Construct the database URL
database_url: str = URL.create(
    drivername="postgresql",
    username=Config.DB_USER,
    password=Config.DB_PASSWORD,
    host=Config.DB_HOST,
    database=Config.DB_NAME,
)

engine = create_engine(database_url.render_as_string())

DatabaseSession = sessionmaker(
    autocommit=False, autoflush=False, bind=engine, class_=Session,
)
