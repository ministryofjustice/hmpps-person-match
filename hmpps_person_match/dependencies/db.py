from db import DatabaseSession


def get_db_session():
    """
    Get the database session
    """
    with DatabaseSession() as db_session:
        yield db_session
