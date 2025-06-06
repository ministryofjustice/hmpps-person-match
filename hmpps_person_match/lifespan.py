from fastapi import FastAPI

from hmpps_person_match.db import engine


async def lifespan(app: FastAPI):
    """
    Lifespan func of app
    Disposes of DB engine to release resources
    """
    yield
    await engine.dispose()
