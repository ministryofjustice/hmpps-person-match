from contextlib import asynccontextmanager

from fastapi import FastAPI

from hmpps_person_match.db import engine


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan func of app
    Disposes of DB engine to release resources
    """
    yield
    await engine.dispose()
