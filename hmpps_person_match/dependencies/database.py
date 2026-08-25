
from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_person_match.db import get_db_session

TransactionalSession = Annotated[
    AsyncSession,
    Depends(get_db_session, scope="function"),
]
