import asyncio
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_person_match.routes.jobs.term_frequencies import TERM_FREQUENCY_TABLES


class IntegrationTestBase:
    """
    Base class for integration tests.
    """

    async def truncate_person_data(self, db_connection: AsyncSession):
        """
        Delete all records from the person table
        """
        await db_connection.execute(text("TRUNCATE TABLE personmatch.person"))
        await db_connection.commit()
        await self.until_asserted(lambda: self.assert_size_of_table(db_connection, "person", size=0))

    async def refresh_term_frequencies(self, db_connection: AsyncSession):
        """
        Refresh term frequencies
        """
        for tf_table in TERM_FREQUENCY_TABLES:
            await db_connection.execute(text(f"REFRESH MATERIALIZED VIEW CONCURRENTLY personmatch.{tf_table};"))
        for table in TERM_FREQUENCY_TABLES:
            await self.until_asserted(lambda: self.assert_size_of_table(db_connection, table, size=0))  # noqa: B023

    @staticmethod
    async def find_by_match_id(db_connection: AsyncSession, match_id: str) -> dict:
        result = await db_connection.execute(text(f"SELECT * FROM personmatch.person WHERE match_id = '{match_id}'"))
        return result.mappings().fetchone()

    @staticmethod
    def to_datetime_object(date: str) -> datetime:
        return datetime.strptime(date, "%Y-%m-%d").astimezone().date()

    @staticmethod
    async def assert_size_of_table(db_connection, table, size=1):
        result = await db_connection.execute(text(f"SELECT * FROM personmatch.{table}"))  # noqa: S608
        rows = result.fetchall()
        assert len(rows) == size

    async def until_asserted(self, assertion_func, max_retries=5, delay=0.3):
        """
        Repeatedly tries to assert something until it passes or max_retries is reached.

        :param assertion_func: A function that performs the assertion.
        :param max_retries: Maximum number of retries.
        :param delay: Delay (in seconds) between retries.
        :raises AssertionError: If the assertion fails after max_retries attempts.
        """
        for attempt in range(1, max_retries + 1):
            try:
                await assertion_func()
                return
            except AssertionError as e:
                if attempt == max_retries:
                    raise e
                else:
                    print(f"Assertion failed, retrying... ({attempt}/{max_retries})")
                    await asyncio.sleep(delay)
