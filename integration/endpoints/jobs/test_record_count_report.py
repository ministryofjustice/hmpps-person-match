import uuid

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_person_match.routes.jobs.record_count_report import ROUTE
from integration.client import Client
from integration.mock_person import MockPerson
from integration.test_base import IntegrationTestBase


class TestRecordCountReportEndPoint(IntegrationTestBase):
    """
    Test record count report endpoint
    """

    @pytest.fixture(autouse=True, scope="function")
    async def before_each(self, db_connection: AsyncSession):
        """
        Before Each
        """
        await self.truncate_person_data(db_connection)

    async def test_record_count_report(
        self,
        call_endpoint,
        create_person_record,
        db_connection: AsyncSession,
    ):
        """
        Test record count report
        """
        person_data = MockPerson(matchId=str(uuid.uuid4()))
        await create_person_record(person_data)

        # Call record count report endpoint
        response = call_endpoint("get", ROUTE, client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 200
