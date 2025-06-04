from unittest.mock import Mock

from hmpps_person_match.domain.telemetry_events import TelemetryEvents
from hmpps_person_match.routes.jobs.record_count_report import ROUTE


class TestRecordCountReportEndPoint:
    """
    Test record count report endpoint
    """

    async def test_no_records_found_report(
        self,
        call_endpoint,
        mock_db_connection,
        mock_logger,
    ):
        """
        Test when record count report is empty
        """
        mock_db_result = Mock()
        mock_db_result.mappings().fetchall.return_value = []
        mock_db_connection.execute.return_value = mock_db_result

        # Call record count report endpoint
        response = call_endpoint("post", ROUTE)
        assert response.status_code == 200

        mock_logger.info.assert_called_with(
            TelemetryEvents.PERSON_MATCH_RECORD_COUNT_REPORT,
            extra={},
        )

    async def test_record_count_report(
        self,
        call_endpoint,
        mock_db_connection,
        mock_logger,
    ):
        """
        Test record count report
        """
        mock_db_result = Mock()
        mock_db_result.mappings().fetchall.return_value = [
            {
                "source_system": "LIBRA",
                "count": 2,
            },
            {
                "source_system": "COMMON_PLATFORM",
                "count": 200,
            },
            {
                "source_system": "NOMIS",
                "count": 100,
            },
            {
                "source_system": "DELIUS",
                "count": 5,
            },
        ]
        mock_db_connection.execute.return_value = mock_db_result

        # Call record count report endpoint
        response = call_endpoint("post", ROUTE)
        assert response.status_code == 200

        mock_logger.info.assert_called_with(
            TelemetryEvents.PERSON_MATCH_RECORD_COUNT_REPORT,
            extra={"LIBRA": 2, "COMMON_PLATFORM": 200, "NOMIS": 100, "DELIUS": 5},
        )
