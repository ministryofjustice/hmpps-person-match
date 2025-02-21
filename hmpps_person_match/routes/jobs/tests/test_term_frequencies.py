from hmpps_person_match.domain.telemetry_events import TelemetryEvents
from hmpps_person_match.routes.jobs.term_frequencies import ROUTE


class TestTermFrequencyGenerationRoute:
    """
    Test Person Create Route
    """

    def test_term_frequency_refreshes(self, call_endpoint, mock_logger, mock_db_connection):
        response = call_endpoint("post", ROUTE, roles=[])
        assert response.status_code == 200
        assert response.json() == {}
        mock_logger.info.assert_called_with(TelemetryEvents.JOBS_TERM_FREQUENCY_REFRESH)
