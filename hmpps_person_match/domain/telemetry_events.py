class TelemetryEvents:
    """
    App telemetry events to be sent to appsights
    """

    PERSON_UPDATED_OR_CREATED = "PersonMatchUpdatedOrCreated"
    PERSON_BATCH_UPDATED_OR_CREATED = "PersonMatchBatchUpdatedOrCreated"
    PERSON_DELETED = "PersonMatchUpdatedOrCreated"
    PERSON_SCORE = "PersonMatchScoresCollected"
    JOBS_TERM_FREQUENCY_REFRESH = "JobsTermFrequencyRefreshTriggered"
