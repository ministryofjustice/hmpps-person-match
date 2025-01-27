# Core Person Record Splink Model

A python package that provides Python functions for:
- Cleaning and standardising records from the [`hmpps-person-record`](https://github.com/ministryofjustice/hmpps-person-record/) system to prepare them for linkage
- Training the Core Person Record record linkage model (a [Splink](https://github.com/moj-analytical-services/splink) model)
    - Joining and bulk cleaning raw data from the `hmpps-person-record` Postgres database
    - Training the Splink model from this cleaned data
- The SQL rules that determine how candidate search works
- Real-time scoring of record pairs using this CPR Splink model

### How this fits into the bigger picture of the Core Person Record system

It is assumed that:
- A RDS Postgres database exists with a `person-match-score` schema
- This database contains a live table of records in `splink_model` (cleaned and standardised) format
- Infrastructure exists to allow this package to lookup term frequency values in `splink_model` format.  In the short term, these tables are assumed to existing in postgres, but in future, they may be migrated to Redis for performance reasons.

The [`hmpps-person-match-score`](https://github.com/ministryofjustice/hmpps-person-match-score/) repo contains a Python web service that handles the interface between the `person-match-score` service.

`hmpps-person-match-score` installs this package as a dedendecy and is responsible for:
- Maintaining a live table in postgres cleaned data that has the `splink_model` schema (i.e. web API, database writes),
    - But will call functions from this package to perform the data cleaning
- Maintaining term frequency tables for all columns needed by the Splink model, which are derived from the `person_cleaned` data
    - But will use functions from this package to obtain the SQL needed to create these tables
- Performing candidate search
    - But will use functions from this package to obtain the blocking rules

### Interfaces and functions

The core functionality of relevance to the [`hmpps-person-match-score`](https://github.com/ministryofjustice/hmpps-person-match-score/) web service is:

- `clean()` Take a a `hmpps-person-record` format and turn them into `splink_model` (cleaned and standardised) format
- `score()` Take a (record, record_list) pair in `splink_model_format` schema and return a list of match scores
- TBD term frequency and blocking rules functions

