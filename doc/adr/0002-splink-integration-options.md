# 2. Splink Integration Options

Date: 2024-03-25

## Status

Accepted

## Context

Core Person Record is defining the approach to establish a person’s identity across both Prison and Probation.  A product called Splink (developed by MoJ Data Science) is going to be used to increase the accuracy of matching data.  Splink is able to run on a batch basis to establish clusters of data records that can be grouped together.  This process will normally run on a weekly (and may, by CPR, have an increased frequency) and is therefore often not the current position of an individual.

An alternative is to access the Splink algorithm directly,  passing in two records to be compared and have it return a probability score that they are the same person.

The Core Person Record key functionality is written using a combination of Kotlin APIs and a Postgres database.  The real-time scoring functionality of Splink is written in Python and uses an in process analytical database (DuckDB) for some of the scoring elements.

The issue, therefore, is considering the integration methods that can be used. The following options were considered.

***Splink Generated SQL***

Splink as previously mentioned, operates using analytical capabilities of DuckDB.  It therefore generates SQL that allows the analysis of two records.  This option allows for the SQL Generator segment of Splink to be run (possibly as part of the CPR Build pipeline), to generate the appropriate SQL to execute.

*Pros*

1. No need to call external services.

2. Will allow the construction of a SQL Statement that can be executed using existing Spring Boot calls.

*Cons*

1. Would generate Duck DB SQL, but as Duck DB would not be running, the generated SQL may need to be manipulated to ensure it can be used.

2. Postgres does not support all of the comparison capabilities required by Splink and therefore functionality may be limited.

3. Potential maintenance Issues if Splink changes its SQL generation + overly complex SQL being used as part of the process.

***Wrapping a call to Python from Kotlin***

There are a number of alternatives that could be used, in order to invoke the Python segment of Splink including the spin up of the necessary Duck DB components.  The Spike considered a number of options and chose Process Builder to achieve this (other options were also considered).

*Pros*

1. Allows the calling and embedding of Python and is able to retrieve the results.

2. Ensures complexity of Splink data manipulation stays within Splink and gives a cleaner separation of concerns.

3. DuckDB would be used,  therefore the Postgres limitations of the previous example are negated.

*Cons*

1. Code is more difficult to maintain.

2. Potential performance issues with Splink dependencies being instantiated on every call.

3. Potential stability issues,  calling code outside of the JVM from within the Core Person Spring Boot application.

4. Potential need to add Python to Build CI pipelines.

***Using a Python API***

This approach would use a Rest API, created using Python which can then be called as a service from within Core Person Record.  A wrapper has already been developed within the PiC team which uses this approach.  The proposal would be for CPR to adopt this component and extend it for CPR needs (as well as supporting it for the PiC team).

*Pros*

1. Cleanest level of separation of concerns,  Python / Splink code only exists in a service specifically for Splink Wrapping Purposes.

2. Initialisation of dependencies can be done within the service e.g. DuckDB to address potential performance issues.

3. Postgres limitations would be negated.

4. Potential stability issues with CPR are negated,  the calling of Rest services and the handling of failures are already understood and used throughout CPR.

*Cons*

1. Limited Python expertise in the team, although this is negated by

   a. A wrapper already being in existence developed by the PiC team

   b. Expertise from the Data Science team to help building this service.

## Decision

Due to the cleaner separation of concerns, expected performance improvements and simplicity of approach; the selected option is to continue with  extending the PiC built Python API described in the final option shown above.

## Consequences

To support these changes,  the ownership of this repository is being transferred to the Core Person Record Team from the Probation In Court Team.  Support for the contents of this repository will be maintained by CPR going forward.
