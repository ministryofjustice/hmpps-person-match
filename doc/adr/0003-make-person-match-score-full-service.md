# 3. Make Person Match Full Service

Date: 2024-12-06

## Status

Accepted

## Context

To facilitate the extension of the Splink Model to support matching for Core Person Record it is necessary to have access to data held within Core Person Record (CPR).  A number of options to allow for this change have been considered.

### Move hmpps-person-match logic to hmpps-person-record
This would involve adding the ability to call Python Splink code within the Core Person Record Kotlin service.  It would likely involve a number of 'call outs' from Kotlin to Python

**Pros**
1. Single Project for CPR Team

**Cons**
1. More complex build pipeline and test infrastructure to test a combination of Kotlin, SQL and Python Code
2. Difficulties in monitoring, maintaining and releasing multiple code bases
3. Tight Coupling between CPR Services and Splink

### Give Direct Access to CPR DB from hmpps-person-match
**Pros**
1. Allows Splink access to all the data that it needs
2. Standard approach taken by data science to run tools on databases

**Cons**
1. Provides Tight Coupling between hmpps-person-match and hmpps-person-record
2. Creates Security Issues due to it having full access to all CPR data including Protected Characteristics

### Extend this service to be a fully fledged Python service
This will mean the service will 
1. Have its own data store holding the minimal amount of data needed for scoring purposes
2. Listen to events 'fired' by hmpps-person-record in order to keep its own data in store
3. Can optionally have a DuckDB instance embedded within Python

**Pros**
1. Gives hmpps-person-match the ability to store all local data needed for linking,  generating term frequency tables and carrying out Model Retraining
2. Creates a clean separation between business logic of core person and scoring / matching logic of Splink

**Cons**
1. Python is not in the standard HMPPS tech stack.  So will require the CPR Team to ensure it always has a developer available that is capable of maintaining the project.
2. Additional security constraints as the service will no longer be stateless and will own data in its own right.

## Decision
It has been decided to extend this service to be a fully fledged Python service

## Consequences

This will ensure that all Splink capabilities will be provided for by this project.  It will have its own pipeline,  tests and security implementations.
