# 4. Switch from flask to fastapi

Date: 2025-01-10

## Status

Accepted

## Context

Flask is the current framework which is limited when it comes to async processing, limiting its performance under heavy data handling with the person-match application resposility is moving toward. As well as the now need to interact with a database instance, a new framework is recommended to aid in the implementation.

### Move from flask to fastapi
This would involve switching the framework to use the fastapi library instead of the flask library. Results in a change in requirements of the application and a refactor of the application configuration.

**Pros**
1. Easier routing for endpoints
2. Asynchronous task support
3. Automatic swagger documentation
4. Built-in pydantic
5. Increased performance

**Cons**
1. Slightly more complex than flask to implement

## Decision
It has been decided to transition from flask to fastapi.

## Consequences

This will mean a new framework is introduced to the organization with limited experience with framework and language.