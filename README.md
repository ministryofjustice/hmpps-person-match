# HMPPS Person Match API

[![API docs](https://img.shields.io/badge/API_docs_-view-85EA2D.svg?logo=swagger)](https://hmpps-person-match-dev.hmpps.service.justice.gov.uk/swagger-ui.html)

An API wrapper around a model developed by the MoJ Analytical Platform for scoring the confidence 
of people matches across MoJ systems.

`hmpps-person-match` is a simple service built in Python which takes key details about a defendant from a court system and corresponding data from a matched offender from Delius and generates a score. This score is the probability that the two sets of details represent the same individual.

## History

The system is based on some code that was provided by the data science team and subsequently productionised by Probation in Court. Unfortunately at the time the code was produced there were limited options for productionising an algorithm of this type so the implementation relies on a somewhat clunky architecture involving an sqlite database which is not used for storage - only for data processing.

In the event that changes are needed  to it either to improve performance or to extend functionality then we should discuss with Data Science as to whether a better method of productionising is available.


## Pre-Requisites

* Python 3.12+
* [uv](https://docs.astral.sh/uv/)


```shell
curl -LsSf https://astral.sh/uv/0.5.24/install.sh | sh
```

## Quickstart

### Install

To install the dependencies and setup the virtual environment, run the following command:

```shell
make install
```

### Run locally

To start the development server locally, run the following command:

```shell
make run-local
```

Utilises hot reloading so you can make changes to the application without having to restart the server.

Which means you can now call the locally running application.

Calling the health endpoint:

```shell
curl -i \-H "Content-Type: application/json" http://127.0.0.1:5000/health
```

### Testing

To run the test suite run the following command:

```shell
make test
```

To run the integration tests run the following command:

```shell
make test-integration
```

## Debugging

We have enabled [debugpy](https://github.com/microsoft/debugpy) when running the containers locally to enable debug support. To attach the applications
debugger create the following .vscode task within the `.vscode/launch.json`, create the file if you don't already have it:

```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Python Debugger: Remote Attach",
            "type": "debugpy",
            "request": "attach",
            "connect": {
                "host": "localhost",
                "port": 5678
            },
            "justMyCode": true,
            "pathMappings": [
                {
                    "localRoot": "${workspaceFolder}",
                    "remoteRoot": "/app"
                }
            ]
        }
    ]
}
```

When the containers are running using the command `make start-containers` you can run the following task and set breakpoints with the code to begin debugging.

To test this is working as expected:

1. Setting a breakpoint on the health endpoint in health_view.py
2. Running the command: `curl http://0.0.0.0:5000/health`

It should stop execution at the set breakpoint to allow you to start stepping through the code.

### Linting

To run the linter, run the following command:

```shell
make lint
```

If any errors are auto fixable run the following command:

```shell
make lint-fix
```

### Formating

To run the automatic python file formatter, run the following command:

```shell
make format
```

### Docker build

To build the application as a Docker image, run the following command:

```shell
make build
```

## Monitoring

Application Insights monitoring is available for the service using the `cloud_RoleName` specified below:

```
requests
| where cloud_RoleName == 'hmpps-person-match'
```

