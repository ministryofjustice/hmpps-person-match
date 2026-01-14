<# HMPPS Person Match API

[![API docs](https://img.shields.io/badge/API_docs_-view-85EA2D.svg?logo=swagger)](https://hmpps-person-match-dev.hmpps.service.justice.gov.uk/swagger-ui.html)

[![Ministry of Justice Repository Compliance Badge](https://github-community.service.justice.gov.uk/repository-standards/api/hmpps-person-match/badge?style=flat)](https://github-community.service.justice.gov.uk/repository-standards/hmpps-person-match)

An API wrapper around a model developed by the MoJ Analytical Platform for scoring the confidence 
of people matches across MoJ systems.

## Pre-Requisites

* Python 3.14
* [uv](https://docs.astral.sh/uv/)


```shell
curl -LsSf https://astral.sh/uv/0.9.7/install.sh | sh
```

Keep `uv` up to date by running `uv self update` to make sure it matches the version specified in the Dockerfile

If you need to update a transitive dependency (for example to fix a security vulnerability), do this:

0. make sure you are on the latest version of uv
1. `uv lock --upgrade-package <package-name>==<package-version>`

This should result in a small change to the lockfile updating the transitive dependency to the desired version.

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

### Formatting

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

