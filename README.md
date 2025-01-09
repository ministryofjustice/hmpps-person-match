# HMPPS Person Match API

An API wrapper around a model developed by the MoJ Analytical Platform for scoring the confidence 
of people matches across MoJ systems.

`hmpps-person-match` is a simple service built in Python which takes key details about a defendant from a court system and corresponding data from a matched offender from Delius and generates a score. This score is the probability that the two sets of details represent the same individual.

## History

The system is based on some code that was provided by the data science team and subsequently productionised by Probation in Court. Unfortunately at the time the code was produced there were limited options for productionising an algorithm of this type so the implementation relies on a somewhat clunky architecture involving an sqlite database which is not used for storage - only for data processing.

In the event that changes are needed  to it either to improve performance or to extend functionality then we should discuss with Data Science as to whether a better method of productionising is available.


## Pre-Requisites

* Python 3.12+
* [Poetry](https://python-poetry.org/docs/)

```
export POETRY_VERSION=1.8.2
curl -sSL https://install.python-poetry.org | python -
```

## Quickstart

### Install

`make install`

### Run

`make run`

Ping:
```
curl -i \-H "Content-Type: application/json" http://127.0.0.1:5000/ping
``````

Check health:
```
curl -i \-H "Content-Type: application/json" http://127.0.0.1:5000/health
```

Generate a score:
```
curl -i \
    -H "Content-Type: application/json" \
    -X POST -d "{\"unique_id\":{\"0\":\"861\",\"1\":\"862\"},\"first_name\":{\"0\":\"Lily\",\"1\":\"Lily\"},\"surname\":{\"0\":\"Robinson\",\"1\":\"Robibnson\"},\"dob\":{\"0\":\"2009-07-06\",\"1\":\"2009-07-06\"},\"pnc_number\":{\"0\":\"2001/0141640Y\",\"1\":\"None\"},\"source_dataset\":{\"0\":\"libra\",\"1\":\"delius\"}}" \
    http://127.0.0.1:5000/match
```

The key data item in the response is `match_probability`:
```  
"match_probability": {
    "0": 0.9172587927
  },
  ```

### Test

`make test`

### Docker build

```make build```

## Monitoring

Application Insights monitoring is available for the service using the `cloud_RoleName` specified below:

```
requests
| where cloud_RoleName == 'hmpps-person-match'
```

