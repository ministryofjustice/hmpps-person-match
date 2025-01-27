# sketches of api

## Postgres

Configure Postgres connexion via environment variables.
That way it's easy to point to test/dev/prod as we need.
Also means we don't need users to know anything about how we acceess database.

## score

```python
from hmpps_cpr_splink.cpr_splink import clean, score

pg_connexion = get_postgres_connexion()  # however this comes

# we start with a record in joined format, and convert to standardised form
joined_record = get_from_queue()  # or API call or w/e
standardised_record = clean(joined_record)  # don't look up tf tables as we don't want to save values with records
# and then we save a copy to our database - not our responsibility
db_record = save_to_match_score_db(standardised_record, pg_connexion)

# then we get score
primary_id = db_record["id"]
scored_candidates = score(primary_id)
send_to_person_record(scored_candidates)
```

In this form, we don't want to store postcodes with standardised form.
We want to look them up in realtime and attach at that stage. Otherwise they will be stale

## candidate search

With our materialised view of cleaned persons should we store & index all columns and expressions used in blocking?
i.e. things like

* `first_name`
* `substr(name_1_std, 1, 1) || '---' || substr(last_name_std, 1, 1) || '---' ||| postcode_arr[1]`

## outdated

Two main input choices:
* organise by record, or by columns
* explicit fields, or everything in a dict

but also:
* are tf columns just bundled in, or treated separately?

```python
import cpr_splink as cs

record_1 = {"id": 1, "name_1_std": "ANDREW", "tf_name_1_std": 0.00123, ...}
record_2 = {...}

data = [record_1, record_2]

score = cs.score(data)
```

This version probably makes sense (modulo perhaps how tf-adjustments are included) as both Splink and (as far as I'm aware) the flask app deal in terms of record rows (rather than columns).

Another option, split by field:
```python
import cpr_splink as cs

score = cs._score(
    ids=[1, 2],
    name_1_stds=["ANDREW", "ANDY"],
    ...
)
```

This makes it easier to directly see the required fields, but requires some reshaping for scoring (and probably to coerce into this shape on the caller's side).

### a question

Do we want the flexibility to score more than a single pair?
Set up for that on the Splink side, but not sure how entrenched the routes are in the app for only single pairings.

Some other questions around responsibility etc.:
* statefulness of DuckDB connexion - spun up each time or longer-lived? Does it live here or passed in?
* What do we give back? May depend on the above - if app has the connexion we could return duckdbpyrelation. Or return as a dict, to mirror the input form
* will we just deal with the types using duckdb? (more explicitly than in this sketch, obvs)

## cleaning SQL

This we can cache as effectively a constant, but it can also probably just be stored as an attribute of the app.

Can have it saved to a file as part of package also if it were needed not in-memory.

```python
import cpr_splink as cs

sql = cs.cleaning_sql(base_table="incoming_records")

...
cleaned = con.sql(sql)

```

## synchronisation

A couple of things that we might want to have some automated tests on:

* ensure that the saved file SQL is in sync with the version we construct modularly / test
* ensure that the save model `.json` is in sync with the defined model, and that these are also in sync with the columns defined in the `score` api
