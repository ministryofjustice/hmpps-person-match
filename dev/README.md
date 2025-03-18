# Dummy data

## Usage

Create a set of dummy data by running:

```sh
uv run python dev/generate_data.py
```

This will get saved to a `.csv` in `dev/data`.

Reference set is 3,500,000 rows, but adjust the value in-script for your use-case.

With containers running as set up in [docker compose file](../docker-compose.yml), insert the data into your local postgres container by running:

```sh
uv run python dev/insert_data.py
```

This will take a while - subsequent insertions can be made faster by generating a db dump with [`pg_dump`](https://www.postgresql.org/docs/current/app-pgdump.html).

In-container `postgres-hpm` run:

```
pg_dump -f db_dump.sql
```

Then from outside container, copy the dump to a local file, and load into database:

```sh
docker cp postgres-hpm:/db_dump.sql ./dev/data/dump.sql
cat ./dev/data/dump.sql | docker exec -i postgres-hpm psql -h localhost -U root -f-
```

## The data

Dummy data is generated with [Faker](https://faker.readthedocs.io/en/master/).
These are just random values, with no attempt to create 'individuals' with realistic errors/value variance.
As such, scores will not be representative.
There are a limit on number of unique `cro` and `pnc` values generated to create duplicate values.

To improve the realism of the fake data adjust the [generation script accordingly](./generate_data.py).
