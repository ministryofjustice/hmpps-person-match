import base64
import time

import duckdb
import requests
from sqlalchemy import URL

from hmpps_cpr_splink.cpr_splink.interface.db import duckdb_connected_to_postgres
from hmpps_person_match.routes.jobs.term_frequencies import ROUTE as ROUTE_TF
from hmpps_person_match.routes.person.score.person_score import ROUTE as ROUTE_SCORE
from integration.client import Client

client = Client.HMPPS_PERSON_MATCH

client_secret = "clientsecret"  # noqa: S105
encoded_auth_basic: bytes = base64.b64encode(f"{client.value}:{client_secret}".encode())
auth_basic: str = encoded_auth_basic.decode("utf-8")
headers = {
    "Authorization": f"Basic {auth_basic}",
}
params = {
    "grant_type": "client_credentials",
}
response = requests.post(
    "http://localhost:8081/auth/oauth/token",
    headers=headers,
    params=params,
    timeout=30,
)
if response.status_code == 200:
    token = response.json()["access_token"]
else:
    raise Exception(f"Failed to generate access token: {response.status_code}")

pg_url = URL.create(
    drivername="postgresql",
    username="root",
    password="dev",  # noqa: S106
    host="localhost",
    port="5432",
    database="postgres",
)
lim = 1_000
with duckdb_connected_to_postgres(pg_url) as con:
    match_ids = [row[0] for row in con.sql(f"SELECT match_id FROM pg_db.personmatch.person LIMIT {lim}").fetchall()]  # noqa: S608

headers = {"Authorization": f"Bearer {token}"}
r = requests.request("get", "http://localhost:5000/health", headers=headers, timeout=30)
print(r.status_code)
r = requests.request("post", "http://localhost:5000" + ROUTE_TF, headers=headers, timeout=60)
print(r.status_code)
n_calls = len(match_ids)
t1 = time.time()
for match_id in match_ids:
    r = requests.request(
        "get",
        "http://localhost:5000" + ROUTE_SCORE.format(match_id=match_id),
        headers=headers,
        timeout=60,
    )
    r.raise_for_status()
    json_data = r.json()
    if any([row["possible_twins"] for row in json_data]):
        print(f"\t{match_id=}")
        print(json_data)
t2 = time.time()
print(f"Scored {n_calls} in: {t2 - t1}, at an average of {(t2 - t1) / n_calls}")
