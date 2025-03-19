import base64
import time

import requests

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
    "http://localhost:8080/auth/oauth/token",
    headers=headers,
    params=params,
    timeout=30,
)
if response.status_code == 200:
    token = response.json()["access_token"]
else:
    raise Exception(f"Failed to generate access token: {response.status_code}")

match_ids = [
    "656925f3-b874-45a3-a015-c7165a5e0261",
    "b17f8bd8-e99a-4de9-b0d5-ec89f9280cf7",
    "fc5d90f6-7ec7-44fd-9fe4-49bac65fa181",
    "03d0781f-b126-407c-add2-875722d46eb2",
    "ec75ed51-49f2-4220-b90b-f84aef3e9b18",
    "e3d6d76e-3a42-4268-afcd-0f361590d892",
    "599d0f1a-a794-4cc8-9981-cb6dc95ea26a",
    "3f314456-0081-4185-a552-24bfedcf4681",
    "183419f8-4675-448a-a2d7-2604973dfc2d",
    "5fd9f78d-2da5-4e93-a27e-301ddb10ef69",
]
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
    print(r.status_code)
    print(r.json())
t2 = time.time()
print(f"Scored {n_calls} in: {t2 - t1}, at an average of {(t2 - t1) / n_calls}")
