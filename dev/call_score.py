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
    "http://localhost:9090/auth/oauth/token",
    headers=headers,
    params=params,
    timeout=30,
)
if response.status_code == 200:
    token = response.json()["access_token"]
else:
    raise Exception(f"Failed to generate access token: {response.status_code}")

match_ids = [
    "15728f46-ef22-4f18-81f0-9fad76502087",
    "00d48107-5e16-4596-9931-d272c4c9ce67",
    "26b2106f-b152-4251-aeee-19f1766807bf",
    "1fe55afa-67da-4772-9748-c4ec0d183f2b",
    "93529fbc-0a8c-43c9-bb83-05d01e350035",
    "9f4293af-22c6-41bd-9d2c-e46a5e656796",
    "259ae982-78e6-47c8-a3a7-c397b1e22a91",
    "20182803-ae68-4d66-ac7d-49853dffe597",
    "9a7e79fc-23e8-4ed4-8320-74b2f5617d52",
    "c6afbb1c-7c4e-465d-93f0-49992c7ee1f4",
    "5ffda560-6e22-425f-a8b0-873438c03c53",
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
