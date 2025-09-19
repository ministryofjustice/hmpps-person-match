"""Utility script to call the /visualise-cluster endpoint and render the Vega chart."""

from __future__ import annotations

import base64

import requests
from IPython.display import display


from integration.client import Client

API_ROOT = "http://localhost:5001"
AUTH_ROOT = "http://localhost:8081"
CLIENT = Client.HMPPS_PERSON_MATCH
CLIENT_SECRET = "clientsecret"  # noqa: S105

MATCH_IDS = [
    "63eb8ba5-6cad-4e2c-99a7-546dca9ff6c9",
]


def render_vega_inline(spec: dict):
    display({"application/vnd.vega.v5+json": spec}, raw=True)


def _get_token() -> str:
    encoded = base64.b64encode(f"{CLIENT.value}:{CLIENT_SECRET}".encode()).decode("utf-8")
    headers = {"Authorization": f"Basic {encoded}"}
    params = {"grant_type": "client_credentials"}
    response = requests.post(
        f"{AUTH_ROOT}/auth/oauth/token",
        headers=headers,
        params=params,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()["access_token"]


token = _get_token()
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
}
response = requests.post(
    f"{API_ROOT}/visualise-cluster",
    headers=headers,
    json=MATCH_IDS,
    timeout=60,
)
response.raise_for_status()
payload = response.json()
print("Status:", response.status_code)
print("Payload:", payload)
# Only render when a spec is returned by the API
if "spec" in payload:
    print("Rendering Vega spec...")
    render_vega_inline(payload["spec"])
