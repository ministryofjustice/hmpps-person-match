"""Utility script to call the /visualise-cluster endpoint and render the Vega chart."""

from __future__ import annotations
import os
import base64

import requests
from IPython.display import display


from integration.client import Client
import json
from pprint import pformat

API_ROOT = "http://localhost:5001"
AUTH_ROOT = "http://localhost:8081"
CLIENT = Client.HMPPS_PERSON_MATCH
CLIENT_SECRET = "clientsecret"  # noqa: S105

MATCH_IDS = [
    "63eb8ba5-6cad-4e2c-99a7-546dca9ff6c9",
    "31c32969-31fb-4bd1-9e65-861e41bfbb1e",
    "f716ef04-4182-427b-92f5-c25140724169",
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


def save_vega_html(spec: dict, out_html: str = "vega_view.html", title: str = "Vega View", actions=True) -> str:
    html = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>{title}</title>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <script src="https://cdn.jsdelivr.net/npm/vega@6"></script>
  <script src="https://cdn.jsdelivr.net/npm/vega-embed@6"></script>
  <style>body {{ margin: 0; padding: 1rem; font-family: system-ui, sans-serif; }}</style>
</head>
<body>
  <div id="vis"></div>
  <script>
    const spec = {json.dumps(spec)};
    vegaEmbed("#vis", spec, {{ mode: "vega", actions: {str(actions).lower()} }}).catch(console.error);
  </script>
</body>
</html>"""
    with open(out_html, "w", encoding="utf-8") as f:
        f.write(html)
    return os.path.abspath(out_html)


print("Status:", response.status_code)
print("Payload:", payload)
try:
    pretty = json.dumps(payload, indent=2, ensure_ascii=False)
except (TypeError, ValueError):
    pretty = pformat(payload)
print("Pretty payload:\n" + pretty)
if "spec" in payload:
    render_vega_inline(payload["spec"])
with open("vega_spec.json", "w", encoding="utf-8") as f:
    json.dump(payload["spec"], f, indent=2, ensure_ascii=False)
save_vega_html(payload["spec"], out_html="vega_view.html", title="Vega View", actions=True)
