from __future__ import annotations

import base64
import json
import os
import html

import requests

from integration.client import Client

API_ROOT = "http://localhost:5000"
AUTH_ROOT = "http://localhost:9090"
CLIENT = Client.HMPPS_PERSON_MATCH
CLIENT_SECRET = "clientsecret"  # noqa: S105

MATCH_IDS = [
    "63eb8ba5-6cad-4e2c-99a7-546dca9ff6c9",
    "31c32969-31fb-4bd1-9e65-861e41bfbb1e",
    "aec35b98-6527-4e9d-8eb8-bd82bb8b0132",
    "e655d939-bb26-493f-8ff4-82d29ca1ec8d",
]


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


def save_vega_html(
    spec: dict,
    out_html: str = "vega_view.html",
    title: str = "Vega View",
    renderer: str = "canvas",  # 'canvas' or 'svg'
) -> str:
    """
    Write a plain HTML file that renders a Vega spec using vega-interpreter
    (no vega-embed). Matches the PR's pattern:
      - vega.parse(spec, null, { ast: true })
      - new vega.View(..., { expr: vega.expressionInterpreter, ... }).runAsync()
    """

    spec_json = json.dumps(spec, ensure_ascii=False)

    html_str = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>{html.escape(title)}</title>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <script src="https://cdn.jsdelivr.net/npm/vega@6"{nonce_attr}></script>
  <script src="https://cdn.jsdelivr.net/npm/vega-interpreter@1"{nonce_attr}></script>
  <style>
    :root {{ color-scheme: light dark; }}
    body {{ margin: 0; padding: 1rem; font-family: system-ui, sans-serif; }}
    #vis {{ max-width: 100%; overflow: auto; }}
  </style>
</head>
<body>
  <div id="vis"></div>
  <script>
    // Inline spec (parsed with AST for the interpreter)
    const spec = {spec_json};

    // Parse to runtime with AST enabled
    const runtime = vega.parse(spec, null, {{ ast: true }});

    // Create a View that uses the interpreter for expressions
    const view = new vega.View(runtime, {{
      expr:      vega.expressionInterpreter,
      renderer:  {json.dumps(renderer)},
      container: '#vis',
      hover:     true
    }});

    // Run and surface any errors
    view.runAsync().catch(console.error);
  </script>
</body>
</html>"""

    with open(out_html, "w", encoding="utf-8") as f:
        f.write(html_str)
    return os.path.abspath(out_html)


print("Status:", response.status_code)
print("Payload:", payload)


with open("vega_spec.json", "w", encoding="utf-8") as f:
    json.dump(payload["spec"], f, indent=2, ensure_ascii=False)
save_vega_html(payload["spec"], out_html="vega_view.html", title="Vega View")
