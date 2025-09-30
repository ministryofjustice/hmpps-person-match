import json
from pathlib import Path
from typing import Any

MODEL_PATH = Path(__file__).parent / "specs/force_directed.json"

NODE_TOOLTIP_FIELDS = [
    "id",
    "match_id",
    "source_system",
    "source_system_id",
    "name_1_std",
    "name_2_std",
    "name_3_std",
    "last_name_std",
    "first_and_last_name_std",
    "date_of_birth",
    "cro_single",
    "pnc_single",
]

EDGE_COMPARISON_FIELDS = [
    "id",
    "match_id",
    "source_system",
    "name_1_std",
    "name_2_std",
    "name_3_std",
    "last_name_std",
    "first_and_last_name_std",
    "date_of_birth",
    "cro_single",
    "pnc_single",
]

NODE_TOOLTIP_SIGNAL = "{" + ", ".join(f'"{field}": datum.{field}' for field in NODE_TOOLTIP_FIELDS) + "}"

EDGE_TOOLTIP_SIGNAL = (
    "{"
    + "match_weight: datum.match_weight, "
    + ", ".join(f'"{field}": datum.{field}_l + " <-> " + datum.{field}_r' for field in EDGE_COMPARISON_FIELDS)
    + "}"
)


def load_base_spec() -> dict[str, Any]:
    """Return a deep copy of the base Vega spec so callers can mutate safely."""

    # see https://www.robinlinacre.com/microblog/#different-ways-of-setting-out-data-in-a-vega-force-directed-layout
    with open(MODEL_PATH) as f:
        base_spec = json.load(f)

    base_spec["marks"][0]["encode"]["update"]["tooltip"]["signal"] = NODE_TOOLTIP_SIGNAL
    base_spec["marks"][3]["encode"]["update"]["tooltip"]["signal"] = EDGE_TOOLTIP_SIGNAL

    return base_spec
