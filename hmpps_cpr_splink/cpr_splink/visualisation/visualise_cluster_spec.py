from copy import deepcopy
from typing import Any

BASE_SPEC: dict[str, Any] = {
    "$schema": "https://vega.github.io/schema/vega/v6.json",
    "width": 500,
    "height": 500,
    "signals": [
        {"name": "$cx", "update": "width / 2"},
        {"name": "$cy", "update": "height / 2"},
        {"name": "$nodeRadius", "value": 8},
        {"name": "$nodeCharge", "value": -15},
        {"name": "$linkDistance", "value": 90},
        {"name": "$static", "value": False},
    ],
    "data": [
        {"name": "node-data", "values": [{"group": 1, "match_id": "a"}, {"group": 1, "match_id": "b"}]},
        {"name": "link-data", "values": [{"source": "a", "target": "b", "weight": 50.784104988004216}]},
    ],
    "scales": [
        {
            "name": "color",
            "type": "ordinal",
            "domain": {"data": "node-data", "field": "group"},
            "range": {"scheme": "category20"},
        }
    ],
    "marks": [
        {
            "name": "nodes",
            "type": "symbol",
            "zindex": 1,
            "from": {"data": "node-data"},
            "encode": {
                "enter": {"fill": {"scale": "color", "field": "group"}, "stroke": {"value": "white"}},
                "update": {"size": {"signal": "2 * $nodeRadius * $nodeRadius"}, "cursor": {"value": "pointer"}},
            },
            "transform": [
                {
                    "type": "force",
                    "iterations": 300,
                    "static": {"signal": "$static"},
                    "signal": "force",
                    "forces": [
                        {"force": "center", "x": {"signal": "$cx"}, "y": {"signal": "$cy"}},
                        {"force": "collide", "radius": {"signal": "$nodeRadius"}},
                        {"force": "nbody", "strength": {"signal": "$nodeCharge"}},
                        {
                            "force": "link",
                            "id": "datum.match_id",
                            "links": "link-data",
                            "distance": {"signal": "$linkDistance"},
                        },
                    ],
                }
            ],
        },
        {
            "type": "path",
            "from": {"data": "link-data"},
            "interactive": False,
            "encode": {"update": {"stroke": {"value": "#ccc"}, "strokeWidth": {"value": 2}}},
            "transform": [
                {
                    "type": "linkpath",
                    "require": {"signal": "force"},
                    "shape": "line",
                    "sourceX": "datum.source.x",
                    "sourceY": "datum.source.y",
                    "targetX": "datum.target.x",
                    "targetY": "datum.target.y",
                }
            ],
        },
    ],
}


def load_base_spec() -> dict[str, Any]:
    """Return a deep copy of the base Vega spec so callers can mutate safely."""

    return deepcopy(BASE_SPEC)
