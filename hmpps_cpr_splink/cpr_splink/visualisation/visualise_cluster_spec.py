from copy import deepcopy
from typing import Any

# see https://www.robinlinacre.com/microblog/#different-ways-of-setting-out-data-in-a-vega-force-directed-layout
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
        {
            "description": "State variable for active node fix status.",
            "name": "fix",
            "value": False,
            "on": [
                {
                    "events": "symbol:pointerout[!event.buttons], window:pointerup",
                    "update": "false",
                },
                {"events": "symbol:pointerover", "update": "fix || true"},
                {
                    "events": "[symbol:pointerdown, window:pointerup] > window:pointermove!",
                    "update": "xy()",
                    "force": True,
                },
            ],
        },
        {
            "description": "Graph node most recently interacted with.",
            "name": "node",
            "value": None,
            "on": [
                {
                    "events": "symbol:pointerover",
                    "update": "fix === true ? item() : node",
                }
            ],
        },
        {
            "description": "Flag to restart force simulation when drag state changes.",
            "name": "restart",
            "value": False,
            "on": [{"events": {"signal": "fix"}, "update": "fix && fix.length"}],
        },
    ],
    "data": [
        {"name": "node-data", "values": [{"source_system": 1, "match_id": "a"}, {"source_system": 1, "match_id": "b"}]},
        {"name": "link-data", "values": [{"source": "a", "target": "b", "weight": 50.784104988004216}]},
    ],
    "scales": [
        {
            "name": "color",
            "type": "ordinal",
            "domain": {"data": "node-data", "field": "source_system"},
            "range": {"scheme": "category10"},
        }
    ],
    "marks": [
        {
            "name": "nodes",
            "type": "symbol",
            "zindex": 1,
            "from": {"data": "node-data"},
            "on": [
                {
                    "trigger": "fix",
                    "modify": "node",
                    "values": "fix === true ? {fx: node.x, fy: node.y} : {fx: fix[0], fy: fix[1]}",
                },
                {"trigger": "!fix", "modify": "node", "values": "{fx: null, fy: null}"},
            ],
            "encode": {
                "enter": {"fill": {"scale": "color", "field": "source_system"}, "stroke": {"value": "white"}},
                "update": {"size": {"signal": "2 * $nodeRadius * $nodeRadius"}, "cursor": {"value": "pointer"}},
            },
            "transform": [
                {
                    "type": "force",
                    "iterations": 300,
                    "restart": {"signal": "restart"},
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
