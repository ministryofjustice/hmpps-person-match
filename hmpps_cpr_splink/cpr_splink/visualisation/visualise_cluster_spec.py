from copy import deepcopy
from typing import Any

BASE_SPEC: dict[str, Any] = {
    "$schema": "https://vega.github.io/schema/vega/v6.json",
    "description": "A node-link diagram",
    "width": 700,
    "height": 500,
    "padding": 0,
    "autosize": "none",
    "signals": [
        {
            "name": "cx",
            "update": "width / 2",
        },
        {
            "name": "cy",
            "update": "height / 2",
        },
        {
            "name": "nodeRadius",
            "value": 22,
        },
        {
            "name": "nodeCharge",
            "value": -30,
        },
        {
            "name": "linkDistance",
            "value": 100,
        },
        {
            "name": "static",
            "value": False,
        },
        {
            "name": "scoreThreshold",
            "value": 0,
            "bind": {
                "input": "range",
                "min": 0,
                "max": 30,
                "step": 1,
                "name": "Score Threshold:",
            },
        },
        {
            "description": "State variable for active node fix status.",
            "name": "fix",
            "value": False,
            "on": [
                {
                    "events": "symbol:pointerout[!event.buttons], window:pointerup",
                    "update": "false",
                },
                {
                    "events": "symbol:pointerover",
                    "update": "fix || true",
                },
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
                },
            ],
        },
        {
            "name": "restart",
            "value": 0,
            "on": [
                {
                    "events": {
                        "signal": "fix",
                    },
                    "update": "restart + 1",
                },
                {
                    "events": {
                        "signal": "scoreThreshold",
                    },
                    "update": "restart + 1",
                },
            ],
        },
        {
            "name": "clickedLink",
            "value": None,
            "on": [
                {
                    "events": "path:click",
                    "update": "item()",
                },
            ],
        },
    ],
    "data": [],
    "scales": [
        {
            "name": "color",
            "type": "ordinal",
            "domain": {
                "data": "node-data",
                "field": "group",
            },
            "range": {
                "scheme": "category10",
            },
        },
        {
            "name": "linkColor",
            "type": "threshold",
            "domain": [
                18,
                24,
            ],
            "range": [
                "red",
                "orange",
                "green",
            ],
        },
    ],
    "marks": [
        {
            "name": "nodes",
            "type": "symbol",
            "zindex": 1,
            "from": {
                "data": "node-data",
            },
            "on": [
                {
                    "trigger": "fix",
                    "modify": "node",
                    "values": "fix === true ? {fx: node.x, fy: node.y} : {fx: fix[0], fy: fix[1]}",
                },
                {
                    "trigger": "!fix",
                    "modify": "node",
                    "values": "{fx: null, fy: null}",
                },
            ],
            "encode": {
                "enter": {
                    "fill": {
                        "signal": "datum.group === 0 ? '#808080' : scale('color', datum.group)",
                    },
                    "stroke": {
                        "value": "white",
                    },
                },
                "update": {
                    "size": {
                        "signal": "2 * nodeRadius * nodeRadius",
                    },
                    "cursor": {
                        "value": "pointer",
                    },
                    "tooltip": {
                        "signal": "datum.tooltip",
                    },
                },
            },
            "transform": [
                {
                    "type": "force",
                    "iterations": 300,
                    "restart": {
                        "signal": "restart",
                    },
                    "static": {
                        "signal": "static",
                    },
                    "signal": "force",
                    "forces": [
                        {
                            "force": "center",
                            "x": {
                                "signal": "cx",
                            },
                            "y": {
                                "signal": "cy",
                            },
                        },
                        {
                            "force": "collide",
                            "radius": {
                                "signal": "nodeRadius",
                            },
                        },
                        {
                            "force": "nbody",
                            "strength": {
                                "signal": "nodeCharge",
                            },
                        },
                        {
                            "force": "link",
                            "links": "link-data",
                            "distance": {
                                "signal": "linkDistance",
                            },
                        },
                    ],
                },
            ],
        },
        {
            "type": "path",
            "from": {
                "data": "link-data",
            },
            "interactive": True,
            "encode": {
                "enter": {
                    "stroke": {
                        "value": "#ccc",
                    },
                    "strokeWidth": {
                        "value": 3,
                    },
                },
                "update": {
                    "stroke": {
                        "scale": "linkColor",
                        "field": "value",
                    },
                    "tooltip": {
                        "signal": "datum.tooltip",
                    },
                },
                "hover": {
                    "stroke": {
                        "value": "darkgreen",
                    },
                },
            },
            "transform": [
                {
                    "type": "linkpath",
                    "require": {
                        "signal": "force",
                    },
                    "shape": "line",
                    "sourceX": "datum.source.x",
                    "sourceY": "datum.source.y",
                    "targetX": "datum.target.x",
                    "targetY": "datum.target.y",
                },
            ],
        },
        {
            "type": "text",
            "from": {
                "data": "nodes",
            },
            "zindex": 2,
            "encode": {
                "enter": {
                    "align": {
                        "value": "center",
                    },
                    "baseline": {
                        "value": "middle",
                    },
                    "fontSize": {
                        "value": 12,
                    },
                    "fontWeight": {
                        "value": "bold",
                    },
                    "fill": {
                        "value": "white",
                    },
                    "text": {
                        "field": "datum.label",
                    },
                    "x": {
                        "field": "x",
                    },
                    "y": {
                        "field": "y",
                    },
                },
                "update": {
                    "x": {
                        "field": "x",
                    },
                    "y": {
                        "field": "y",
                    },
                },
            },
        },
    ],
}


def load_base_spec() -> dict[str, Any]:
    """Return a deep copy of the base Vega spec so callers can mutate safely."""

    return deepcopy(BASE_SPEC)
