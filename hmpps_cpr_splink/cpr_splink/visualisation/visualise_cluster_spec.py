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
        {"name": "$nodeRadius", "value": 22},
        {"name": "$nodeCharge", "value": -15},
        {"name": "$linkDistance", "value": 140},
        {"name": "$static", "value": False},
        {
            "name": "scoreThreshold",
            "value": 18,
            "bind": {
                "input": "range",
                "min": -60,
                "max": 60,
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
            "value": 0,
            "on": [
                {"events": {"signal": "fix"}, "update": "restart + 1"},
                {"events": {"signal": "scoreThreshold"}, "update": "restart + 1"},
            ],
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
        },
        {"name": "linkColor", "type": "threshold", "domain": [18, 24], "range": ["#fb9191", "#fdbe7a", "#9fff9f"]},
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
                "update": {
                    "size": {"signal": "2 * $nodeRadius * $nodeRadius"},
                    "cursor": {"value": "pointer"},
                    "tooltip": {
                        "signal": '{"id": datum.id, "match_id": datum.match_id, "source_system": datum.source_system, "source_system_id": datum.source_system_id, "name_1_std": datum.name_1_std, "name_2_std": datum.name_2_std, "name_3_std": datum.name_3_std, "last_name_std": datum.last_name_std, "first_and_last_name_std": datum.first_and_last_name_std, "date_of_birth": datum.date_of_birth, "cro_single": datum.cro_single, "pnc_single": datum.pnc_single}'
                    },
                },
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
            "type": "text",
            "name": "node-labels",
            "from": {"data": "nodes"},
            "interactive": False,
            "zindex": 2,
            "encode": {
                "enter": {
                    "align": {"value": "center"},
                    "baseline": {"value": "middle"},
                    "fontSize": {"value": 12},
                    "fontWeight": {"value": "bold"},
                    "fill": {"value": "white"},
                    "text": {"signal": "slice(datum.datum.match_id, 0, 4)"},
                },
                "update": {
                    "x": {"field": "x"},
                    "y": {"field": "y"},
                },
            },
        },
        {
            "type": "text",
            "name": "link-labels",
            "from": {"data": "link-data"},
            "interactive": False,
            "zindex": 1,
            "encode": {
                "enter": {
                    "align": {"value": "center"},
                    "baseline": {"value": "middle"},
                    "fontSize": {"value": 12},
                    "fill": {"value": "black"},
                    "fontWeight": {"value": "bold"},
                },
                "update": {
                    "text": {"signal": "format(datum.match_weight, '.1f')"},
                    "x": {"signal": "(datum.source.x + datum.target.x) / 2"},
                    "y": {"signal": "(datum.source.y + datum.target.y) / 2"},
                },
            },
        },
        {
            "type": "path",
            "from": {"data": "link-data"},
            "interactive": True,
            "encode": {
                "update": {
                    "stroke": {"scale": "linkColor", "field": "match_weight"},
                    "strokeWidth": {"value": 3},
                    "tooltip": {
                        "signal": "{\"id\": datum.id_l + ' <-> ' + datum.id_r, \"match_id\": datum.match_id_l + ' <-> ' + datum.match_id_r, \"source_system\": datum.source_system_l + ' <-> ' + datum.source_system_r, \"source_system_id\": datum.source_system_id_l + ' <-> ' + datum.source_system_id_r, \"name_1_std\": datum.name_1_std_l + ' <-> ' + datum.name_1_std_r, \"name_2_std\": datum.name_2_std_l + ' <-> ' + datum.name_2_std_r, \"name_3_std\": datum.name_3_std_l + ' <-> ' + datum.name_3_std_r, \"last_name_std\": datum.last_name_std_l + ' <-> ' + datum.last_name_std_r, \"first_and_last_name_std\": datum.first_and_last_name_std_l + ' <-> ' + datum.first_and_last_name_std_r, \"date_of_birth\": datum.date_of_birth_l + ' <-> ' + datum.date_of_birth_r, \"cro_single\": datum.cro_single_l + ' <-> ' + datum.cro_single_r, \"pnc_single\": datum.pnc_single_l + ' <-> ' + datum.pnc_single_r}"
                    },
                }
            },
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
