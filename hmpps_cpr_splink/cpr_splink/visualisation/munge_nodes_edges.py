from typing import Any

from splink import SettingsCreator
from splink.internals.settings import Settings
from splink.internals.waterfall_chart import records_to_waterfall_data

from hmpps_cpr_splink.cpr_splink.model.score import MODEL_PATH
from hmpps_cpr_splink.cpr_splink.visualisation.visualise_cluster_spec import load_base_spec


def _add_waterfall_data(edges: list[dict[str, Any]], settings: Settings) -> None:
    waterfall_data = records_to_waterfall_data(edges, settings, hide_details=False)

    waterfall_data_by_record_number: dict = {}
    for rec in waterfall_data:
        waterfall_data_by_record_number.setdefault(rec["record_number"], []).append(rec)

    for idx, e in enumerate(edges):
        record_waterfall = waterfall_data_by_record_number.get(idx, [])

        unaltered = e["unaltered_match_weight"]  # Unaltered score is without twins adjustment
        actual = e["match_weight"]  # Post twins adjustment
        twins_adjustment = actual - unaltered
        detector_fired = e["possible_twins"]

        final_score_entry = None
        final_score_sort_order = 0
        for entry in record_waterfall:
            if entry["column_name"] == "Final score":
                final_score_entry = entry
                final_score_sort_order = entry["bar_sort_order"]
                break

        # Only add twins adjustment bar if there's an actual adjustment
        if twins_adjustment != 0:
            # Insert twins adjustment bar before Final score
            twins_bar = {
                "column_name": "twins_adjustment",
                "label_for_charts": "Twins adjustment",
                "sql_condition": None,
                "log2_bayes_factor": twins_adjustment,
                "bayes_factor": 2**twins_adjustment,
                "comparison_vector_value": None,
                "m_probability": None,
                "u_probability": None,
                "bayes_factor_description": "Score adjusted for possible twins",
                "value_l": str(detector_fired),
                "value_r": str(detector_fired),
                "term_frequency_adjustment": False,
                "bar_sort_order": final_score_sort_order,  # Takes Final score's original position
                "record_number": idx,
            }

            record_waterfall.append(twins_bar)

            # Update Final score: increment sort order and update value to actual match_weight
            if final_score_entry:
                final_score_entry["bar_sort_order"] = final_score_sort_order + 1
                final_score_entry["log2_bayes_factor"] = actual

        e["waterfall_data"] = record_waterfall


def build_spec(nodes: list[dict[str, Any]], edges: list[dict[str, Any]]) -> dict[str, Any]:
    spec = load_base_spec()

    settings = SettingsCreator.from_path_or_dict(MODEL_PATH).get_settings("duckdb")

    filtered_edges = []
    filtered_nodes = []

    for e in edges:
        e["waterfall_data"] = []

    # Above 30, the number of edges gets very large and performance degrades
    if len(nodes) <= 30:
        _add_waterfall_data(edges, settings)

    edge_drop_prefixes = ("bf_", "tf_", "gamma_")

    for edge in edges:
        filtered_edge = {key: value for key, value in edge.items() if not key.startswith(edge_drop_prefixes)}
        filtered_edge["source"] = edge["match_id_l"]
        filtered_edge["target"] = edge["match_id_r"]
        filtered_edge["match_weight"] = float(filtered_edge["match_weight"])
        filtered_edges.append(filtered_edge)

    for node in nodes:
        filtered_node = {key: value for key, value in node.items() if not key.startswith("tf_")}
        filtered_nodes.append(filtered_node)

    spec["data"] = [
        {
            "name": "link-data",
            "values": filtered_edges,
            "transform": [
                {
                    "type": "filter",
                    "expr": "datum.match_weight >= scoreThreshold",
                },
            ],
        },
        {"name": "node-data", "values": filtered_nodes},
    ]

    return spec
