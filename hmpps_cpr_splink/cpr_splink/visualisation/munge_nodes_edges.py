from splink import SettingsCreator
from splink.internals.waterfall_chart import record_to_waterfall_data

from hmpps_cpr_splink.cpr_splink.model.score import MODEL_PATH
from hmpps_cpr_splink.cpr_splink.visualisation.visualise_cluster_spec import load_base_spec


def build_spec(nodes, edges):
    spec = load_base_spec()

    settings = SettingsCreator.from_path_or_dict(MODEL_PATH).get_settings("duckdb")

    filtered_edges = []
    filtered_nodes = []

    for e in edges:
        e["waterfall_data"] = record_to_waterfall_data(edges[0], settings, hide_details=False)

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
        {
            "name": "hoverCols",
            "values": [
                {
                    "key": "source_system",
                    "fieldL": "source_system_l",
                    "fieldR": "source_system_r",
                    "fieldN": "source_system",
                },
                {"key": "name_1_std", "fieldL": "name_1_std_l", "fieldR": "name_1_std_r", "fieldN": "name_1_std"},
                {"key": "name_2_std", "fieldL": "name_2_std_l", "fieldR": "name_2_std_r", "fieldN": "name_2_std"},
                {"key": "name_3_std", "fieldL": "name_3_std_l", "fieldR": "name_3_std_r", "fieldN": "name_3_std"},
                {
                    "key": "last_name_std",
                    "fieldL": "last_name_std_l",
                    "fieldR": "last_name_std_r",
                    "fieldN": "last_name_std",
                },
                {
                    "key": "date_of_birth_arr",
                    "fieldL": "date_of_birth_arr_l",
                    "fieldR": "date_of_birth_arr_r",
                    "fieldN": "date_of_birth_arr",
                },
                {"key": "cro_single", "fieldL": "cro_single_l", "fieldR": "cro_single_r", "fieldN": "cro_single"},
                {"key": "pnc_single", "fieldL": "pnc_single_l", "fieldR": "pnc_single_r", "fieldN": "pnc_single"},
                {
                    "key": "postcode_arr",
                    "fieldL": "postcode_arr_l",
                    "fieldR": "postcode_arr_r",
                    "fieldN": "postcode_arr",
                },
            ],
            "transform": [
                {"type": "window", "ops": ["row_number"], "as": ["idx"]},
                {"type": "formula", "as": "colIdx", "expr": "datum.idx - 1"},
                {
                    "type": "formula",
                    "as": "valL",
                    "expr": "hoverLink ? (isValid(hoverLink[datum.fieldL]) ? hoverLink[datum.fieldL] : '') : (hoverNode ? (isValid(hoverNode[datum.fieldN]) ? hoverNode[datum.fieldN] : '') : '')",
                },
                {
                    "type": "formula",
                    "as": "valR",
                    "expr": "hoverLink && isValid(hoverLink[datum.fieldR]) ? hoverLink[datum.fieldR] : ''",
                },
            ],
        },
    ]

    return spec
