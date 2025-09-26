from hmpps_cpr_splink.cpr_splink.visualisation.visualise_cluster_spec import load_base_spec


def build_spec(nodes, edges):
    spec = load_base_spec()

    edge_drop_prefixes = ("bf_", "tf_", "gamma_")
    filtered_edges = []
    filtered_nodes = []

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
                {"key": "source_system", "fieldL": "source_system_l", "fieldR": "source_system_r"},
                {"key": "name_1_std", "fieldL": "name_1_std_l", "fieldR": "name_1_std_r"},
                {"key": "name_2_std", "fieldL": "name_2_std_l", "fieldR": "name_2_std_r"},
                {"key": "name_3_std", "fieldL": "name_3_std_l", "fieldR": "name_3_std_r"},
                {"key": "last_name_std", "fieldL": "last_name_std_l", "fieldR": "last_name_std_r"},
                {"key": "date_of_birth", "fieldL": "date_of_birth_l", "fieldR": "date_of_birth_r"},
                {"key": "cro_single", "fieldL": "cro_single_l", "fieldR": "cro_single_r"},
                {"key": "pnc_single", "fieldL": "pnc_single_l", "fieldR": "pnc_single_r"},
            ],
            "transform": [
                {"type": "window", "ops": ["row_number"], "as": ["idx"]},
                {"type": "formula", "as": "colIdx", "expr": "datum.idx - 1"},
                {
                    "type": "formula",
                    "as": "valL",
                    "expr": "hoverLink && isValid(hoverLink[datum.fieldL]) ? hoverLink[datum.fieldL] : ''",
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
