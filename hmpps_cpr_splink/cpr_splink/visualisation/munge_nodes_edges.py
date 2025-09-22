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
    ]

    return spec
