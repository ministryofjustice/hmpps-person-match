from hmpps_cpr_splink.cpr_splink.visualisation.visualise_cluster_spec import load_base_spec


def build_spec(nodes, edges):
    spec = load_base_spec()

    for n in nodes:
        n["id"] = n["match_id"]

    for n in edges:
        n["source"] = n["match_id_l"]
        n["target"] = n["match_id_r"]

    spec["data"] = [
        {
            "name": "node-data",
            "values": nodes,
        },
        {
            "name": "link-data",
            "values": edges,
            "transform": [
                {
                    "type": "filter",
                    "expr": "datum.value >= scoreThreshold",
                },
            ],
        },
    ]

    return spec
