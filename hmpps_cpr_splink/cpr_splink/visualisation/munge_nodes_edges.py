from hmpps_cpr_splink.cpr_splink.visualisation.visualise_cluster_spec import load_base_spec


def build_spec(nodes, edges):
    spec = load_base_spec()

    new_nodes = []

    for i, n in enumerate(nodes):
        nn = {}

        nn["source_system"] = n["source_system"]
        nn["match_id"] = n["match_id"]
        new_nodes.append(nn)

    new_edges = []
    for e in edges:
        ee = {}

        ee["source"] = e["match_id_l"]
        ee["target"] = e["match_id_r"]
        ee["weight"] = float(e["match_weight"])
        new_edges.append(ee)

    spec["data"] = [
        {
            "name": "link-data",
            "values": new_edges,
        },
        {"name": "node-data", "values": new_nodes},
    ]

    return spec
