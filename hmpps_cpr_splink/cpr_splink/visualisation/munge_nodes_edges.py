from hmpps_cpr_splink.cpr_splink.visualisation.visualise_cluster_spec import load_base_spec


def build_spec(nodes, edges):
    spec = load_base_spec()

    new_nodes = []
    # for n in nodes:
    #     n["id"] = n["match_id"]
    for i, n in enumerate(nodes):
        nn = {}
        nn["id"] = n["match_id"]
        nn["index"] = i
        nn["group"] = 1
        new_nodes.append(nn)

    new_edges = []
    for e in edges:
        # n["source"] = n["match_id_l"]
        # n["target"] = n["match_id_r"]
        # n["value"] = float(n["match_weight"])
        ee = {}
        ee["source"] = 0
        ee["target"] = 1
        ee["weight"] = float(e["match_weight"])
        new_edges.append(ee)

    spec["data"] = [
        {
            "name": "node-data",
            "values": new_nodes,
        },
        {
            "name": "link-data",
            "values": new_edges,
        },
    ]

    return spec
