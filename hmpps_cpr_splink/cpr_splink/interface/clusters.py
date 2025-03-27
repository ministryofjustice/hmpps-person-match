from dataclasses import dataclass


@dataclass
class Clusters:
    clusters_groupings: list[list[str]]

    @property
    def is_single_cluster(self):
        return len(self.clusters_groupings) == 1
