class TopologyBuilder:
    """Construct CNG-fronted replication graphs between named clusters.

    Each build_* method:
      1. Bootstraps CNG infra in front of the clusters that need to be CNG
         targets (the head of a chain is never a target).
      2. Adds a CNG remote-ref for every edge (src, dst) and returns the
         list of (src, dst, rc_name) edges so the test can start reps.
    """

    def __init__(self, infra_manager, refs_manager, get_cb_cluster_by_name,
                 all_clusters_fn):
        self._infra = infra_manager
        self._refs = refs_manager
        self._get_cluster = get_cb_cluster_by_name
        self._all_clusters = all_clusters_fn

    def _bootstrap(self, target_names, multi_backend=False):
        all_clusters = self._all_clusters()
        targets = [self._get_cluster(n) for n in target_names]
        self._infra.bootstrap_targets(all_clusters, targets,
                                      multi_backend=multi_backend)

    def build_chain(self, cluster_names):
        """C1 -> CNG(C2) -> CNG(C3) ...; all non-head clusters are CNG targets."""
        self._bootstrap(cluster_names[1:])
        edges = []
        for i in range(len(cluster_names) - 1):
            src = self._get_cluster(cluster_names[i])
            dst = self._get_cluster(cluster_names[i + 1])
            edges.append(
                (src, dst, self._refs.add_cng_ref_through_registry(src, dst)))
        return edges

    def build_star(self, hub_name, spoke_names, bidirectional=False):
        """Hub -> CNG(spoke) for every spoke; plus spoke -> CNG(hub) if bidirectional."""
        targets = list(spoke_names) + ([hub_name] if bidirectional else [])
        self._bootstrap(targets)
        hub = self._get_cluster(hub_name)
        edges = []
        for spoke_name in spoke_names:
            spoke = self._get_cluster(spoke_name)
            edges.append(
                (hub, spoke, self._refs.add_cng_ref_through_registry(hub, spoke)))
            if bidirectional:
                edges.append(
                    (spoke, hub, self._refs.add_cng_ref_through_registry(spoke, hub)))
        return edges

    def build_ring(self, cluster_names):
        """C1 -> CNG(C2) -> CNG(C3) -> ... -> CNG(C1); every cluster is a target."""
        self._bootstrap(cluster_names)
        edges = []
        n = len(cluster_names)
        for i in range(n):
            src = self._get_cluster(cluster_names[i])
            dst = self._get_cluster(cluster_names[(i + 1) % n])
            edges.append(
                (src, dst, self._refs.add_cng_ref_through_registry(src, dst)))
        return edges

    def build_mesh(self, cluster_names):
        """Fully-connected bidirectional CNG mesh between every cluster pair."""
        self._bootstrap(cluster_names)
        edges = []
        for i, src_name in enumerate(cluster_names):
            for j, dst_name in enumerate(cluster_names):
                if i == j:
                    continue
                src = self._get_cluster(src_name)
                dst = self._get_cluster(dst_name)
                edges.append(
                    (src, dst, self._refs.add_cng_ref_through_registry(src, dst)))
        return edges
