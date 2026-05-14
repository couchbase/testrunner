import logger

log = logger.Logger.get_logger()


class CNGClusterInfra:
    """Tracks CNG + HAProxy resources standing in front of one cluster."""

    def __init__(self, cluster, lb_server, haproxy_helper, cng_helpers,
                 node_ips):
        self.cluster = cluster
        self.lb_server = lb_server
        self.haproxy_helper = haproxy_helper
        self.cng_helpers = cng_helpers
        self.node_ips = node_ips

    @property
    def lb_ip(self):
        return self.lb_server.ip


class CNGInfraRegistry:
    """Shared mutable state for CNG infrastructure across managers.

    Owns both the per-cluster infra dict and the load-balancer floating-server
    reservation pool. No manager reads or mutates the underlying lists
    directly — every access goes through this registry.
    """

    def __init__(self, floating_server_pool):
        self._infras = {}
        self._pool = floating_server_pool

    def _dedupe_pool(self):
        """In-place dedup of the shared floating-server pool by IP.

        `XDCRNewBaseTest.__set_free_servers` re-appends every spare INI
        server on every setUp without checking for membership, so the
        underlying `FloatingServers._serverlist` accretes duplicates
        across tests in the same Python process. Without this, a single
        physical floating server can be popped as LB twice in one ring
        bootstrap — two clusters share an LB IP, HAProxy on the second
        attempt clobbers the first, and target identity gets crossed.
        """
        seen = set()
        deduped = []
        for s in self._pool:
            if s.ip in seen:
                continue
            seen.add(s.ip)
            deduped.append(s)
        if len(deduped) != len(self._pool):
            log.warning(
                "Floating pool deduped from {0} to {1} entries".format(
                    len(self._pool), len(deduped)))
            self._pool[:] = deduped

    def reserve_lb_server(self):
        self._dedupe_pool()
        if not self._pool:
            raise Exception("No floating servers available for load balancer.")
        server = self._pool.pop(0)
        log.info("Reserved {0} as load balancer".format(server.ip))
        return server

    def return_lb_server(self, server):
        if server and not any(s.ip == server.ip for s in self._pool):
            self._pool.append(server)

    def register(self, name, infra):
        self._infras[name] = infra

    def get(self, name):
        return self._infras.get(name)

    def items(self):
        return list(self._infras.items())

    def pop(self, name):
        return self._infras.pop(name, None)

    def __contains__(self, name):
        return name in self._infras

    def __len__(self):
        return len(self._infras)
