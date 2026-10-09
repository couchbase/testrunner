"""``RunTarget`` -- the per-test facade for dual-run (on-prem + Capella) tests.

Attached to a test by composition in ``setUp`` (``self.target = RunTarget(self)``),
it owns the target's capabilities, the node-shell operations, and the *neutral*
topology / failover verbs. A test never branches on ``capella_run`` to decide how
to do something; it calls one neutral method and ``RunTarget`` dispatches:

    self.target.resize_cluster(remove=out, services=["index"]).result()
    if self.target.supports(Capability.NODE_SHELL):
        self.target.install_packages(["stress-ng"])

``resize_cluster`` drives an ns_server rebalance on-prem and the Capella scaling
API on Capella -- the underlying task is chosen here, not in the test.

N1QL follows the same rule: ``run_query`` targets a query node from the services
map on-prem, and the cluster connect string on Capella (no query-node lookup):

    self.target.run_query(query=q, server=self.target.query_node())
"""

# TODO: remove the temporary DUALRUN-FLOW logging once all components have tested the dual-run flow.

from lib.Cb_constants.CBServer import CbServer
from lib.testops.capabilities import Capability, UnsupportedOnTarget
from lib.testops.node_ops import make_node_ops
from lib.testops.query_ops import make_query_ops


class RunTarget(object):
    def __init__(self, test):
        self.test = test
        capella_run = getattr(test, "capella_run", None)
        if capella_run is None:
            capella_run = test.input.param("capella_run", CbServer.capella_run)
        self.capella_run = capella_run
        CbServer.capella_run = capella_run
        self.ssh_reachable = True if not capella_run else self._probe_ssh()
        self._node = make_node_ops(self)
        self._query = make_query_ops(self)
        self.capabilities = self._compute_caps()
        _log = getattr(test, "log", None)
        if _log:
            _log.info("DUALRUN-FLOW: RunTarget ready | capella_run=%s ssh_reachable=%s node_backend=%s "
                      "query_backend=%s caps=%s"
                      % (self.capella_run, self.ssh_reachable, type(self._node).__name__,
                         type(self._query).__name__, sorted(c.name for c in self.capabilities)))

    # --- capability pre-check (the one form tests use) ---
    def supports(self, capability):
        result = capability in self.capabilities
        _log = getattr(self.test, "log", None)
        if _log:
            _log.info("DUALRUN-FLOW: supports(%s) -> %s"
                      % (getattr(capability, "name", capability), result))
        return result

    # --- node-shell facade (delegates to the backend; backend raises if unsupported) ---
    def install_packages(self, packages, nodes=None):
        _log = getattr(self.test, "log", None)
        if _log:
            _log.info("DUALRUN-FLOW: install_packages via %s: %s"
                      % (type(self._node).__name__, packages))
        return self._node.install_packages(packages, nodes)

    def restart_service(self, service, node):
        return self._node.restart_service(service, node)

    def reboot(self, node):
        return self._node.reboot(node)

    def read_service_log(self, node, log_path, pattern):
        return self._node.read_service_log(node, log_path, pattern)

    def collect_logs(self, reason=None):
        return self._node.collect_logs(reason)

    # --- query facade (on-prem: a query node; Capella: the cluster connect string) ---
    def query_node(self):
        """The ``server`` to hand to query calls.

        On-prem this is a query node from the services map. On Capella it is ``None``
        and no node lookup is made: ``run_query`` goes through the connect string, so a
        scale operation that evicts a query node cannot strand the test.
        """
        return self._query.query_node()

    def run_query(self, query=None, server=None, **kwargs):
        """Run a N1QL statement; drop-in for ``n1ql_helper.run_cbq_query``.

        On Capella ``server`` (and ``database``) are ignored -- the statement is sent
        via the cluster connect string and the SDK picks a live query node.
        """
        return self._query.run_query(query=query, server=server, **kwargs)

    # --- neutral topology / failover verbs (dispatch by capella_run) ---
    def resize_cluster(self, add=None, remove=None, services=None):
        """Change cluster topology. On-prem this rebalances; on Capella it scales.

        Delegates to the existing ``Cluster.async_rebalance`` task manager, which
        already dispatches to a ``RebalanceTask`` on-prem and a
        ``CapellaRebalanceTask`` (the scaling API) when ``capella_run`` is set. The
        neutral verb lives here so tests never name "rebalance"/"scale", and no core
        file changes are needed.

        Returns a task/Future -- call ``.result()`` to block until it completes.
        """
        add = add or []
        remove = remove or []
        _log = getattr(self.test, "log", None)
        if _log:
            _path = "CAPELLA scale (CapellaRebalanceTask)" if self.capella_run else "ON-PREM rebalance (RebalanceTask)"
            _log.info("DUALRUN-FLOW: resize_cluster -> %s | add=%d remove=%d services=%s"
                      % (_path, len(add), len(remove), services))
        return self.test.cluster.async_rebalance(
            self.test.servers[:self.test.nodes_init], add, remove,
            services=services, cluster_config=getattr(self.test, "cluster_config", None))

    def refresh_cluster_nodes(self, prefer_service="kv"):
        """Re-sync the test's ``servers``, ``master`` and ``rest`` from the live cluster.

        On Capella a topology change only sends a target node *count*; the scale API
        auto-picks which physical node it adds or evicts. So the node a caller named for
        a scale-in is merely a count signal, and Capella may well evict the node the
        framework was using as ``master`` (the REST/N1QL endpoint) — or add a node the
        local ``servers`` list has never seen. Call this after every Capella scale so
        subsequent REST/N1QL never targets an evicted node and can see an added one.

        On-prem the caller controls the exact node (a non-master node), so the local
        view is already correct: this is a no-op and returns the current server list.

        Returns the refreshed server list (or the existing one on-prem / on failure).
        """
        _log = getattr(self.test, "log", None)
        if not self.capella_run:
            return getattr(self.test, "servers", None)
        try:
            from lib.capella.utils import CapellaAPI
            api = CapellaAPI(CbServer.capella_credentials)
            servers = api.get_nodes_formatted(
                CbServer.capella_cluster_id,
                CbServer.rest_username, CbServer.rest_password)
        except Exception as e:
            if _log:
                _log.warning("DUALRUN-FLOW: refresh_cluster_nodes -> failed to fetch live "
                             "nodes (%s); keeping existing server list" % e)
            return getattr(self.test, "servers", None)
        if not servers:
            if _log:
                _log.warning("DUALRUN-FLOW: refresh_cluster_nodes -> Capella returned no "
                             "nodes; keeping existing server list")
            return getattr(self.test, "servers", None)

        old_master_ip = getattr(getattr(self.test, "master", None), "ip", None)
        # Keep the existing master if it survived the scale; otherwise promote a
        # surviving node that carries the preferred service (kv) so REST/N1QL keep working.
        new_master = next((s for s in servers if s.ip == old_master_ip), None)
        if new_master is None:
            new_master = next((s for s in servers if prefer_service in (s.services or [])),
                              servers[0])
            if _log:
                _log.info("DUALRUN-FLOW: refresh_cluster_nodes -> master %s no longer in "
                          "cluster; promoting %s" % (old_master_ip, new_master.ip))

        cfg = getattr(self.test, "cluster_config", None)
        if cfg is not None and hasattr(cfg, "update_servers"):
            # Before touching self.test.servers: on Capella servers/master are backed by
            # cluster_config, and update_servers must see the OLD list to mark evicted
            # nodes dummy. It also resets master to servers[0], so master is set after.
            try:
                cfg.update_servers(servers)
            except Exception as e:
                if _log:
                    _log.warning("DUALRUN-FLOW: refresh_cluster_nodes -> update_servers "
                                 "failed (%s)" % e)
        self.test.servers = servers
        self.test.master = new_master
        # self.rest is cached against the old master; re-point it at the live one so
        # helpers that use it (e.g. get_nodes_from_services_map) don't hit a dead node.
        try:
            from membase.api.rest_client import RestConnection
            self.test.rest = RestConnection(new_master)
        except Exception as e:
            if _log:
                _log.warning("DUALRUN-FLOW: refresh_cluster_nodes -> could not rebuild "
                             "RestConnection on %s (%s)" % (new_master.ip, e))
        if _log:
            _log.info("DUALRUN-FLOW: refresh_cluster_nodes -> %d live node(s), master=%s"
                      % (len(servers), new_master.ip))
        return servers

    def node_to_add(self, services=None):
        """Return a node object to hand to ``resize_cluster(add=[...])`` for a scale-out.

        The two targets add capacity in fundamentally different ways, so the choice is
        gated here rather than in the test:

        * On-prem: a real, pre-provisioned spare server from the INI
          (``servers[nodes_init]``). Adding it is an ns_server rebalance-in. Returns
          ``None`` when the INI has no spare (the caller asserts/decides).
        * On Capella: there is no pre-provisioned spare. Adding a node is a capacity
          request -- ``CapellaRebalanceTask`` recomputes the per-service node counts
          from ``to_add`` and calls the scaling API (count + 1). So a lightweight
          placeholder server carrying the target services is all that is needed; Capella
          provisions the real node.
        """
        services = services or ["index"]
        if not isinstance(services, list):
            services = [services]
        if not self.capella_run:
            idx = self.test.nodes_init
            servers = self.test.servers
            spare = servers[idx] if idx < len(servers) else None
            _log = getattr(self.test, "log", None)
            if _log:
                _log.info("DUALRUN-FLOW: node_to_add -> ON-PREM spare %s"
                          % (getattr(spare, "ip", None)))
            return spare
        from TestInput import TestInputServer
        placeholder = TestInputServer()
        placeholder.services = services
        _log = getattr(self.test, "log", None)
        if _log:
            _log.info("DUALRUN-FLOW: node_to_add -> CAPELLA capacity-request placeholder "
                      "(services=%s); scale API will provision the node" % services)
        return placeholder

    def failover_node(self, nodes, graceful=False):
        """Failover node(s). Not available on Capella -> raises ``UnsupportedOnTarget``."""
        _log = getattr(self.test, "log", None)
        if self.capella_run:
            if _log:
                _log.info("DUALRUN-FLOW: failover_node -> UNSUPPORTED on Capella, raising "
                          "UnsupportedOnTarget (test will skip)")
            raise UnsupportedOnTarget(Capability.NODE_FAILOVER)
        if _log:
            _log.info("DUALRUN-FLOW: failover_node -> ON-PREM async_failover")
        return self.test.cluster.async_failover([self.test.master], nodes, graceful)

    def close(self):
        """Release backend resources (the Capella SDK cluster used for N1QL)."""
        self._query.close()

    # --- internals ---
    def _probe_ssh(self):
        """Whether SSH to the cluster nodes is usable on this (Capella) target.

        Defaults to False (Capella is treated as no-SSH) unless the run opts in
        with ``-p capella_ssh_reachable=True``. A live connectivity probe can
        replace this later; the opt-in flag avoids blocking on an SSH connect
        attempt against a cluster that does not allow it.
        """
        try:
            return self.test.input.param("capella_ssh_reachable", False)
        except Exception:
            return False

    def _compute_caps(self):
        """Platform capabilities plus whatever the selected backends declare in ``CAPS``.

        Node/query capabilities come from the backends, so plugging in a new access
        channel (e.g. a new ``NodeOps`` backend) only needs that backend and its
        selection in ``make_node_ops`` / ``make_query_ops``; ``supports()`` follows.
        """
        if not self.capella_run:
            platform = {Capability.TOPOLOGY_CONTROL, Capability.NODE_FAILOVER,
                        Capability.CLUSTER_SETTINGS}
        else:
            platform = {Capability.TOPOLOGY_CONTROL, Capability.PRIVATE_ENDPOINT}
        return frozenset(platform | self._node.CAPS | self._query.CAPS)
