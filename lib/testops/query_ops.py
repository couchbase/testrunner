"""N1QL query backends for dual-run tests.

These are the internal implementations behind ``RunTarget``'s query facade.
``NodeQueryOps`` runs statements against a specific query node (on-prem: the node
the test picked from the services map). ``ConnectStringQueryOps`` covers Capella:
statements go through the cluster's connect string (``couchbases://<srv>``) and the
SDK routes them to a live query node, so a test never has to discover, or pin
itself to, a query node that a scale operation may evict. Tests do not use these
directly -- they call ``self.target.query_node()`` and ``self.target.run_query(...)``.

The SDK and ``CapellaAPI`` are imported lazily inside methods to avoid import
cycles at module load time and to keep on-prem runs free of Capella imports.
"""

# TODO: remove the temporary DUALRUN-FLOW logging once all components have tested the dual-run flow.

import threading
from datetime import timedelta

from lib.Cb_constants.CBServer import CbServer
from lib.testops.capabilities import Capability, UnsupportedOnTarget


class QueryOps(object):
    """Interface for running N1QL statements on the current target. Base methods raise
    ``UnsupportedOnTarget`` so a backend opts in by overriding them and declaring
    ``Capability.QUERY`` in ``CAPS`` (same contract as ``NodeOps``)."""

    CAPS = frozenset()

    def __init__(self, target):
        self.target = target
        self.test = target.test
        self.log = getattr(target.test, "log", None)

    def query_node(self):
        raise UnsupportedOnTarget(Capability.QUERY)

    def run_query(self, query=None, server=None, **kwargs):
        raise UnsupportedOnTarget(Capability.QUERY)

    def close(self):
        """Release backend resources (no-op unless the backend opened one)."""
        return None


class NodeQueryOps(QueryOps):
    """Queries against a query node picked from the services map. Used on-prem."""

    CAPS = frozenset({Capability.QUERY})

    def __init__(self, target):
        super().__init__(target)
        # Capture the helper's own method now so a test that rebinds
        # ``n1ql_helper.run_cbq_query`` to ``target.run_query`` cannot recurse.
        helper = getattr(target.test, "n1ql_helper", None)
        self._run_cbq_query = getattr(helper, "run_cbq_query", None)

    def query_node(self):
        return self.test.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)

    def run_query(self, query=None, server=None, **kwargs):
        kwargs.pop("database", None)
        if server is None:
            server = self.query_node()
        run_cbq_query = self._run_cbq_query or self.test.run_cbq_query
        return run_cbq_query(query=query, server=server, **kwargs)


class ConnectStringQueryOps(QueryOps):
    """Queries on Capella through the cluster connect string, never a specific node.

    Accepts (and ignores) ``server``/``database`` so it is a drop-in replacement for
    ``n1ql_helper.run_cbq_query`` and the ``GSIUtils`` query callable. Returns the
    same ``{"results", "status", "metrics"}`` shape and raises ``CBQError`` on a
    failed statement, as the REST path does.
    """

    CAPS = frozenset({Capability.QUERY})

    def __init__(self, target):
        super().__init__(target)
        self._cluster = None
        self._connect_string = None
        self._lock = threading.Lock()

    def query_node(self):
        # No node lookup on Capella: callers pass this straight back as ``server=``
        # and run_query ignores it in favour of the connect string.
        return None

    def connect_string(self):
        if self._connect_string is None:
            from lib.capella.utils import CapellaAPI
            api = CapellaAPI(CbServer.capella_credentials)
            srv = api.get_cluster_srv(CbServer.capella_cluster_id)
            # Test clusters (incl. non-prod pods) may not chain to the SDK's bundled
            # Capella CA, so verification is opt-in: -p capella_tls_verify=True.
            verify = str(self.test.input.param("capella_tls_verify", False)).lower() == "true"
            self._connect_string = "couchbases://%s" % srv if verify \
                else "couchbases://%s?ssl=no_verify" % srv
            if self.log:
                self.log.info("DUALRUN-FLOW: query connect string -> %s" % self._connect_string)
        return self._connect_string

    def _sdk_cluster(self):
        with self._lock:
            if self._cluster is None:
                from couchbase.auth import PasswordAuthenticator
                from couchbase.cluster import Cluster
                from couchbase.options import ClusterOptions
                opts = ClusterOptions(PasswordAuthenticator(CbServer.rest_username,
                                                            CbServer.rest_password))
                opts.apply_profile("wan_development")
                cluster = Cluster(self.connect_string(), opts)
                cluster.wait_until_ready(timedelta(seconds=60))
                self._cluster = cluster
            return self._cluster

    def run_query(self, query=None, server=None, database=None, scan_consistency=None,
                  query_params=None, timeout=None, query_context=None, verbose=True, **kwargs):
        from couchbase.n1ql import QueryScanConsistency
        from couchbase.options import QueryOptions
        from membase.api.exception import CBQError

        query_params = dict(query_params or {})
        scan_consistency = scan_consistency or query_params.get("scan_consistency")
        options = {}
        if scan_consistency and str(scan_consistency).lower() == "request_plus":
            options["scan_consistency"] = QueryScanConsistency.REQUEST_PLUS
        if timeout:
            options["timeout"] = _to_timedelta(timeout)
        if query_context:
            options["query_context"] = query_context
        if verbose and self.log:
            self.log.info("RUN QUERY (connect string) %s" % query)
        try:
            rows_iter = self._sdk_cluster().query(query, QueryOptions(**options))
            rows = [row for row in rows_iter]
            metadata = rows_iter.metadata()
        except Exception as e:
            raise CBQError(str(e)[:500], self.connect_string())
        metrics = {"resultCount": len(rows)}
        try:
            sdk_metrics = metadata.metrics()
            if sdk_metrics is not None:
                metrics["elapsedTime"] = str(sdk_metrics.elapsed_time())
                metrics["executionTime"] = str(sdk_metrics.execution_time())
        except Exception:
            pass
        status = getattr(metadata.status(), "value", metadata.status()) if metadata else "success"
        return {"results": rows, "status": str(status).lower(), "metrics": metrics}

    def close(self):
        with self._lock:
            if self._cluster is not None:
                try:
                    self._cluster.close()
                finally:
                    self._cluster = None


def _to_timedelta(timeout):
    """Accept the REST-style ``"120s"``/``"2m"``/``"500ms"`` strings or plain seconds."""
    if isinstance(timeout, timedelta):
        return timeout
    text = str(timeout).strip().lower()
    for suffix, scale in (("ms", 0.001), ("s", 1), ("m", 60), ("h", 3600)):
        if text.endswith(suffix):
            return timedelta(seconds=float(text[:-len(suffix)]) * scale)
    return timedelta(seconds=float(text))


def make_query_ops(target):
    """Pick the query backend for a ``RunTarget``.

    Capella -> ``ConnectStringQueryOps`` (SDK over the cluster connect string).
    On-prem -> ``NodeQueryOps`` (a query node from the services map).
    """
    _log = getattr(target.test, "log", None)
    if target.capella_run:
        if _log:
            _log.info("DUALRUN-FLOW: make_query_ops -> ConnectStringQueryOps (capella_run)")
        return ConnectStringQueryOps(target)
    if _log:
        _log.info("DUALRUN-FLOW: make_query_ops -> NodeQueryOps")
    return NodeQueryOps(target)
