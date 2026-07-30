import json
import time
import urllib.parse

from pytests.xdcr.xdcrnewbasetests import XDCRNewBaseTest, NodeHelper, FloatingServers
from lib.membase.api.on_prem_rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection

class TargetAwarenessXDCR(XDCRNewBaseTest):
    """Target-side awareness of source clusters, i.e. XDCR topology heartbeats.

    The source cluster's ns_server *orchestrator* - not necessarily its master
    node - sends a heartbeat to one node of every cluster it replicates to,
    carrying the source cluster's UUID and name, its replication specs and the
    list of its *data-service* nodes. The receiving node caches it and proxies
    it to its peers, so every target node ends up with the same view, exposed
    through `GET /xdcr/sourceClusters` and through the
    `xdcr_number_of_source_nodes_total`,
    `xdcr_number_of_source_replications_total` and
    `xdcr_source_cluster_heartbeat_recv_bytes` metrics.
    """

    # Shipped cadence is a 60s minimum interval, x5 for the maximum (300s) and
    # a 2x-maximum cache TTL (600s), so a heartbeat reflecting a change the test
    # just made can be five minutes away. Tests shorten it to 10s/10s/20s and
    # derive every wait from it.
    HB_MIN_INTERVAL = 10
    HB_MAX_INTERVAL_FACTOR = 1
    # Shipped values, restored in tearDown: the setting lives in metakv, so a
    # test that lowers it leaves a shared lab cluster on a 10s cadence forever.
    DEFAULT_HB_MIN_INTERVAL = 60
    DEFAULT_HB_MAX_INTERVAL_FACTOR = 5
    DEFAULT_HB_MAX_INTERVAL = DEFAULT_HB_MIN_INTERVAL * DEFAULT_HB_MAX_INTERVAL_FACTOR

    # testrunner appends these as ordinary tests around the suite. They exist so
    # its AFTER_SUITE lookup resolves - without suite_tearDown every run of this
    # suite ends with "ERROR: suite_tearDown ... has no attribute", which reads as
    # a suite failure in any report - and setUp/tearDown skip their work for them
    # so the placeholders do not each cost a full cluster init.
    SUITE_HOOKS = ("suite_setUp", "suite_tearDown")

    # A settings POST restarts goxdcr cluster-wide, and the 8091 proxy answers
    # "Unexpected server error" while that is in flight, so the write is retried.
    SETTINGS_POST_ATTEMPTS = 4

    def suite_setUp(self):
        pass

    def suite_tearDown(self):
        pass

    def setUp(self):
        if self._testMethodName in self.SUITE_HOOKS:
            return
        super().setUp()
        self.src_cluster = self.get_cb_cluster_by_name('C1')
        self.dest_cluster = self.get_cb_cluster_by_name('C2')
        self.src_master = self.src_cluster.get_master_node()
        self.dest_master = self.dest_cluster.get_master_node()
        self.src_master_rest = RestConnection(self.src_master)
        self.dest_master_rest = RestConnection(self.dest_master)
        # Worst-case gap between two heartbeats; speed_up_heartbeats() lowers it.
        self.hb_max_interval = self.DEFAULT_HB_MAX_INTERVAL
        # n1ql by default: it is non-KV like any other service node but needs no
        # memory quota, so it can be added to any source cluster the suite runs
        # against. Pass e.g. non_kv_services=index,n1ql on clusters provisioned
        # for the index service.
        self.autofailover_state = {}
        self.hb_tuned_clusters = []
        self.network_blocked_nodes = []
        self.non_kv_services = self._input.param("non_kv_services", "n1ql")
        self.orchestrator_on_non_kv = self._input.param("orchestrator_on_non_kv", False)

    def tearDown(self):
        if self._testMethodName in self.SUITE_HOOKS:
            return
        try:
            # Safety net: a run killed between block and unblock would otherwise
            # leave the target cluster partitioned from the source. Undo it before
            # the framework's cleanup, which talks REST to both clusters.
            try:
                self.unblock_network_nftables()
            except Exception as e:
                self.log.error(f"Could not undo the network block in tearDown: {e}")
            super().tearDown()
        finally:
            # Last, so the goxdcr restart the settings change triggers cannot race
            # the REST calls cleanup makes.
            self.restore_heartbeat_cadence()

    def restore_heartbeat_cadence(self):
        """Put the shipped heartbeat cadence back on every cluster this test
        tuned. Never fails the test: the run is already over by this point."""
        settings = {
            "SrcHeartbeatMinInterval": self.DEFAULT_HB_MIN_INTERVAL,
            "SrcHeartbeatMaxIntervalFactor": self.DEFAULT_HB_MAX_INTERVAL_FACTOR,
        }
        for cluster in getattr(self, "hb_tuned_clusters", []):
            try:
                self.set_internal_xdcr_settings(cluster, settings)
            except Exception as e:
                self.log.warning(f"Could not restore the heartbeat cadence on "
                                 f"{cluster.get_name()}: {e}")

    def verify_source_to_dest_replication(self, src_outgoing_repls, dest_incoming_repls):
        src_target_cluster_uuid_list = set()  # list of target cluster UUIDs for src cluster
        for outgoing_repl in src_outgoing_repls:
            src_target_cluster_uuid_list.add(outgoing_repl["uuid"])
        for repl in dest_incoming_repls:
            specs = repl.get("SourceClusterReplSpecs") or []
            if not specs:
                self.fail(f"SourceClusterReplSpecs empty for source cluster UUID {repl.get('SourceClusterUUID')}")
            uuid = specs[0]["targetClusterUUID"]
            if uuid not in src_target_cluster_uuid_list:
                self.fail(f"Target cluster UUID {uuid} not found in source cluster's outgoing replications")

    def verify_dest_to_source_replication(self, dest_outgoing_repls, src_incoming_repls):
        dest_target_cluster_uuid_list = set()  # list of target cluster UUIDs for dest cluster
        for outgoing_repl in dest_outgoing_repls:
            dest_target_cluster_uuid_list.add(outgoing_repl["uuid"])
        for repl in src_incoming_repls:
            specs = repl.get("SourceClusterReplSpecs") or []
            if not specs:
                self.fail(f"SourceClusterReplSpecs empty for source cluster UUID {repl.get('SourceClusterUUID')}")
            uuid = specs[0]["targetClusterUUID"]
            if uuid not in dest_target_cluster_uuid_list:
                self.fail(f"Target cluster UUID {uuid} not found in dest cluster's outgoing replications")

    def _wait_for_incoming_repl_specs(self, master_rest, retries=18, interval=10):
        repls = self.get_incoming_replications(master_rest)
        count = 0
        while count < retries:
            ready = repls and all(r.get("SourceClusterReplSpecs") for r in repls)
            if ready:
                return repls
            count += 1
            self.wait_interval(interval, f"Waiting for incoming repls + SourceClusterReplSpecs to populate {count}/{retries}")
            repls = self.get_incoming_replications(master_rest)
        return repls

    def stop_couchbase(self, server):
        remote_shell_conn = RemoteMachineShellConnection(server)
        try:
            remote_shell_conn.stop_couchbase()
        except Exception as e:
            self.fail(f"Exception while stopping couchbase-server on {server.ip}: {e}")
        finally:
            remote_shell_conn.disconnect()

    def start_couchbase(self, server):
        remote_shell_conn = RemoteMachineShellConnection(server)
        try:
            remote_shell_conn.start_couchbase()
        except Exception as e:
            self.fail(f"Exception while starting couchbase-server on {server.ip}: {e}")
        finally:
            remote_shell_conn.disconnect()

    def set_internal_xdcr_settings(self, cluster, settings):
        """POST goxdcr's internal settings for `cluster` and assert they applied.

        Goes through ns_server's proxy on 8091, which forwards /xdcr/* to goxdcr's
        admin port - so no SSH, no credentials on a command line, and a real
        status code. All parameters go in one request: the settings live in metakv,
        so each POST restarts goxdcr cluster-wide, and N requests mean N restarts
        and N windows in which the next call fails.

        The values are read back and asserted because every timeout in this suite
        is derived from the cadence set here. A POST that quietly did not apply
        would leave the shipped 60s/300s cadence in place while the waits assume
        10s, turning an infrastructure failure into a product-shaped one.
        """
        api = self.get_live_rest(cluster).baseUrl + "xdcr/internalSettings"
        params = urllib.parse.urlencode(settings)
        # The proxy answers "Unexpected server error" while goxdcr is restarting -
        # which a settings change causes cluster-wide, so a POST can land in the
        # window opened by an earlier one. Retry before giving up; only a genuinely
        # unapplied setting should fail the test.
        last = None
        for attempt in range(1, self.SETTINGS_POST_ATTEMPTS + 1):
            try:
                rest = self.get_live_rest(cluster)
                status, content, _ = rest._http_request(
                    rest.baseUrl + "xdcr/internalSettings", "POST", params, timeout=30)
            except Exception as e:
                status, content = False, repr(e)
            if status:
                applied = json.loads(content)
                for key, value in settings.items():
                    self.assertEqual(
                        applied.get(key), value,
                        f"{cluster.get_name()} reports {key}={applied.get(key)!r} "
                        f"after setting it to {value!r}; the cadence every wait in "
                        f"this suite is derived from did not apply")
                self.log.info(
                    f"Set internal XDCR settings {settings} on {cluster.get_name()}")
                return
            last = content
            if attempt < self.SETTINGS_POST_ATTEMPTS:
                self.wait_interval(
                    15, f"POST {api} returned an error, retrying "
                        f"({attempt}/{self.SETTINGS_POST_ATTEMPTS}): {last}")
        self.fail(f"POST xdcr/internalSettings {settings} on {cluster.get_name()} "
                  f"failed {self.SETTINGS_POST_ATTEMPTS} times, last error: {last}")

    def speed_up_heartbeats(self, *clusters):
        """Shorten the heartbeat cadence of `clusters`.

        goxdcr keeps its internal settings in metakv, so one POST per cluster
        covers every node - including nodes that join later, which matters here
        because any node can end up the orchestrator, and therefore the sender -
        and restarts goxdcr cluster-wide.
        """
        settings = {"SrcHeartbeatMinInterval": self.HB_MIN_INTERVAL,
                    "SrcHeartbeatMaxIntervalFactor": self.HB_MAX_INTERVAL_FACTOR}
        for cluster in clusters:
            self.set_internal_xdcr_settings(cluster, settings)
            if cluster not in self.hb_tuned_clusters:
                self.hb_tuned_clusters.append(cluster)
        self.hb_max_interval = self.HB_MIN_INTERVAL * self.HB_MAX_INTERVAL_FACTOR
        self.wait_interval(30, "Waiting for goxdcr to restart after the internal settings change")

    def hb_timeout(self, factor=3):
        """How long to allow for a heartbeat-derived view to catch up: a few
        maximum intervals, plus slack for the target to cache and proxy it."""
        return max(90, int(self.hb_max_interval * factor) + 30)

    def get_cluster_uuid(self, cluster):
        rest = self.get_live_rest(cluster)
        status, content, _ = rest._http_request(
            api=rest.baseUrl + "pools/default/terseClusterInfo", method="GET", timeout=30)
        if not status:
            self.fail(f"Could not read terseClusterInfo of cluster {cluster.get_name()}")
        return json.loads(content)["clusterUUID"]

    def get_incoming_entry(self, dest_rest, src_uuid):
        """The target's cached heartbeat for source cluster `src_uuid`, or None."""
        for entry in self.get_incoming_replications(dest_rest) or []:
            if entry.get("SourceClusterUUID") == src_uuid:
                return entry
        return None

    def wait_for_source_cluster_nodes(self, dest_rest, src_uuid, expected_fn, timeout=None):
        """Poll the target until it advertises exactly the expected source nodes.

        `expected_fn` is re-evaluated on every pass rather than snapshotted, so a
        topology change *during* the wait (an auto-failover, someone else's
        rebalance) is compared against what the source cluster actually looks like
        now instead of producing a mismatch that misattributes the cause.
        @return: (matched, last entry seen, last expectation)
        """
        timeout = timeout or self.hb_timeout()
        end_time = time.time() + timeout
        entry = None
        expected = []
        while True:
            expected = sorted(expected_fn())
            entry = self.get_incoming_entry(dest_rest, src_uuid)
            if entry and sorted(entry.get("SourceClusterNodes") or []) == expected:
                return True, entry, expected
            if time.time() >= end_time:
                return False, entry, expected
            self.wait_interval(
                10, f"Waiting for target to report source nodes {expected}, "
                    f"currently {entry.get('SourceClusterNodes') if entry else None}")

    def wait_for_source_nodes_metric(self, dest_cluster, src_uuid, expected_count, timeout=None):
        """Poll `xdcr_number_of_source_nodes_total` on the target until *every*
        node reports `expected_count` for this source cluster.

        The gauge is cluster-level and exported by every target node, so the
        per-series read is the point: it also proves the receiving node proxied
        the heartbeat to its peers. Summing the series would just double it.
        @return: (matched, readings as {node: value})
        """
        timeout = timeout or self.hb_timeout()
        end_time = time.time() + timeout
        readings = {}
        while True:
            readings = {}
            for series in self.query_prometheus_metric_series(
                    dest_cluster, "xdcr_number_of_source_nodes_total"):
                if series["labels"].get("sourceClusterUUID") != src_uuid:
                    continue
                nodes = series["labels"].get("nodes")
                key = nodes[0] if isinstance(nodes, list) and nodes else str(nodes)
                readings[key] = series["value"]
            if readings and all(v == expected_count for v in readings.values()):
                return True, readings
            if time.time() >= end_time:
                return False, readings
            self.wait_interval(
                10, f"Waiting for xdcr_number_of_source_nodes_total to read "
                    f"{expected_count} on every target node, currently {readings}")

    def verify_source_cluster_nodes(self, src_uuid, context="", expect_nodes=None):
        """Assert the target's advertised source-node list matches the source
        cluster's active data nodes, and that the source-node gauge agrees.

        This is the assertion the suite was missing: the replication specs can
        be perfectly correct while the node list is wrong, which is exactly how
        MB-71771 (non-KV orchestrator advertised as a source node) looked.
        """
        if expect_nodes is not None:
            expected_fn = lambda: expect_nodes
        else:
            expected_fn = lambda: self.get_active_kv_nodes(self.src_cluster)
        matched, entry, expected = self.wait_for_source_cluster_nodes(
            self.dest_master_rest, src_uuid, expected_fn)
        self.assertTrue(
            matched,
            f"{context}: target advertises source nodes "
            f"{entry.get('SourceClusterNodes') if entry else None} but the source "
            f"cluster's active data nodes are {expected}"
            f" (heartbeat entry: {entry})")
        self.log.info(f"{context}: target reports source nodes {expected} as expected")

        matched, readings = self.wait_for_source_nodes_metric(
            self.dest_cluster, src_uuid, float(len(expected)))
        self.assertTrue(
            matched,
            f"{context}: xdcr_number_of_source_nodes_total per target node is "
            f"{readings}, expected every node to report {len(expected)}")
        return entry

    def verify_heartbeat_view_on_all_target_nodes(self, src_uuid, context="",
                                                  expect_nodes=None):
        """Every target node must expose the same cached heartbeat as the source
        cluster's real topology: the node that receives it proxies it to its peers.

        Each node is checked against the source cluster's active data nodes rather
        than against whatever the first node happened to report - comparing peers
        to each other both races a topology change (two nodes can briefly hold
        different-but-valid snapshots) and passes a list that is consistently
        wrong on every node.
        """
        expected_specs = None
        for node in self.dest_cluster.get_nodes():
            rest = RestConnection(node)
            expected_fn = ((lambda: expect_nodes) if expect_nodes is not None
                           else (lambda: self.get_active_kv_nodes(self.src_cluster)))
            matched, entry, expected = self.wait_for_source_cluster_nodes(
                rest, src_uuid, expected_fn)
            self.assertIsNotNone(
                entry,
                f"{context}: target node {node.ip}:{node.port} has no heartbeat "
                f"cached for source cluster {src_uuid}")
            self.assertTrue(
                matched,
                f"{context}: target node {node.ip}:{node.port} reports source nodes "
                f"{sorted(entry.get('SourceClusterNodes') or [])}, expected {expected}")
            specs = sorted(spec["id"] for spec in entry.get("SourceClusterReplSpecs") or [])
            if expected_specs is None:
                expected_specs = specs
            self.assertEqual(
                specs, expected_specs,
                f"{context}: target node {node.ip}:{node.port} reports source specs "
                f"{specs} while another node reports {expected_specs}")
        self.log.info(f"{context}: all {len(self.dest_cluster.get_nodes())} target nodes "
                      f"agree with the source topology and on specs {expected_specs}")

    def same_node(self, one, other):
        return one.ip == other.ip and str(one.port) == str(other.port)

    def disable_autofailover(self, cluster):
        """Turn auto-failover off, remembering the previous setting, so that a
        node a test takes down on purpose is not failed over behind its back -
        which would change the source topology mid-assertion."""
        rest = self.get_live_rest(cluster)
        settings = rest.get_autofailover_settings()
        self.autofailover_state[cluster.get_name()] = (settings.enabled, settings.timeout)
        self.assertTrue(
            rest.update_autofailover_settings(False, settings.timeout),
            f"Could not disable auto-failover on cluster {cluster.get_name()}")

    def restore_autofailover(self, cluster):
        enabled, timeout = self.autofailover_state.pop(cluster.get_name(), (True, 120))
        rest = self.get_live_rest(cluster)
        rest.update_autofailover_settings(enabled, timeout)

    def wait_for_node_healthy(self, cluster, node, timeout=300):
        """Wait until `node` is back to 'healthy' in the cluster's node list."""
        end_time = time.time() + timeout
        status = None
        while time.time() < end_time:
            rest = self.get_live_rest(cluster)
            for info in rest.get_pools_default()["nodes"]:
                if info["hostname"] != f"{node.ip}:{node.port}":
                    continue
                status = info.get("status")
                if status == "healthy":
                    return
            self.wait_interval(10, f"Waiting for {node.ip}:{node.port} to become healthy, "
                                   f"currently {status}")
        self.fail(f"Node {node.ip}:{node.port} did not become healthy within {timeout}s")

    def force_orchestrator(self, cluster, node, max_attempts=2):
        """Best-effort attempt to make `node` the ns_server orchestrator.

        Returns True if `node` ended up the orchestrator, False otherwise -
        callers must handle False, because leadership is not ours to place.
        ns_server elects it, there is no API to hand it over, and stopping the
        current orchestrator only moves it *while that node is down*: on
        8.1.0-2570 the restarted node reclaimed leadership every time (4/4),
        which is also why `restart` is useless here. What does reliably land
        leadership on a node is rebalancing that node into the cluster.
        Auto-failover is off for the duration so a node that is briefly down is
        not failed over behind the test's back.
        """
        if self.same_node(self.get_orchestrator_node(cluster), node):
            return True
        self.disable_autofailover(cluster)
        try:
            for attempt in range(1, max_attempts + 1):
                current = self.get_orchestrator_node(cluster)
                if self.same_node(current, node):
                    break
                self.log.info(
                    f"Attempt {attempt}/{max_attempts}: orchestrator of "
                    f"{cluster.get_name()} is {current.ip}:{current.port}, stopping it "
                    f"to move leadership towards {node.ip}:{node.port}")
                self.stop_couchbase(current)
                try:
                    end_time = time.time() + 180
                    while time.time() < end_time:
                        new_orch = self.get_orchestrator_node(cluster)
                        if not self.same_node(new_orch, current):
                            self.log.info(f"Leadership moved to {new_orch.ip}:{new_orch.port}")
                            break
                        self.wait_interval(10, "Waiting for leadership to move off "
                                               f"{current.ip}:{current.port}")
                finally:
                    self.start_couchbase(current)
                self.wait_for_node_healthy(cluster, current)
        finally:
            self.restore_autofailover(cluster)
        current = self.get_orchestrator_node(cluster)
        landed = self.same_node(current, node)
        self.log.info(
            f"Orchestrator of {cluster.get_name()} is {current.ip}:{current.port}; "
            f"wanted {node.ip}:{node.port} -> {'ok' if landed else 'could not place it'}")
        return landed

    def otp_node_of(self, cluster, node):
        """The otpNode id `cluster` knows `node` by."""
        rest = self.get_live_rest(cluster)
        for info in rest.get_pools_default()["nodes"]:
            if info["hostname"] == f"{node.ip}:{node.port}":
                return info["otpNode"]
        self.fail(f"{node.ip}:{node.port} is not a member of {cluster.get_name()}")

    def kv_node_other_than_orchestrator(self, cluster):
        """An active data node of `cluster` that is not the orchestrator.

        Tests that need a node which is *not* the heartbeat sender pick one
        relative to wherever leadership currently sits, rather than trying to
        move leadership (see force_orchestrator).
        """
        orchestrator = self.get_orchestrator_node(cluster)
        active_kv = set(self.get_active_kv_nodes(cluster))
        for node in cluster.get_nodes():
            if self.same_node(node, orchestrator):
                continue
            if f"{node.ip}:{node.port}" in active_kv:
                return node
        self.fail(f"{cluster.get_name()} has no active data node other than the "
                  f"orchestrator {orchestrator.ip}:{orchestrator.port}")

    def failover_node(self, cluster, node, graceful=False):
        """Fail over one specific node and wait for it to read inactiveFailed.

        The cluster object's own failover helper always takes the *last* node,
        which is not good enough here: which node matters relative to the
        orchestrator.
        """
        otp = self.otp_node_of(cluster, node)
        rest = self.get_live_rest(cluster)
        self.assertTrue(
            rest.fail_over(otp, graceful=graceful),
            f"Failover of {otp} on {cluster.get_name()} failed")
        end_time = time.time() + 300
        membership = None
        while time.time() < end_time:
            for info in self.get_live_rest(cluster).get_pools_default()["nodes"]:
                if info["hostname"] == f"{node.ip}:{node.port}":
                    membership = info.get("clusterMembership")
            if membership == "inactiveFailed":
                return
            self.wait_interval(10, f"Waiting for {otp} to read inactiveFailed, "
                                   f"currently {membership}")
        self.fail(f"{otp} did not reach inactiveFailed; membership is {membership}")

    def add_back_node_and_rebalance(self, cluster, node, recovery_type="full"):
        """Add a failed-over node back and rebalance the cluster whole again."""
        rest = self.get_live_rest(cluster)
        otp = self.otp_node_of(cluster, node)
        rest.add_back_node(otp)
        rest.set_recovery_type(otpNode=otp, recoveryType=recovery_type)
        known = [info["otpNode"] for info in rest.get_pools_default()["nodes"]]
        rest.rebalance(known, [])
        self.assertTrue(
            rest.monitorRebalance(),
            f"Rebalance after adding {otp} back to {cluster.get_name()} failed")

    def test_target_awareness(self):
        self.setup_xdcr_and_load()
        self.speed_up_heartbeats(self.src_cluster, self.dest_cluster)
        src_uuid = self.get_cluster_uuid(self.src_cluster)

        src_outgoing_repls = self.get_outgoing_replications(self.src_master_rest)
        dest_incoming_repls = self._wait_for_incoming_repl_specs(self.dest_master_rest)
        if not dest_incoming_repls or not all(r.get("SourceClusterReplSpecs") for r in dest_incoming_repls):
            self.fail(f"Incoming replications / SourceClusterReplSpecs not populated on dest cluster: {dest_incoming_repls}")
        self.verify_source_to_dest_replication(src_outgoing_repls, dest_incoming_repls)
        self.verify_source_cluster_nodes(src_uuid, context="unidirectional setup")
        if self._rdirection == "bidirection":
            dest_outgoing_repls = self.get_outgoing_replications(self.dest_master_rest)
            src_incoming_repls = self._wait_for_incoming_repl_specs(self.src_master_rest)
            if not src_incoming_repls or not all(r.get("SourceClusterReplSpecs") for r in src_incoming_repls):
                self.fail(f"Incoming replications / SourceClusterReplSpecs not populated on src cluster: {src_incoming_repls}")
            self.verify_dest_to_source_replication(dest_outgoing_repls, src_incoming_repls)
            dest_uuid = self.get_cluster_uuid(self.dest_cluster)
            matched, entry, expected = self.wait_for_source_cluster_nodes(
                self.src_master_rest, dest_uuid,
                lambda: self.get_active_kv_nodes(self.dest_cluster))
            self.assertTrue(
                matched,
                f"bidirectional setup: C1 advertises C2's nodes as "
                f"{entry.get('SourceClusterNodes') if entry else None}, expected "
                f"{expected}")

    def test_heartbeat(self):
        # Check heartbeat appearing in logs

        self.setup_xdcr_and_load()
        self.speed_up_heartbeats(self.src_cluster, self.dest_cluster)
        src_uuid = self.get_cluster_uuid(self.src_cluster)
        src_outgoing_repls = self.get_outgoing_replications(self.src_master_rest)
        dest_incoming_repls = self._wait_for_incoming_repl_specs(self.dest_master_rest)

        self.verify_source_to_dest_replication(src_outgoing_repls, dest_incoming_repls)
        self.verify_source_cluster_nodes(src_uuid, context="heartbeat received")

        src_target_cluster_uuid_list = set()
        for outgoing_repl in src_outgoing_repls:
            src_target_cluster_uuid_list.add(outgoing_repl["uuid"])

        for replications in dest_incoming_repls:
            src_cluster_uuid_on_dest = replications["SourceClusterUUID"]
            for specs in replications["SourceClusterReplSpecs"]:
                uuid = specs["id"].split('/')[0]
                if uuid in src_target_cluster_uuid_list:
                    search_str = f"GOXDCR.P2PManager: Heartbeats heard from - SrcUUID: {src_cluster_uuid_on_dest}"
                    matches, count = NodeHelper.check_goxdcr_log(self.dest_master, search_str, timeout=30)
                    if count == 0:
                        self.fail(f"No heartbeat heard in dest cluster for source cluster UUID {src_cluster_uuid_on_dest}")
                    self.log.info(f"Found logs for heartbeat in dest cluster with source cluster UUID {src_cluster_uuid_on_dest}")

    def test_non_kv_node_excluded_from_heartbeat(self):
        """A source node without the data service must never be advertised to a
        target cluster - including when it is the source orchestrator, i.e. the
        very node that composes and sends the heartbeat (MB-71771).

        Run with `orchestrator_on_non_kv=True` for the regression case; the
        default covers the same node as a plain peer.
        """
        self.require_spare_nodes(1)
        self.setup_xdcr_and_load()
        self.speed_up_heartbeats(self.src_cluster, self.dest_cluster)
        src_uuid = self.get_cluster_uuid(self.src_cluster)
        self.verify_source_cluster_nodes(src_uuid, context="before adding a non-KV node")

        non_kv_node = self.src_cluster.rebalance_in_with_services([self.non_kv_services])[0]
        non_kv_addr = f"{non_kv_node.ip}:{non_kv_node.port}"
        self.log.info(f"Rebalanced in {non_kv_addr} with services {self.non_kv_services}")
        try:
            if self.orchestrator_on_non_kv and not self.force_orchestrator(
                    self.src_cluster, non_kv_node):
                # Rebalancing the node in normally lands leadership on it; when it
                # does not there is no supported way to move it, so skip rather
                # than report a failure the product is not responsible for.
                self.skipTest(
                    f"ns_server would not make the non-KV node {non_kv_addr} the "
                    f"orchestrator, so the MB-71771 scenario cannot be set up")
            orchestrator = self.get_orchestrator_node(self.src_cluster)
            self.log.info(f"Source cluster orchestrator is {orchestrator.ip}:{orchestrator.port}, "
                          f"non-KV node is {non_kv_addr}")

            entry = self.verify_source_cluster_nodes(
                src_uuid,
                context=f"with non-KV node {non_kv_addr} in the source cluster "
                        f"(orchestrator: {orchestrator.ip}:{orchestrator.port})")
            self.assertNotIn(
                non_kv_addr, entry.get("SourceClusterNodes") or [],
                f"Non-KV source node {non_kv_addr} is advertised to the target in "
                f"SourceClusterNodes {entry.get('SourceClusterNodes')} "
                f"(orchestrator: {orchestrator.ip}:{orchestrator.port})")
            self.verify_heartbeat_view_on_all_target_nodes(
                src_uuid, context=f"with non-KV node {non_kv_addr} in the source cluster")
        finally:
            # Ejects the last node of the cluster, i.e. the one just added.
            self.src_cluster.rebalance_out(num_nodes=1)
        self.verify_source_cluster_nodes(
            src_uuid, context="after rebalancing the non-KV node out")

    def test_failed_over_node_dropped_from_heartbeat(self):
        """A hard-failed-over data node stays a cluster member (inactiveFailed)
        but must drop out of the source-node list advertised to the target.

        The node failed over here is deliberately *not* the orchestrator, so the
        test isolates goxdcr's active-member filter from what happens when the
        heartbeat sender itself disappears - that is
        `test_failed_over_orchestrator_dropped_from_heartbeat`.
        """
        self.setup_xdcr_and_load()
        self.speed_up_heartbeats(self.src_cluster, self.dest_cluster)
        src_uuid = self.get_cluster_uuid(self.src_cluster)
        failed_node = self.kv_node_other_than_orchestrator(self.src_cluster)
        self.assert_failed_over_node_dropped(src_uuid, failed_node)

    def test_failed_over_orchestrator_dropped_from_heartbeat(self):
        """Hard-fail-over the source *orchestrator* - the node that composes and
        sends the heartbeat - and the target must stop being told that node is a
        source data node."""
        self.setup_xdcr_and_load()
        self.speed_up_heartbeats(self.src_cluster, self.dest_cluster)
        src_uuid = self.get_cluster_uuid(self.src_cluster)
        failed_node = self.get_orchestrator_node(self.src_cluster)
        self.assert_failed_over_node_dropped(src_uuid, failed_node)

    def assert_failed_over_node_dropped(self, src_uuid, failed_node):
        """Hard-fail-over `failed_node` and assert the target stops advertising it,
        then add it back and assert the list recovers."""
        failed_addr = f"{failed_node.ip}:{failed_node.port}"
        orchestrator = self.get_orchestrator_node(self.src_cluster)
        self.log.info(f"Failing over {failed_addr}; source orchestrator (the heartbeat "
                      f"sender) is {orchestrator.ip}:{orchestrator.port}")
        self.verify_source_cluster_nodes(
            src_uuid, context=f"before failing over {failed_addr}")
        self.failover_node(self.src_cluster, failed_node)
        try:
            entry = self.verify_source_cluster_nodes(
                src_uuid, context=f"after hard failover of {failed_addr}, before rebalance")
            self.assertNotIn(
                failed_addr, entry.get("SourceClusterNodes") or [],
                f"Failed-over source node {failed_addr} is still advertised to the "
                f"target in SourceClusterNodes {entry.get('SourceClusterNodes')}")
        finally:
            self.add_back_node_and_rebalance(self.src_cluster, failed_node)
        self.verify_source_cluster_nodes(
            src_uuid, context=f"after adding {failed_addr} back")

    def test_rebalance_updates_source_node_list(self):
        """Rebalancing a data node in and out of the source cluster must be
        reflected in what the target advertises."""
        self.require_spare_nodes(1)
        self.setup_xdcr_and_load()
        self.speed_up_heartbeats(self.src_cluster, self.dest_cluster)
        src_uuid = self.get_cluster_uuid(self.src_cluster)
        self.verify_source_cluster_nodes(src_uuid, context="initial topology")

        self.src_cluster.rebalance_in(num_nodes=1)
        added = self.src_cluster.get_nodes()[-1]
        added_addr = f"{added.ip}:{added.port}"
        entry = self.verify_source_cluster_nodes(
            src_uuid, context=f"after rebalancing in data node {added_addr}")
        self.assertIn(
            added_addr, entry.get("SourceClusterNodes") or [],
            f"New source data node {added_addr} is not advertised to the target in "
            f"SourceClusterNodes {entry.get('SourceClusterNodes')}")

        self.src_cluster.rebalance_out(num_nodes=1)
        entry = self.verify_source_cluster_nodes(
            src_uuid, context=f"after rebalancing out data node {added_addr}")
        self.assertNotIn(
            added_addr, entry.get("SourceClusterNodes") or [],
            f"Rebalanced-out source node {added_addr} is still advertised to the "
            f"target in SourceClusterNodes {entry.get('SourceClusterNodes')}")

    def test_heartbeat_view_on_all_target_nodes(self):
        """Every node of the target cluster must expose the same heartbeat view,
        not just the node that happened to receive it."""
        self.setup_xdcr_and_load()
        self.speed_up_heartbeats(self.src_cluster, self.dest_cluster)
        src_uuid = self.get_cluster_uuid(self.src_cluster)
        self.verify_source_cluster_nodes(src_uuid, context="heartbeat received")
        self.verify_heartbeat_view_on_all_target_nodes(src_uuid, context="steady state")

    def test_source_cluster_name_propagation(self):
        """The source cluster's name travels in the heartbeat and shows up on the
        target, in the REST view and as the sourceClusterName metric label."""
        self.setup_xdcr_and_load()
        self.speed_up_heartbeats(self.src_cluster, self.dest_cluster)
        src_uuid = self.get_cluster_uuid(self.src_cluster)
        self.verify_source_cluster_nodes(src_uuid, context="before renaming the source cluster")

        original_name = self.src_master_rest.get_pools_default().get("clusterName", "")
        new_name = self._input.param("source_cluster_name", "xdcr-src-cluster")
        self.assertTrue(
            self.src_master_rest.set_cluster_name(new_name),
            f"Could not set source cluster name to {new_name}")
        try:
            end_time = time.time() + self.hb_timeout()
            reported = None
            while True:
                entry = self.get_incoming_entry(self.dest_master_rest, src_uuid)
                reported = entry.get("SourceClusterName") if entry else None
                if reported == new_name:
                    break
                if time.time() >= end_time:
                    break
                self.wait_interval(
                    10, f"Waiting for source cluster name {new_name} to reach the target, "
                        f"currently {reported}")
            self.assertEqual(
                reported, new_name,
                f"Target reports source cluster name {reported!r}, expected {new_name!r}")

            # The rename produces a new series; the pre-rename one lingers for the
            # rest of the query window, so require the new name to be present
            # rather than asserting on every series.
            names = {series["labels"].get("sourceClusterName")
                     for series in self.query_prometheus_metric_series(
                         self.dest_cluster, "xdcr_number_of_source_nodes_total")
                     if series["labels"].get("sourceClusterUUID") == src_uuid}
            self.assertTrue(
                names, f"No xdcr_number_of_source_nodes_total series for {src_uuid}")
            self.assertIn(
                new_name, names,
                f"Metric label sourceClusterName never carried {new_name!r}, saw {names}")
        finally:
            self.src_master_rest.set_cluster_name(original_name)

    def test_replication_delete(self):
        self.setup_xdcr_and_load()
        self.speed_up_heartbeats(self.src_cluster, self.dest_cluster)
        src_outgoing_repls = self.get_outgoing_replications(self.src_master_rest)
        dest_incoming_repls = self._wait_for_incoming_repl_specs(self.dest_master_rest)

        self.verify_source_to_dest_replication(src_outgoing_repls, dest_incoming_repls)

        self.src_master_rest.remove_all_replications()
        self.src_master_rest.remove_all_remote_clusters()
        end_time = time.time() + self.hb_timeout(factor=6)
        while True:
            dest_incoming_repls = self.get_incoming_replications(self.dest_master_rest)
            if not dest_incoming_repls:
                break
            if time.time() >= end_time:
                break
            self.wait_interval(10, "Waiting for the target to drop the source cluster entry")

        if dest_incoming_repls:
            self.fail(f"Incoming replications not deleted from dest cluster: {dest_incoming_repls}")

    def test_node_crash(self):
        """Crashing the source *orchestrator* - the heartbeat sender - must not
        stop heartbeats: leadership moves and the new orchestrator keeps the
        target's view fresh and unchanged."""
        self.setup_xdcr_and_load()
        self.speed_up_heartbeats(self.src_cluster, self.dest_cluster)
        src_uuid = self.get_cluster_uuid(self.src_cluster)
        src_outgoing_repls = self.get_outgoing_replications(self.src_master_rest)
        dest_incoming_repls = self._wait_for_incoming_repl_specs(self.dest_master_rest)

        self.verify_source_to_dest_replication(src_outgoing_repls, dest_incoming_repls)
        entry = self.verify_source_cluster_nodes(src_uuid, context="before crashing the orchestrator")
        expected_nodes = sorted(entry.get("SourceClusterNodes") or [])
        last_recv = entry.get("SourceClusterHBReceiveTime")

        orchestrator = self.get_orchestrator_node(self.src_cluster)
        self.log.info(f"Crashing source orchestrator {orchestrator.ip}:{orchestrator.port}")
        # A node that is down but not failed over is still an active member, so
        # goxdcr keeps advertising it: the node list must not change.
        self.disable_autofailover(self.src_cluster)
        try:
            self.stop_couchbase(orchestrator)
            new_orchestrator = None
            end_time = time.time() + 180
            while time.time() < end_time:
                new_orchestrator = self.get_orchestrator_node(self.src_cluster)
                if not self.same_node(new_orchestrator, orchestrator):
                    break
                self.wait_interval(10, "Waiting for a new orchestrator to be elected")
            self.assertFalse(
                self.same_node(new_orchestrator, orchestrator),
                f"Orchestrator is still the stopped node {orchestrator.ip}:{orchestrator.port}")
            self.log.info(f"New orchestrator is {new_orchestrator.ip}:{new_orchestrator.port}")

            fresh = None
            end_time = time.time() + self.hb_timeout(factor=6)
            while True:
                fresh = self.get_incoming_entry(self.dest_master_rest, src_uuid)
                if fresh and fresh.get("SourceClusterHBReceiveTime") != last_recv:
                    break
                if time.time() >= end_time:
                    break
                self.wait_interval(10, "Waiting for a heartbeat from the new orchestrator")
            self.assertIsNotNone(
                fresh, "Target dropped the source cluster entry after the orchestrator crash")
            self.assertNotEqual(
                fresh.get("SourceClusterHBReceiveTime"), last_recv,
                f"No heartbeat received from the new orchestrator "
                f"{new_orchestrator.ip}:{new_orchestrator.port} within "
                f"{self.hb_timeout(factor=6)}s; last one is still from {last_recv}")
            self.assertEqual(
                sorted(fresh.get("SourceClusterNodes") or []), expected_nodes,
                f"Source node list changed after the orchestrator crash: "
                f"{sorted(fresh.get('SourceClusterNodes') or [])} vs {expected_nodes} "
                f"(a stopped node that has not been failed over is still an active member)")
        finally:
            self.start_couchbase(orchestrator)
            self.wait_for_node_healthy(self.src_cluster, orchestrator)
            self.restore_autofailover(self.src_cluster)

        self.verify_source_cluster_nodes(src_uuid, context="after the orchestrator came back")

    def test_network_failure(self):
        self.require_network_blocking(self.dest_cluster)
        self.setup_xdcr_and_load()
        self.speed_up_heartbeats(self.src_cluster, self.dest_cluster)
        src_outgoing_repls = self.get_outgoing_replications(self.src_master_rest)
        dest_incoming_repls = self._wait_for_incoming_repl_specs(self.dest_master_rest)

        self.verify_source_to_dest_replication(src_outgoing_repls, dest_incoming_repls)

        # Block network between source and destination clusters
        self.block_network_nftables(self.src_cluster, self.dest_cluster)
        try:
            # The cached entry lives for 2 x the maximum heartbeat interval.
            end_time = time.time() + self.hb_timeout(factor=6)
            while True:
                dest_incoming_repls = self.get_incoming_replications(self.dest_master_rest)
                if not dest_incoming_repls:
                    break
                if time.time() >= end_time:
                    break
                self.wait_interval(10, "Waiting for the target's cached heartbeat to expire")
            if dest_incoming_repls:
                self.fail(f"Replications exist despite network failure: {dest_incoming_repls}")
        finally:
            self.unblock_network_nftables(self.src_cluster, self.dest_cluster)

        dest_incoming_repls_after_unblock = self._wait_for_incoming_repl_specs(self.dest_master_rest)
        if not dest_incoming_repls_after_unblock:
            self.fail("No incoming replications reported by dest cluster after unblocking network")

        self.verify_source_to_dest_replication(src_outgoing_repls, dest_incoming_repls_after_unblock)
        self.verify_source_cluster_nodes(
            self.get_cluster_uuid(self.src_cluster), context="after the network was restored")
        self.log.info("Network failure test completed successfully.")

    # nftables table the block rules live in. Its own table, not `inet filter`,
    # so cleanup can drop exactly what the test added instead of flushing every
    # rule on the node.
    NFT_TABLE = "xdcr_target_awareness_test"

    def run_remote(self, server, cmd, timeout=60):
        """Run `cmd` on `server` and return (exit code, combined output).

        Deliberately not `use_channel=True`: that path surfaces neither stderr nor
        a failure - it logged "command executed successfully" for a command that
        died with "nft: command not found", which is how a firewall test once
        blocked nothing and then blamed the product. `get_exit_code=True` gives the
        real status, and stderr comes back with it so failures are legible.
        """
        shell = RemoteMachineShellConnection(server)
        try:
            out, err, rc = shell.execute_command(
                cmd, timeout=timeout, get_exit_code=True)
            text = "\n".join((out or []) + (err or []))
            return rc, text
        finally:
            shell.disconnect()

    def require_spare_nodes(self, count=1):
        """Fail early, naming the reason, when the ini has no spare node to add.

        Tests that rebalance a node in need servers listed in [servers] but in no
        cluster group. Without them the framework aborts mid-test with "Number of
        free nodes: 0 is not preset to add 1 nodes", which does not say what to fix;
        b/resources/6-nodes-template-xdcr.ini is a fitting ini (C1=_1,_2 C2=_3,_4,
        spares=_5,_6). This is a failure rather than a skip on purpose: the suite is
        meant to run on an ini with spares, and skipping would quietly drop the
        topology coverage.
        """
        available = len(FloatingServers._serverlist)
        self.assertGreaterEqual(
            available, count,
            f"This test rebalances {count} node(s) in but the ini provides "
            f"{available} spare (floating) node(s). Use an ini that lists spare "
            f"servers outside [cluster1]/[cluster2], e.g. "
            f"b/resources/6-nodes-template-xdcr.ini")

    def require_network_blocking(self, dest_cluster):
        """Skip the test unless every target node can actually block traffic.

        These container images ship neither nft nor iptables, so the block would
        silently no-op and the test would report a product failure.
        """
        for node in dest_cluster.get_nodes():
            rc, out = self.run_remote(node, "nft --version")
            if rc != 0:
                self.skipTest(
                    f"nft is unusable on target node {node.ip} (exit {rc}: "
                    f"{out.strip()!r}); install nftables on the lab image, or the "
                    f"block would silently do nothing and the test would report a "
                    f"product failure")

    def block_network_nftables(self, src_cluster, dest_cluster):
        """Drop traffic from the source cluster on every target node, and prove it.

        Registers the blocked nodes first so tearDown can always undo the block,
        even if this method or the test dies half way through.
        """
        src_ips = [node.ip for node in src_cluster.get_nodes()]
        src_nodes_str = ", ".join(src_ips)
        for dest_node in dest_cluster.get_nodes():
            self.network_blocked_nodes.append(dest_node)
            cmd = (f"nft add table inet {self.NFT_TABLE} && "
                   f"nft add chain inet {self.NFT_TABLE} input "
                   r"'{ type filter hook input priority 0 ; policy accept ; }'"
                   f" && nft add rule inet {self.NFT_TABLE} input ip saddr "
                   f"{{ {src_nodes_str} }} drop")
            rc, out = self.run_remote(dest_node, cmd)
            self.assertEqual(
                rc, 0,
                f"Could not install the nftables block on {dest_node.ip}: {out}")
        self.verify_network_blocked(src_cluster, dest_cluster)

    def verify_network_blocked(self, src_cluster, dest_cluster):
        """Confirm the block took effect before asserting on its consequences:
        REST from a source node to a target node must not succeed."""
        src_node = src_cluster.get_nodes()[0]
        dest_node = dest_cluster.get_nodes()[0]
        rc, out = self.run_remote(
            src_node,
            f"curl -s -m 5 -o /dev/null -w '%{{http_code}}' "
            f"http://{dest_node.ip}:8091/pools", timeout=30)
        # A dropped packet makes curl fail to connect: non-zero exit and http_code
        # 000. Anything else means traffic still flows.
        self.assertTrue(
            rc != 0 or "000" in out,
            f"Traffic from {src_node.ip} to {dest_node.ip} still gets through after "
            f"the block was installed (rc={rc}, output {out.strip()!r}); the test "
            f"would otherwise blame the product for a firewall that never applied")
        self.log.info(f"Block confirmed: REST from {src_node.ip} to {dest_node.ip} "
                      f"no longer succeeds")

    def unblock_network_nftables(self, src_cluster=None, dest_cluster=None):
        """Remove the test's nftables table wherever it was installed.

        Log-and-continue on error, never fail: this runs from the test's `finally`
        and from tearDown, where raising would mask the real failure and leave the
        cluster partitioned.
        """
        for dest_node in list(getattr(self, "network_blocked_nodes", [])):
            rc, out = self.run_remote(
                dest_node, f"nft delete table inet {self.NFT_TABLE}")
            if rc == 0:
                self.network_blocked_nodes.remove(dest_node)
                self.log.info(f"Removed the nftables block on {dest_node.ip}")
            else:
                self.log.error(
                    f"Could not remove the nftables block on {dest_node.ip}: {out}; "
                    f"run 'nft delete table inet {self.NFT_TABLE}' there by hand")
