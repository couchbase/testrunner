"""
XDCR CNG (stellar-gateway) integration tests.

Tests validate XDCR replication through CNG acting as a proxy gateway
in front of Couchbase Server. CNG requires full TLS connections and
replications use the couchbase2:// protocol prefix.

Topology:
  C1 (source) <-> C2 (target with CNG), HAProxy load balancer in between.

Requirements:
  - INI file with cluster1 (2 nodes), cluster2 (2 nodes), and a floating
    server for the load balancer.
  - Go toolchain and HAProxy pre-installed on target VMs.

Organisation:
  - CNGXDCRBaseTest is a thin orchestrator. It wires per-concern managers
    from pytests/xdcr/cng/ together at setUp time and exposes a handful
    of assertion helpers that bind observations to the unittest runner.
  - Runnable subclasses (XDCRCNGBasicTests, XDCRCNGResiliencyTests,
    XDCRCNGTopologyTests, XDCRCNGRebalanceFailoverTests) own their own
    test methods — conf files must reference these, not the base.
"""

import gc
import json

from couchbase_helper.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection
from .xdcrnewbasetests import XDCRNewBaseTest, FloatingServers, NodeHelper, \
    REPL_PARAM, CONNECTIVITY_STATUS
from .haproxy_utils import HAPROXY_FRONTEND_PORT
from .cng import (
    CNGInfraRegistry, CertManager, InfraManager, RemoteRefManager,
    ReplicationManager, Diagnostics, TopologyBuilder,
)

import logger

log = logger.Logger.get_logger()

ROLE_REPL_TARGET_ALL = "replication_target[*]"
ROLE_REPL_ADMIN      = "replication_admin"
ROLE_RO_ADMIN        = "ro_admin"


class CNGXDCRBaseTest(XDCRNewBaseTest):
    """Shared orchestration for all CNG XDCR test groups.

    Constructs per-concern managers in setUp, tears them down in reverse.
    Holds only the assertion wrappers that glue manager observations to
    the unittest runner — all mechanical work lives on a manager.

    Must not be referenced directly from conf files; every runnable test
    lives on one of the four group subclasses below.
    """

    _CNG_CHECKPOINT_POLL_INTERVAL_SECS = 10
    _CNG_CHECKPOINT_WAIT_TIMEOUT_SECS = 180

    def suite_setUp(self):
        pass

    def suite_tearDown(self):
        pass

    def setUp(self):
        # Managers are attached before super().setUp() so tearDown's cleanup
        # loop never hits AttributeError if parent setUp fails mid-way.
        self._registry = CNGInfraRegistry(FloatingServers._serverlist)
        self._certs = CertManager()
        self._infra = InfraManager(self._registry, self._certs)
        self._diag = Diagnostics(
            self.log_xdcr_state,
            self.log_replications_state,
            self.log_remote_clusters_state,
            self.log_per_replication_checkpoints,
            self.wait_for_new_checkpoint)
        self._refs = RemoteRefManager(
            self.get_cluster_certificates, self._diag, self._registry,
            active_remote_refs_fn=self.active_remote_refs,
            change_standard_credentials_fn=self.change_standard_remote_ref_credentials)        
        self._replication = None
        self._topology = None        
        NodeHelper.raise_fd_soft_limit(log=log)
        super().setUp()
        self._replication = ReplicationManager(
            self.find_matching_bucket, self.sleep,
            self._value_size, self._num_items,
            get_total_changes_left_fn=self.get_total_changes_left,
            wait_for_backlog_fn=self.wait_for_backlog_to_drain,
            bucket_item_count_fn=self.bucket_item_count)
        self._topology = TopologyBuilder(
            self._infra, self._refs, self.get_cb_cluster_by_name,
            self._all_clusters)

    def tearDown(self):        
        for step_name, step in (
            ("flush_nftables_all_clusters", self._flush_nftables_all_clusters),
            ("infra.cleanup_all", self._infra.cleanup_all),
            ("certs.teardown",
             lambda: self._certs.teardown(self._all_clusters(), self._log_fd_count)),
        ):
            try:
                step()
            except Exception as e:
                log.warning("tearDown step {0} failed: {1}".format(step_name, e))
            finally:
                gc.collect()
        super().tearDown()

    def _log_fd_count(self, label):
        """FD probe for EMFILE diagnosis (passed as callback into certs.teardown)."""
        NodeHelper.log_fd_count(label, log=log)

    def _flush_nftables_all_clusters(self):
        """Safety net: flush nftables on all clusters during teardown."""
        for cluster in self._all_clusters():
            try:
                self.unblock_nftables(cluster)
            except Exception as e:
                log.warning(
                    "unblock_nftables failed on {0}: {1}".format(
                        cluster.get_name(), e))

    def _all_clusters(self):
        return [self.get_cb_cluster_by_name("C{0}".format(i))
                for i in range(1, self.get_num_cb_cluster() + 1)]

 
    def _setup_single_cng_pair(self, multi_backend=False):
        """Stand up CNG in front of C2 with C1 as source.

        Returns (src, dst, infra, rest_src). Callers must route any access
        to CNG/HAProxy through `infra.cng_helpers` / `infra.haproxy_helper`
        rather than caching their own references — that keeps the registry
        as the single source of truth.
        """
        src = self.get_cb_cluster_by_name("C1")
        dst = self.get_cb_cluster_by_name("C2")
        self._infra.bootstrap_targets(
            self._all_clusters(), [dst], multi_backend=multi_backend)
        infra = self._registry.get(dst.get_name())
        return src, dst, infra, RestConnection(src.get_master_node())

    def _setup_cng_replication(self, src_cluster, dest_cluster, lb_ip,
                               rc_name="cng_C1_to_C2", xdcr_params=None):
        """Create a CNG remote ref and start replications for all bucket pairs."""
        self._refs.add_cng_ref(src_cluster, dest_cluster, lb_ip, rc_name)
        return self._replication.start_for_buckets(
            src_cluster, dest_cluster, rc_name, xdcr_params)


    def _expect_connectivity(self, src_rest, rc_name, expected_status, timeout=120):
        self.assertTrue(
            self.wait_for_connectivity_status(
                src_rest, expected_status, rc_name=rc_name, timeout=timeout),
            "Remote ref '{0}' did not reach '{1}' within {2}s".format(
                rc_name, expected_status, timeout))

    def _expect_error_connectivity(self, src_rest, rc_name, timeout=180):
        status = self.wait_for_error_connectivity_status(
            src_rest, rc_name=rc_name, timeout=timeout)
        self.assertIsNotNone(
            status,
            "Remote ref '{0}' did not reach an error state within {1}s".format(
                rc_name, timeout))
        return status

    def _assert_checkpoints_progress(self, rest_src, ckpt_before, phase,
                                     timeout=None):
        """Wait for checkpoint count to increase; fail if it doesn't.

        On failure, dump per-replication checkpoints and the goxdcr scan so
        "no replication is checkpointing" (likely product bug) is
        distinguishable from "counter reset due to restart" (test artifact).
        """
        timeout = timeout or self._CNG_CHECKPOINT_WAIT_TIMEOUT_SECS
        ckpt_after = self._diag.wait_for_new_checkpoint(
            rest_src, ckpt_before,
            self._CNG_CHECKPOINT_POLL_INTERVAL_SECS, timeout)
        if ckpt_after <= ckpt_before:
            log.error("Checkpoints did not progress after {0}: before={1}, "
                      "after={2}. Dumping per-replication detail.".format(
                          phase, ckpt_before, ckpt_after))
            self._diag.log_per_replication_checkpoints(
                rest_src, "checkpoint-stall:{0}".format(phase))
            try:
                self._diag.scan_goxdcr_log(
                    self.get_cb_cluster_by_name("C1"),
                    "checkpoint-stall:{0}".format(phase))
            except Exception as e:
                log.warning("goxdcr scan on checkpoint stall failed: {0}".format(e))
        self.assertGreater(
            ckpt_after, ckpt_before,
            "No new checkpoints after {0} (before={1}, after={2})".format(
                phase, ckpt_before, ckpt_after))
        return ckpt_after

    def _assert_add_remote_cluster_fails(self, src_rest, hostname, port,
                                         name, username, password,
                                         certificate=None):
        """Assert add_remote_cluster raises. On unexpected success, remove
        the stale ref and fail."""
        add_succeeded = False
        try:
            src_rest.add_remote_cluster(
                hostname, str(port), username, password, name,
                demandEncryption=1, certificate=certificate, encryptionType="full")
            add_succeeded = True
        except Exception as e:
            log.info("Expected failure for hostname='{0}': {1}".format(hostname, e))

        if add_succeeded:
            try:
                src_rest.remove_remote_cluster(name)
            except Exception:
                pass
            self.fail(
                "Expected add_remote_cluster to fail for hostname='{0}' "
                "but it succeeded".format(hostname))

    def _assert_only_ref(self, rest, expected_name):
        active = self._refs.active_refs(rest)
        self.assertEqual(
            len(active), 1,
            "Expected exactly 1 active remote ref; found {0}: {1}".format(
                len(active), [r.get('name') for r in active]))
        self.assertEqual(
            active[0]['name'], expected_name,
            "Expected ref '{0}'; found '{1}'".format(
                expected_name, active[0]['name']))

    def _verify_setting(self, rest, src_bucket, dest_bucket, param, expected):
        actual = str(self._replication.get_param(rest, src_bucket, dest_bucket, param)).lower()
        if actual != str(expected).lower():
            self.fail("Setting '{0}' mismatch: expected={1}, actual={2}".format(
                param, expected, actual))

    def _expect_all_refs_ok(self, edges, timeout=180):
        """Assert every edge's remote ref reaches RC_OK."""
        for src, _, rc_name in edges:
            rest = RestConnection(src.get_master_node())
            self._expect_connectivity(
                rest, rc_name, CONNECTIVITY_STATUS.RC_OK, timeout=timeout)


class XDCRCNGBasicTests(CNGXDCRBaseTest):
    """Basic CNG replication, settings, connection-state, scope/collection."""

    def test_bidirectional_cng_with_lb(self):
        """Bidirectional XDCR replication with CNG and HAProxy."""
        src_cluster, dest_cluster, infra, _ = self._setup_single_cng_pair()

        rc_name_fwd = "cng_C1_to_C2"
        self._refs.add_cng_ref(
            src_cluster, dest_cluster, infra.lb_ip, rc_name_fwd)

        rc_name_rev = "standard_C2_to_C1"
        self._refs.add_standard_ref(
            dest_cluster, src_cluster, rc_name_rev)

        src_buckets = src_cluster.get_buckets()
        dest_buckets = dest_cluster.get_buckets()

        for src_bucket in src_buckets:
            dest_bucket = self.find_matching_bucket(src_bucket, dest_buckets)
            self._replication.create_single(
                src_cluster, rc_name_fwd, src_bucket, dest_bucket)
            self._replication.create_single(
                dest_cluster, rc_name_rev, dest_bucket, src_bucket)

        self.load_data_topology()
        self.perform_update_delete()
        self.verify_results()

    def test_remote_ref_switch_to_and_from_cng(self):
        """Switch remote cluster ref between standard and CNG modes.

        Verifies replication continues from the same checkpoints after
        each switch without data loss.
        """
        src_cluster, dest_cluster, infra, rest_src = self._setup_single_cng_pair()

        rc_name = "switchable_ref"

        try:
            # Phase 1: Standard remote ref
            self._refs.add_standard_ref(src_cluster, dest_cluster, rc_name)
            self._replication.start_for_buckets(src_cluster, dest_cluster, rc_name)
            self._diag.log_xdcr_state(rest_src, src_cluster,
                                      "phase1-standard-ref-started")

            gen1 = BlobGenerator("switch-p1-", "switch-p1-", self._value_size,
                                 end=self._num_items)
            src_cluster.load_all_buckets_from_generator(gen1)
            self._wait_for_replication_to_catchup(timeout=300)
            ckpt_before = self.get_successful_checkpoint_count(rest_src)
            self._diag.log_xdcr_state(rest_src, src_cluster,
                                      "phase1-after-load-and-catchup")

            # Phase 2: Switch to CNG
            self._refs.modify_to_cng(
                src_cluster, dest_cluster, infra.lb_ip, rc_name)
            self.assertTrue(
                self.wait_for_connectivity_status(
                    rest_src, CONNECTIVITY_STATUS.RC_OK, rc_name=rc_name),
                "Did not reach RC_OK after switching to CNG")
            self._diag.log_xdcr_state(rest_src, src_cluster,
                                      "phase2-rc-ok-after-cng-switch")

            gen2 = BlobGenerator("switch-p2-", "switch-p2-", self._value_size,
                                 end=self._num_items)
            src_cluster.load_all_buckets_from_generator(gen2)
            self._wait_for_replication_to_catchup(timeout=300)
            self._assert_checkpoints_progress(
                rest_src, ckpt_before, phase="CNG switch")
            self._diag.log_xdcr_state(rest_src, src_cluster,
                                      "phase2-after-load-and-catchup")

            # Phase 3: Switch back to standard
            self._refs.modify_to_standard(src_cluster, dest_cluster, rc_name)
            self.assertTrue(
                self.wait_for_connectivity_status(
                    rest_src, CONNECTIVITY_STATUS.RC_OK, rc_name=rc_name),
                "Did not reach RC_OK after switching back to standard")
            self._diag.log_xdcr_state(rest_src, src_cluster,
                                      "phase3-rc-ok-after-standard-switch")

            gen3 = BlobGenerator("switch-p3-", "switch-p3-", self._value_size,
                                 end=self._num_items)
            src_cluster.load_all_buckets_from_generator(gen3)
            self._wait_for_replication_to_catchup(timeout=300)
            self._diag.log_xdcr_state(rest_src, src_cluster,
                                      "phase3-after-load-and-catchup")
            
            self.sleep(180, "Stabilising replication after CNG→standard "
                           "switch before teardown")
            self._diag.log_xdcr_state(rest_src, src_cluster,
                                      "phase3-post-stabilise-pre-teardown")
            self._diag.scan_goxdcr_log(src_cluster,
                                       "phase3-post-stabilise-pre-teardown",
                                       rc_name=rc_name)
        finally:
            self._cleanup_switchable_ref(rest_src, src_cluster, dest_cluster,
                                         rc_name)

    def _cleanup_switchable_ref(self, rest_src, src_cluster, dest_cluster,
                                rc_name):        
        src_buckets = src_cluster.get_buckets()
        dest_buckets = dest_cluster.get_buckets()

        self._diag.log_xdcr_state(rest_src, src_cluster, "teardown-entry")
        repl_ids_before = [r.get("id") for r in self._diag.log_replications_state(
            rest_src, "teardown-pre-pause")]

        for src_bucket in src_buckets:
            dest_bucket = self.find_matching_bucket(src_bucket, dest_buckets)
            try:
                self._replication.pause(rest_src, src_bucket.name, dest_bucket.name)
            except Exception as e:
                log.warning("pause before teardown failed for {0}->{1}: {2}".format(
                    src_bucket.name, dest_bucket.name, e))       
        self.wait_for_replications_paused(rest_src, timeout=90, interval=5)

        self._diag.log_xdcr_state(rest_src, src_cluster,
                                  "teardown-pre-remove-all-replications")
        try:
            rest_src.remove_all_replications()
        except Exception as e:
            log.warning("remove_all_replications failed: {0}".format(e))
            self._diag.log_xdcr_state(rest_src, src_cluster,
                                      "teardown-remove-all-replications-FAILED")
            self._diag.scan_goxdcr_log(
                src_cluster, "teardown-remove-all-replications-FAILED",
                rc_name=rc_name, repl_ids=repl_ids_before)

        if rest_src.get_replications():
            log.warning("Replications still present after DELETE; killing "
                        "goxdcr on source to clear stuck spec")
            self._diag.log_xdcr_state(rest_src, src_cluster,
                                      "teardown-pre-kill-goxdcr")
            self._diag.scan_goxdcr_log(src_cluster,
                                       "teardown-pre-kill-goxdcr",
                                       rc_name=rc_name, repl_ids=repl_ids_before)
            self.kill_goxdcr_on_cluster(src_cluster, wait_to_recover=True)
            # Replace blind 45s with bounded poll — kill_goxdcr returns
            # once the service is back; specs typically clear within
            # seconds, so the original sleep was almost always wasted.
            self.wait_for_replications_to_clear(rest_src, timeout=45)
            self._diag.log_xdcr_state(rest_src, src_cluster,
                                      "teardown-post-kill-goxdcr")
            try:
                rest_src.remove_all_replications()
            except Exception as e:
                log.warning("remove_all_replications retry failed: {0}".format(e))
                self._diag.scan_goxdcr_log(
                    src_cluster,
                    "teardown-remove-all-replications-retry-FAILED",
                    rc_name=rc_name, repl_ids=repl_ids_before)

        self._diag.log_xdcr_state(rest_src, src_cluster,
                                  "teardown-pre-remove-remote-cluster")
        try:
            rest_src.remove_remote_cluster(rc_name)
        except Exception as e:
            log.warning("remove_remote_cluster({0}) failed: {1}".format(
                rc_name, e))
            self._diag.scan_goxdcr_log(
                src_cluster, "teardown-remove-remote-cluster-FAILED",
                rc_name=rc_name, repl_ids=repl_ids_before)
            self.kill_goxdcr_on_cluster(src_cluster, wait_to_recover=True)
            # Same bounded poll — wait for ANY remaining replication spec
            # to clear before the ref-remove retry, capped at 45s.
            self.wait_for_replications_to_clear(rest_src, timeout=45)
            try:
                rest_src.remove_remote_cluster(rc_name)
            except Exception as e2:
                log.warning("remove_remote_cluster retry failed: {0}".format(e2))
                self._diag.scan_goxdcr_log(
                    src_cluster,
                    "teardown-remove-remote-cluster-retry-FAILED",
                    rc_name=rc_name, repl_ids=repl_ids_before)

        self._diag.log_xdcr_state(rest_src, src_cluster, "teardown-exit")

    def test_modify_replication_settings(self):
        """Modify all major replication settings while running through CNG."""
        src_cluster, dest_cluster, infra, rest_src = self._setup_single_cng_pair()

        rc_name = "cng_settings"
        self._refs.add_cng_ref(
            src_cluster, dest_cluster, infra.lb_ip, rc_name)

        src_bucket = src_cluster.get_buckets()[0]
        dest_bucket = self.find_matching_bucket(
            src_bucket, dest_cluster.get_buckets())
        self._replication.create_single(
            src_cluster, rc_name, src_bucket, dest_bucket)

        gen_initial = BlobGenerator("settings-init-", "settings-init-",
                                    self._value_size, end=self._num_items)
        src_cluster.load_all_buckets_from_generator(gen_initial)
        self._wait_for_replication_to_catchup(timeout=300)

        sb = src_bucket.name
        db = dest_bucket.name

        # Group 1: Pipeline tuning
        tuning_settings = {
            REPL_PARAM.SOURCE_NOZZLES: 4,
            REPL_PARAM.TARGET_NOZZLES: 4,
            REPL_PARAM.BATCH_COUNT: 1024,
            REPL_PARAM.BATCH_SIZE: 4096,
            REPL_PARAM.CHECKPOINT_INTERVAL: 120,
        }
        for param, value in tuning_settings.items():
            self._replication.set_param(rest_src, sb, db, param, value)
            self._verify_setting(rest_src, sb, db, param, value)

        self._replication.pause(rest_src, sb, db)
        self.sleep(15, "Settling pause before resume")
        self._replication.resume(rest_src, sb, db)

        gen_g1 = BlobGenerator("settings-g1-", "settings-g1-",
                               self._value_size, end=self._num_items // 4)
        src_cluster.load_all_buckets_from_generator(gen_g1)
        self._wait_for_replication_to_catchup(timeout=300)

        for param, value in tuning_settings.items():
            self._verify_setting(rest_src, sb, db, param, value)

        # Group 2: Compression and priority
        self._replication.set_param(rest_src, sb, db, REPL_PARAM.COMPRESSION_TYPE, "None")
        self._verify_setting(rest_src, sb, db, REPL_PARAM.COMPRESSION_TYPE, "None")
        self._replication.set_param(rest_src, sb, db, REPL_PARAM.PRIORITY, "High")
        self._verify_setting(rest_src, sb, db, REPL_PARAM.PRIORITY, "High")

        self._replication.pause(rest_src, sb, db)
        self.sleep(15, "Settling pause before resume")
        self._replication.resume(rest_src, sb, db)

        gen_g2 = BlobGenerator("settings-g2-", "settings-g2-",
                               self._value_size, end=self._num_items // 4)
        src_cluster.load_all_buckets_from_generator(gen_g2)
        self._wait_for_replication_to_catchup(timeout=300)

        # Group 3: Advanced filtering
        filter_exp = "REGEXP_CONTAINS(META().id, '^settings-')"
        rest_src.set_xdcr_params(sb, db, {
            REPL_PARAM.FILTER_EXP: filter_exp,
            REPL_PARAM.FILTER_SKIP_RESTREAM: "false"
        })
        self._verify_setting(rest_src, sb, db, REPL_PARAM.FILTER_EXP, filter_exp)

        self._replication.pause(rest_src, sb, db)
        self.sleep(15, "Settling pause before resume")
        self._replication.resume(rest_src, sb, db)

        gen_g3 = BlobGenerator("settings-g3-", "settings-g3-",
                               self._value_size, end=self._num_items // 4)
        src_cluster.load_all_buckets_from_generator(gen_g3)
        self._wait_for_replication_to_catchup(timeout=300)

        # Group 4: Network usage limit
        repl = rest_src.get_replication_for_buckets(sb, db)
        self.set_replication_setting_by_id(
            rest_src, repl["id"], "networkUsageLimit", 50)

        self._replication.pause(rest_src, sb, db)
        self.sleep(15, "Settling pause before resume")
        self._replication.resume(rest_src, sb, db)

        gen_g4 = BlobGenerator("settings-g4-", "settings-g4-",
                               self._value_size, end=self._num_items // 4)
        src_cluster.load_all_buckets_from_generator(gen_g4)
        self._wait_for_replication_to_catchup(timeout=300)

        # Group 5: Explicit mapping
        self._replication.set_param(rest_src, sb, db, REPL_PARAM.EXPLICIT_MAPPING, "true")
        self._verify_setting(rest_src, sb, db, REPL_PARAM.EXPLICIT_MAPPING, "true")
        mapping_rules = json.dumps({"_default._default": "_default._default"})
        self._replication.set_param(rest_src, sb, db, REPL_PARAM.MAPPING_RULES, mapping_rules)

        self._replication.pause(rest_src, sb, db)
        self.sleep(15, "Settling pause before resume")
        self._replication.resume(rest_src, sb, db)

        gen_g5 = BlobGenerator("settings-g5-", "settings-g5-",
                               self._value_size, end=self._num_items // 4)
        src_cluster.load_all_buckets_from_generator(gen_g5)
        self._wait_for_replication_to_catchup(timeout=300)

        # Group 6: Optimistic replication threshold
        self._replication.set_param(rest_src, sb, db, REPL_PARAM.OPTIMISTIC_THRESHOLD, 512)
        self._verify_setting(rest_src, sb, db, REPL_PARAM.OPTIMISTIC_THRESHOLD, 512)

        self._replication.pause(rest_src, sb, db)
        self.sleep(15, "Settling pause before resume")
        self._replication.resume(rest_src, sb, db)
        self._wait_for_replication_to_catchup(timeout=300)

    def test_target_awareness(self):
        """Verify CNG cluster reports incoming connections and source shows outgoing."""
        src_cluster, dest_cluster, infra, rest_src = self._setup_single_cng_pair()

        rest_dest = RestConnection(dest_cluster.get_master_node())
        rc_name = "cng_awareness"
        self._setup_cng_replication(
            src_cluster, dest_cluster, infra.lb_ip, rc_name=rc_name)

        gen = BlobGenerator("aware-", "aware-", self._value_size,
                            end=self._num_items)
        src_cluster.load_all_buckets_from_generator(gen)
        self.wait_interval(30, "Waiting for heartbeats to propagate")

        src_outgoing = self.get_outgoing_replications(rest_src)
        self.assertIsNotNone(src_outgoing,
                             "No outgoing replications reported by source")

        dest_incoming = self.get_incoming_replications(rest_dest)
        for attempt in range(5):
            if dest_incoming is not None:
                break
            self.wait_interval(10, "Polling incoming replications ({0}/5)".format(
                attempt + 1))
            dest_incoming = self.get_incoming_replications(rest_dest)
        self.assertIsNotNone(dest_incoming,
                             "No incoming replications reported by CNG dest cluster")

        src_uuid = RestConnection(
            src_cluster.get_master_node()).get_pools_info()["uuid"]
        for incoming_repl in dest_incoming:
            incoming_src_uuid = incoming_repl.get("sourceClusterUUID", "")
            if incoming_src_uuid:
                self.assertEqual(incoming_src_uuid, src_uuid,
                                 "sourceClusterUUID mismatch")

        self._wait_for_replication_to_catchup(timeout=300)

    def test_connection_state_cng_nftables(self):
        """Verify RC_DEGRADED/RC_ERROR when network to LB is blocked via nftables."""
        src_cluster, dest_cluster, infra, rest_src = self._setup_single_cng_pair()

        rc_name = "cng_connstate_nft"
        self._setup_cng_replication(
            src_cluster, dest_cluster, infra.lb_ip, rc_name=rc_name)

        gen = BlobGenerator("connstate-", "connstate-", self._value_size,
                            end=self._num_items)
        src_cluster.load_all_buckets_from_generator(gen)
        self._wait_for_replication_to_catchup(timeout=300)

        self.assertTrue(
            self.wait_for_connectivity_status(
                rest_src, CONNECTIVITY_STATUS.RC_OK, rc_name=rc_name, timeout=60),
            "Replication not in RC_OK before network block")

        self.block_nftables_traffic(src_cluster, infra.lb_ip)

        error_status = self.wait_for_error_connectivity_status(
            rest_src, rc_name=rc_name, timeout=180)
        self.assertIsNotNone(error_status,
                             "Expected error state after network block")

        if error_status == CONNECTIVITY_STATUS.RC_DEGRADED:
            self.wait_for_connectivity_status(
                rest_src, CONNECTIVITY_STATUS.RC_ERROR,
                rc_name=rc_name, timeout=180)

        self.unblock_nftables(src_cluster)

        self.assertTrue(
            self.wait_for_connectivity_status(
                rest_src, CONNECTIVITY_STATUS.RC_OK, rc_name=rc_name, timeout=180),
            "Replication did not recover to RC_OK after unblocking")

        gen2 = BlobGenerator("connstate-post-", "connstate-post-",
                             self._value_size, end=self._num_items // 2)
        src_cluster.load_all_buckets_from_generator(gen2)
        self._wait_for_replication_to_catchup(timeout=300)

    def test_connection_state_cng_process_kill(self):
        """Verify RC_DEGRADED/RC_ERROR when CNG process is killed."""
        src_cluster, dest_cluster, infra, rest_src = self._setup_single_cng_pair()

        rc_name = "cng_connstate_kill"
        self._setup_cng_replication(
            src_cluster, dest_cluster, infra.lb_ip, rc_name=rc_name)

        gen = BlobGenerator("kill-pre-", "kill-pre-", self._value_size,
                            end=self._num_items)
        src_cluster.load_all_buckets_from_generator(gen)
        self._wait_for_replication_to_catchup(timeout=300)

        self.assertTrue(
            self.wait_for_connectivity_status(
                rest_src, CONNECTIVITY_STATUS.RC_OK, rc_name=rc_name, timeout=60),
            "Replication not in RC_OK before CNG kill")

        infra.cng_helpers[0].stop()

        error_status = self.wait_for_error_connectivity_status(
            rest_src, rc_name=rc_name, timeout=180)
        self.assertIsNotNone(error_status,
                             "Expected error state after CNG process kill")

        if error_status == CONNECTIVITY_STATUS.RC_DEGRADED:
            self.wait_for_connectivity_status(
                rest_src, CONNECTIVITY_STATUS.RC_ERROR,
                rc_name=rc_name, timeout=180)

        infra.cng_helpers[0].start()

        self.assertTrue(
            self.wait_for_connectivity_status(
                rest_src, CONNECTIVITY_STATUS.RC_OK, rc_name=rc_name, timeout=180),
            "Replication did not recover to RC_OK after CNG restart")

        gen2 = BlobGenerator("kill-post-", "kill-post-", self._value_size,
                             end=self._num_items // 2)
        src_cluster.load_all_buckets_from_generator(gen2)
        self._wait_for_replication_to_catchup(timeout=300)

    def test_variable_vbucket(self):
        """CNG with variable vbucket mode and goxdcr kill/restart cycles."""
        src_cluster, dest_cluster, infra, rest_src = self._setup_single_cng_pair()

        rc_name = "cng_vbucket"
        self._setup_cng_replication(
            src_cluster, dest_cluster, infra.lb_ip, rc_name=rc_name)

        gen_initial = BlobGenerator("vb-init-", "vb-init-", self._value_size,
                                    end=self._num_items)
        src_cluster.load_all_buckets_from_generator(gen_initial)
        self._wait_for_replication_to_catchup(timeout=300)

        kill_count = self._input.param("kill_count", 2)
        kill_interval = self._input.param("kill_interval", 30)

        for i in range(kill_count):
            log.info("=== goxdcr kill cycle {0}/{1} ===".format(i + 1, kill_count))
            self.kill_goxdcr_on_cluster(src_cluster, wait_to_recover=False)
            self.kill_goxdcr_on_cluster(dest_cluster, wait_to_recover=True)

            gen = BlobGenerator("vb-kill{0}-".format(i), "vb-kill{0}-".format(i),
                                self._value_size, end=self._num_items // kill_count)
            src_cluster.load_all_buckets_from_generator(gen)
            self.sleep(kill_interval, "Wait after kill {0}".format(i + 1))

        self._wait_for_replication_to_catchup(timeout=600)

    def test_connection_pre_check(self):
        """Verify the connection pre-check utility works with CNG clusters."""
        src_cluster, dest_cluster, infra, rest_src = self._setup_single_cng_pair()

        dest_master = dest_cluster.get_master_node()
        certificate = self.get_cluster_certificates(dest_cluster)
        cng_hostname = "couchbase2://{0}".format(infra.lb_ip)

        result = self._run_connection_pre_check(
            rest_src, cng_hostname, HAPROXY_FRONTEND_PORT,
            dest_master.rest_username, dest_master.rest_password,
            "cng_precheck", certificate=certificate)
        self.assertTrue(result.get("done"), "Connection pre-check did not complete")

        rc_name = "cng_after_precheck"
        self._setup_cng_replication(
            src_cluster, dest_cluster, infra.lb_ip, rc_name=rc_name)

        gen = BlobGenerator("precheck-", "precheck-", self._value_size,
                            end=self._num_items)
        src_cluster.load_all_buckets_from_generator(gen)
        self._wait_for_replication_to_catchup(timeout=300)

    def _run_connection_pre_check(self, rest_src, dest_ip, dest_port,
                                  username, password, name,
                                  certificate=None, encryption=True):
        """Run the connection pre-check utility and poll for result.

        Raises on timeout so callers cannot confuse "not done" with "ok".
        """
        res = rest_src.start_connection_pre_check(
            dest_ip, str(dest_port), username, password, name,
            demandEncryption=1 if encryption else 0,
            certificate=certificate, encryptionType="full")
        task_id = res["taskId"]

        status = {}
        max_polls = 10
        poll_interval = 15
        for _ in range(max_polls):
            self.sleep(poll_interval,
                       "Polling connection pre-check for '{0}'".format(name))
            status = rest_src.connection_pre_check_status(
                username, password, task_id)
            if status.get("done"):
                return status
        self.fail(
            "Connection pre-check for '{0}' did not complete after {1}s; "
            "last status: {2}".format(
                name, max_polls * poll_interval, status))

    def test_lb_and_pod_resiliency(self):
        """LB and CNG pod resiliency: XDCR recovers without manual intervention."""
        src_cluster, dest_cluster, infra, rest_src = self._setup_single_cng_pair()

        rc_name = "cng_resiliency"
        self._setup_cng_replication(
            src_cluster, dest_cluster, infra.lb_ip, rc_name=rc_name)

        # Phase 1: Baseline
        gen_baseline = BlobGenerator("resil-base-", "resil-base-",
                                     self._value_size, end=self._num_items)
        src_cluster.load_all_buckets_from_generator(gen_baseline)
        self._wait_for_replication_to_catchup(timeout=300)
        self.assertTrue(
            self.wait_for_connectivity_status(
                rest_src, CONNECTIVITY_STATUS.RC_OK, rc_name=rc_name, timeout=60),
            "Baseline: replication not in RC_OK state")

        cng = infra.cng_helpers[0]
        haproxy = infra.haproxy_helper

        # Phase 2: CNG pod failure and recovery
        cng.stop()
        self.assertIsNotNone(
            self.wait_for_error_connectivity_status(
                rest_src, rc_name=rc_name, timeout=180),
            "Expected error state after CNG pod kill")

        gen_cng_down = BlobGenerator("resil-cngdown-", "resil-cngdown-",
                                     self._value_size, end=self._num_items // 2)
        src_cluster.load_all_buckets_from_generator(gen_cng_down)

        cng.start()
        self.assertTrue(
            self.wait_for_connectivity_status(
                rest_src, CONNECTIVITY_STATUS.RC_OK, rc_name=rc_name, timeout=180),
            "CNG pod recovery: did not reach RC_OK")
        self._wait_for_replication_to_catchup(timeout=300)

        # Phase 3: HAProxy failure and recovery
        haproxy.stop()
        self.assertIsNotNone(
            self.wait_for_error_connectivity_status(
                rest_src, rc_name=rc_name, timeout=180),
            "Expected error state after HAProxy stop")

        gen_lb_down = BlobGenerator("resil-lbdown-", "resil-lbdown-",
                                    self._value_size, end=self._num_items // 2)
        src_cluster.load_all_buckets_from_generator(gen_lb_down)

        haproxy.start()
        self.assertTrue(
            self.wait_for_connectivity_status(
                rest_src, CONNECTIVITY_STATUS.RC_OK, rc_name=rc_name, timeout=180),
            "HAProxy recovery: did not reach RC_OK")
        self._wait_for_replication_to_catchup(timeout=300)

        # Phase 4: Combined failure
        cng.stop()
        haproxy.stop()
        self.assertIsNotNone(
            self.wait_for_error_connectivity_status(
                rest_src, rc_name=rc_name, timeout=180),
            "Expected error state after combined failure")

        gen_combined = BlobGenerator("resil-combined-", "resil-combined-",
                                     self._value_size, end=self._num_items // 2)
        src_cluster.load_all_buckets_from_generator(gen_combined)

        cng.start()
        haproxy.start()
        self.assertTrue(
            self.wait_for_connectivity_status(
                rest_src, CONNECTIVITY_STATUS.RC_OK, rc_name=rc_name, timeout=180),
            "Combined recovery: did not reach RC_OK")
        self._wait_for_replication_to_catchup(timeout=300)

        # Final verification
        gen_final = BlobGenerator("resil-final-", "resil-final-",
                                  self._value_size, end=self._num_items)
        src_cluster.load_all_buckets_from_generator(gen_final)
        self._wait_for_replication_to_catchup(timeout=300)

    def test_source_bucket_delete_repl_auto_gc(self):
        """Source bucket deletion auto-GCs replication via CNG.

        Phase A: Delete source bucket, verify replication spec is auto-GC'd.
        Phase B: Enable skipReplSpecAutoGc, delete source bucket, verify spec stays.
        """
        src_cluster, dest_cluster, infra, rest_src = self._setup_single_cng_pair()

        rc_name = "cng_auto_gc"
        self._setup_cng_replication(
            src_cluster, dest_cluster, infra.lb_ip, rc_name=rc_name)

        src_bucket_name = src_cluster.get_buckets()[0].name
        dest_bucket_name = dest_cluster.get_buckets()[0].name

        gen_initial = BlobGenerator("autogc-init-", "autogc-init-",
                                    self._value_size, end=self._num_items)
        src_cluster.load_all_buckets_from_generator(gen_initial)
        self._wait_for_replication_to_catchup(timeout=300)

        # Phase A: Default auto-GC
        self.assertTrue(len(rest_src.get_replications()) > 0,
                        "Expected active replications before bucket deletion")

        rest_src.delete_bucket(bucket=src_bucket_name)
        self.assertTrue(
            self.wait_for_replications_to_clear(rest_src, timeout=120),
            "Replication spec not auto-GC'd after source bucket deletion")

        # Phase B: skipReplSpecAutoGc
        rest_src.create_bucket(bucket=src_bucket_name, ramQuotaMB=256,
                               replicaNumber=0, bucketType='membase')
        self.sleep(30, "Waiting for bucket to be ready")

        rep_id = rest_src.start_replication(
            "continuous", src_bucket_name, rc_name,
            rep_type="xmem", toBucket=dest_bucket_name)
        self.sleep(45, "Waiting for replication to establish")

        self.set_replication_setting_by_id(
            rest_src, rep_id, "skipReplSpecAutoGc", "true")
        self.sleep(15, "Waiting for skipReplSpecAutoGc to propagate")

        self.assertTrue(len(rest_src.get_replications()) > 0,
                        "No replication found after creation in Phase B")

        rest_src.delete_bucket(bucket=src_bucket_name)        
        gc_poll_interval = 30
        gc_poll_count = 6
        for poll_idx in range(gc_poll_count):
            repls = rest_src.get_replications()
            self.assertGreater(
                len(repls), 0,
                "Replication spec was GC'd despite skipReplSpecAutoGc "
                "(observed at poll {0}/{1}, ~{2}s after bucket delete)".format(
                    poll_idx + 1, gc_poll_count,
                    poll_idx * gc_poll_interval))
            if poll_idx < gc_poll_count - 1:
                self.sleep(
                    gc_poll_interval,
                    "auto-GC absence poll {0}/{1}".format(
                        poll_idx + 1, gc_poll_count))

        try:
            rest_src.remove_all_replications()
        except Exception as e:
            log.warning("Orphaned replication cleanup: {0}".format(e))

    def test_scope_collection_delete_recreate(self):
        """Scope/collection delete and recreate with CNG replication.

        Verifies default replication continues through CNG after DDL on
        both source and destination clusters.
        """
        src_cluster, dest_cluster, infra, rest_src = self._setup_single_cng_pair()

        rest_dest = RestConnection(dest_cluster.get_master_node())
        rc_name = "cng_scope_coll"
        self._setup_cng_replication(
            src_cluster, dest_cluster, infra.lb_ip, rc_name=rc_name)

        src_bucket = src_cluster.get_buckets()[0].name
        dest_bucket = dest_cluster.get_buckets()[0].name
        test_scope = self._input.param("test_scope", "xdcr_test_scope")
        test_coll = self._input.param("test_collection", "xdcr_test_col")

        # Phase 1: Baseline
        gen_baseline = BlobGenerator("sc-base-", "sc-base-",
                                     self._value_size, end=self._num_items)
        src_cluster.load_all_buckets_from_generator(gen_baseline)
        self._wait_for_replication_to_catchup(timeout=300)

        # Phase 2: Create scope/collection on both clusters
        self.create_scope_and_collection(rest_src, src_bucket, test_scope, test_coll)
        self.create_scope_and_collection(rest_dest, dest_bucket, test_scope, test_coll)
        self.sleep(15, "Propagating scope/collection creation on both clusters")

        self.assertTrue(self.verify_scope_exists(rest_src, src_bucket, test_scope))
        self.assertTrue(self.verify_collection_exists(rest_src, src_bucket, test_scope, test_coll))
        self.assertTrue(self.verify_scope_exists(rest_dest, dest_bucket, test_scope))
        self.assertTrue(self.verify_collection_exists(rest_dest, dest_bucket, test_scope, test_coll))

        gen_create = BlobGenerator("sc-create-", "sc-create-",
                                   self._value_size, end=self._num_items // 4)
        src_cluster.load_all_buckets_from_generator(gen_create)
        self._wait_for_replication_to_catchup(timeout=300)

        # Phase 3: Delete scope on source
        rest_src.delete_collection(src_bucket, test_scope, test_coll)
        self.sleep(9, "Propagating collection delete on source")
        self.delete_scope_with_collections(rest_src, src_bucket, test_scope)
        self.sleep(15, "Propagating scope delete on source")
        self.assertFalse(self.verify_scope_exists(rest_src, src_bucket, test_scope))

        gen_src_del = BlobGenerator("sc-srcdel-", "sc-srcdel-",
                                    self._value_size, end=self._num_items // 4)
        src_cluster.load_all_buckets_from_generator(gen_src_del)
        self._wait_for_replication_to_catchup(timeout=300)

        # Phase 4: Recreate on source
        self.create_scope_and_collection(rest_src, src_bucket, test_scope, test_coll)
        self.sleep(15, "Propagating scope/collection recreate on source")
        self.assertTrue(self.verify_scope_exists(rest_src, src_bucket, test_scope))

        gen_src_recr = BlobGenerator("sc-srcrecr-", "sc-srcrecr-",
                                     self._value_size, end=self._num_items // 4)
        src_cluster.load_all_buckets_from_generator(gen_src_recr)
        self._wait_for_replication_to_catchup(timeout=300)

        # Phase 5: Delete and recreate on destination
        rest_dest.delete_collection(dest_bucket, test_scope, test_coll)
        self.sleep(9, "Propagating collection delete on destination")
        self.delete_scope_with_collections(rest_dest, dest_bucket, test_scope)
        self.sleep(15, "Propagating scope delete on destination")
        self.assertFalse(self.verify_scope_exists(rest_dest, dest_bucket, test_scope))

        gen_dest_del = BlobGenerator("sc-destdel-", "sc-destdel-",
                                     self._value_size, end=self._num_items // 4)
        src_cluster.load_all_buckets_from_generator(gen_dest_del)
        self._wait_for_replication_to_catchup(timeout=300)

        self.create_scope_and_collection(rest_dest, dest_bucket, test_scope, test_coll)
        self.sleep(15, "Propagating scope/collection recreate on destination")
        self.assertTrue(self.verify_scope_exists(rest_dest, dest_bucket, test_scope))

        gen_dest_recr = BlobGenerator("sc-destrecr-", "sc-destrecr-",
                                      self._value_size, end=self._num_items // 4)
        src_cluster.load_all_buckets_from_generator(gen_dest_recr)
        self._wait_for_replication_to_catchup(timeout=300)

        # Phase 6: Delete scope on both clusters
        self.delete_scope_with_collections(rest_src, src_bucket, test_scope)
        self.delete_scope_with_collections(rest_dest, dest_bucket, test_scope)
        self.sleep(15, "Propagating final scope delete on both clusters")

        gen_final = BlobGenerator("sc-final-", "sc-final-",
                                  self._value_size, end=self._num_items)
        src_cluster.load_all_buckets_from_generator(gen_final)
        self._wait_for_replication_to_catchup(timeout=300)

        self.assertTrue(
            self.wait_for_connectivity_status(
                rest_src, CONNECTIVITY_STATUS.RC_OK, rc_name=rc_name, timeout=60),
            "Replication not in RC_OK after all scope/collection operations")


class XDCRCNGResiliencyTests(CNGXDCRBaseTest):
    """Credential rotation, goxdcr kill, hostname validation, burst load,
    ref collision / mutual exclusion, remote-ref churn."""

    def test_cng_strip_roles_stage_creds_revert(self):
        """Strip XDCR user roles, stage secondary creds, verify auto-promotion, revert."""
        items = self._input.param("items", 5000)
        primary_user = self._input.param("primary_user", "cng_xdcr_primary")
        staged_user = self._input.param("staged_user", "cng_xdcr_staged")
        user_password = self._input.param("user_password", "Passw0rd!")
        recovery_timeout = self._input.param("recovery_timeout", 300)

        src_cluster, dest_cluster, infra, rest_src = self._setup_single_cng_pair()

        self._create_xdcr_user(dest_cluster, primary_user, user_password)
        self._create_xdcr_user(dest_cluster, staged_user, user_password)

        rc_name = "cng_staged_creds"
        try:
            self._refs.add_cng_ref_with_creds(
                src_cluster, dest_cluster, infra.lb_ip, rc_name,
                primary_user, user_password)
            self._replication.start_for_buckets(src_cluster, dest_cluster, rc_name)

            load_tasks = self._replication.load_async(src_cluster, count=items)
            self._expect_connectivity(
                rest_src, rc_name, CONNECTIVITY_STATUS.RC_OK, timeout=60)
            ckpt_before = self.get_successful_checkpoint_count(rest_src)
            for task in load_tasks:
                task.result()
            self._wait_for_replication_to_catchup(timeout=300)

            # Stage secondary before triggering auth failure
            self._refs.stage_credentials(rest_src, rc_name, staged_user, user_password)

            src_bucket = src_cluster.get_buckets()[0].name
            dest_bucket = dest_cluster.get_buckets()[0].name

            disruption_tasks = self._replication.load_async(src_cluster, count=items // 2)

            # Pause, strip roles while paused, resume (genuine auth gap)
            self._replication.pause(rest_src, src_bucket, dest_bucket)
            self.sleep(6, "Settle paused state before role strip")
            self._strip_xdcr_roles(dest_cluster, primary_user)
            self.sleep(30, "Role change propagation on target")
            self._replication.resume(rest_src, src_bucket, dest_bucket)

            self._expect_error_connectivity(rest_src, rc_name, timeout=180)
            self._expect_connectivity(
                rest_src, rc_name, CONNECTIVITY_STATUS.RC_OK, timeout=recovery_timeout)

            self._assert_checkpoints_progress(
                rest_src, ckpt_before, phase="staged-creds promotion")

            for task in disruption_tasks:
                task.result()
            self._wait_for_replication_to_catchup(timeout=300)

            # Revert: restore primary user roles; rotate ref back to primary creds
            self._set_user_roles(dest_cluster, primary_user, ROLE_REPL_TARGET_ALL)
            self._refs.change_credentials(
                src_cluster, dest_cluster, rc_name,
                primary_user, user_password, lb_ip=infra.lb_ip)
            self._expect_connectivity(
                rest_src, rc_name, CONNECTIVITY_STATUS.RC_OK, timeout=60)

            _, leak_count = NodeHelper.check_goxdcr_log(
                src_cluster.get_master_node(), user_password, print_matches=False)
            self.assertEqual(leak_count, 0,
                             "User password found in plain text in goxdcr.log")

            self.perform_update_delete()
            self.verify_results()
        finally:
            self._delete_user(dest_cluster, primary_user)
            self._delete_user(dest_cluster, staged_user)

    def test_cng_goxdcr_kill_restart_under_pressure(self):
        """Kill goxdcr repeatedly under continuous load with pause/resume interleaved."""
        items = self._input.param("items", 10000)
        kill_cycles = self._input.param("kill_cycles", 4)
        between_kill_sleep = self._input.param("between_kill_sleep", 15)

        src_cluster, dest_cluster, infra, rest_src = self._setup_single_cng_pair()
        rc_name = "cng_goxdcr_pressure"
        self._setup_cng_replication(
            src_cluster, dest_cluster, infra.lb_ip, rc_name=rc_name)

        src_bucket = src_cluster.get_buckets()[0].name
        dest_bucket = dest_cluster.get_buckets()[0].name

        gen_baseline = BlobGenerator("pressure-base-", "pressure-base-",
                                     self._value_size, end=items)
        src_cluster.load_all_buckets_from_generator(gen_baseline)
        self._wait_for_replication_to_catchup(timeout=300)
        self._expect_connectivity(
            rest_src, rc_name, CONNECTIVITY_STATUS.RC_OK, timeout=60)
        ckpt_before = self.get_successful_checkpoint_count(rest_src)

        all_async_tasks = []

        for i in range(kill_cycles):
            # Rotate kill targets: src -> dest -> both -> src ...
            if i % 3 == 0:
                kill_targets = [src_cluster]
            elif i % 3 == 1:
                kill_targets = [dest_cluster]
            else:
                kill_targets = [src_cluster, dest_cluster]

            log.info("=== Kill cycle {0}/{1}: {2} ===".format(
                i + 1, kill_cycles,
                [t.get_name() for t in kill_targets]))

            tasks = self._replication.load_async(src_cluster, count=items // kill_cycles)
            all_async_tasks.extend(tasks)

            for target in kill_targets:
                self.kill_goxdcr_on_cluster(target, wait_to_recover=False)

            self.sleep(between_kill_sleep,
                       "Post-kill delay, cycle {0}".format(i + 1))

            if i % 2 == 0:
                self._replication.pause_resume_cycle(
                    rest_src, src_bucket, dest_bucket,
                    reason="kill cycle {0}".format(i + 1))

            if not self.wait_for_connectivity_status(
                    rest_src, CONNECTIVITY_STATUS.RC_OK,
                    rc_name=rc_name, timeout=120):
                self.fail(
                    "goxdcr did not recover to RC_OK after kill cycle {0}".format(
                        i + 1))

        for task in all_async_tasks:
            task.result()

        self._assert_checkpoints_progress(
            rest_src, ckpt_before, phase="kill cycles")

        self._wait_for_replication_to_catchup(timeout=900)

        for panic_str in ["panic", "fatal error", "nil pointer"]:
            _, count = NodeHelper.check_goxdcr_log(
                src_cluster.get_master_node(), panic_str, print_matches=True)
            self.assertEqual(
                count, 0,
                "Found '{0}' in goxdcr.log — possible crash detected".format(
                    panic_str))

        self.verify_results()

    def test_cng_target_admin_password_change(self):
        """Rotate target Administrator password in correct and incorrect order."""
        items = self._input.param("items", 5000)
        new_password = self._input.param("new_password", "NewP@ssw0rd!")
        recovery_timeout = self._input.param("recovery_timeout", 180)

        src_cluster, dest_cluster, infra, rest_src = self._setup_single_cng_pair()
        rc_name = "cng_pw_change"
        self._setup_cng_replication(
            src_cluster, dest_cluster, infra.lb_ip, rc_name=rc_name)

        src_bucket = src_cluster.get_buckets()[0].name
        dest_bucket = dest_cluster.get_buckets()[0].name
        dest_master = dest_cluster.get_master_node()
        original_password = dest_master.rest_password

        gen_baseline = BlobGenerator("pwchange-base-", "pwchange-base-",
                                     self._value_size, end=items)
        src_cluster.load_all_buckets_from_generator(gen_baseline)
        self._wait_for_replication_to_catchup(timeout=300)
        self._expect_connectivity(
            rest_src, rc_name, CONNECTIVITY_STATUS.RC_OK, timeout=60)
        ckpt_before = self.get_successful_checkpoint_count(rest_src)

        try:
            # Phase 1 (correct order): change target first, then update source ref
            self._change_admin_password(dest_cluster, new_password)
            self._expect_error_connectivity(rest_src, rc_name, timeout=120)

            tasks_p1 = self._replication.load_async(src_cluster, count=items // 3)
            self._refs.change_credentials(
                src_cluster, dest_cluster, rc_name,
                dest_master.rest_username, new_password, lb_ip=infra.lb_ip)
            self._expect_connectivity(
                rest_src, rc_name, CONNECTIVITY_STATUS.RC_OK, timeout=recovery_timeout)
            for task in tasks_p1:
                task.result()
            self._wait_for_replication_to_catchup(timeout=300)

            self._assert_checkpoints_progress(
                rest_src, ckpt_before, phase="Phase 1 recovery")

            # Phase 2 (wrong order): update source ref first with wrong password -> error
            self._refs.change_credentials(
                src_cluster, dest_cluster, rc_name,
                dest_master.rest_username, "WrongPassword123!", lb_ip=infra.lb_ip)
            self._expect_error_connectivity(rest_src, rc_name, timeout=120)

            # Fix: revert dest to original, update ref to match
            self._change_admin_password(dest_cluster, original_password)
            self._refs.change_credentials(
                src_cluster, dest_cluster, rc_name,
                dest_master.rest_username, original_password, lb_ip=infra.lb_ip)
            self._expect_connectivity(
                rest_src, rc_name, CONNECTIVITY_STATUS.RC_OK, timeout=recovery_timeout)

            # Phase 3 (hostile): password change under in-flight load + pause/resume
            tasks_p3 = self._replication.load_async(src_cluster, count=items // 3)
            self._change_admin_password(dest_cluster, new_password)
            for j in range(3):
                self._replication.pause_resume_cycle(
                    rest_src, src_bucket, dest_bucket,
                    reason="auth-error window, pass/resume {0}".format(j + 1))
            self._refs.change_credentials(
                src_cluster, dest_cluster, rc_name,
                dest_master.rest_username, new_password, lb_ip=infra.lb_ip)
            self._expect_connectivity(
                rest_src, rc_name, CONNECTIVITY_STATUS.RC_OK, timeout=recovery_timeout)
            for task in tasks_p3:
                task.result()
            self._wait_for_replication_to_catchup(timeout=300)

            self.verify_results()
        finally:
            try:
                self._change_admin_password(dest_cluster, original_password)
            except Exception as e:
                log.warning("Password restore in teardown failed: {0}".format(e))

    def test_cng_hostname_format_validation(self):
        """Non-couchbase2:// hostname forms must be rejected by the remote-cluster API."""
        src_cluster, dest_cluster, infra, rest_src = self._setup_single_cng_pair()

        dest_master = dest_cluster.get_master_node()
        certificate = self.get_cluster_certificates(dest_cluster)
        lb_ip = infra.lb_ip
        username = dest_master.rest_username
        password = dest_master.rest_password

        def cleanup(name):
            try:
                rest_src.remove_remote_cluster(name)
            except Exception:
                pass

        # A: plain IP — no protocol prefix
        log.info("Sub-case A: plain IP, no prefix")
        self._assert_add_remote_cluster_fails(
            rest_src, lb_ip, HAPROXY_FRONTEND_PORT, "fmt_plain_ip",
            username, password, certificate=certificate)
        cleanup("fmt_plain_ip")

        # B: couchbases:// prefix (S3-style, not valid for CNG)
        log.info("Sub-case B: couchbases:// prefix")
        self._assert_add_remote_cluster_fails(
            rest_src, "couchbases://{0}".format(lb_ip),
            HAPROXY_FRONTEND_PORT, "fmt_couchbases",
            username, password, certificate=certificate)
        cleanup("fmt_couchbases")

        # C: couchbase:// prefix (N2N-style, should not reach CNG)
        log.info("Sub-case C: couchbase:// prefix")
        self._assert_add_remote_cluster_fails(
            rest_src, "couchbase://{0}".format(lb_ip),
            HAPROXY_FRONTEND_PORT, "fmt_couchbase",
            username, password, certificate=certificate)
        cleanup("fmt_couchbase")

        # D: typo — couchbase2:/ (single slash)
        log.info("Sub-case D: couchbase2:/ (single slash)")
        self._assert_add_remote_cluster_fails(
            rest_src, "couchbase2:/{0}".format(lb_ip),
            HAPROXY_FRONTEND_PORT, "fmt_typo",
            username, password, certificate=certificate)
        cleanup("fmt_typo")

        # E: valid couchbase2:// prefix but aimed at a non-CNG Couchbase port
        log.info("Sub-case E: couchbase2:// at non-CNG port (8091)")
        non_cng_hostname = "couchbase2://{0}".format(dest_master.cluster_ip)
        try:
            rest_src.add_remote_cluster(
                non_cng_hostname, str(dest_master.port),
                username, password, "fmt_wrong_port",
                demandEncryption=1, certificate=certificate, encryptionType="full")
            self.sleep(90, "Observing connectivity for wrong-port CNG ref")
            error_status = self.wait_for_error_connectivity_status(
                rest_src, rc_name="fmt_wrong_port", timeout=60)
            self.assertIsNotNone(
                error_status,
                "Expected error state for couchbase2:// pointed at non-CNG port "
                "(RC_OK sustained — possible server bug)")
            log.info("Sub-case E: add succeeded but correctly entered error "
                     "state '{0}'".format(error_status))
        except Exception as e:
            log.info("Sub-case E: add correctly rejected: {0}".format(e))
        finally:
            cleanup("fmt_wrong_port")

        # Positive control
        log.info("Positive control: couchbase2://{0}:{1}".format(
            lb_ip, HAPROXY_FRONTEND_PORT))
        self._refs.add_cng_ref(
            src_cluster, dest_cluster, lb_ip, "fmt_positive_ctrl")
        self._expect_connectivity(
            rest_src, "fmt_positive_ctrl", CONNECTIVITY_STATUS.RC_OK, timeout=120)
        cleanup("fmt_positive_ctrl")

    def test_cng_n2n_ref_collision(self):
        """CNG and N2N refs to the same target cluster must not coexist."""
        items = self._input.param("items", 500)
        src_cluster, dest_cluster, infra, rest_src = self._setup_single_cng_pair()
        lb_ip = infra.lb_ip
        dest_master = dest_cluster.get_master_node()

        def cleanup():            
            try:
                rest_src.remove_all_replications()
            except Exception as e:
                log.warning("remove_all_replications failed: {0}".format(e))
            try:
                rest_src.remove_all_remote_clusters()
            except Exception as e:
                log.warning("remove_all_remote_clusters failed: {0}".format(e))
            self.sleep(15, "Waiting for ref/replication teardown to settle")

        def cert():
            return self.get_cluster_certificates(dest_cluster)

        # Sub-case A: CNG ref exists; N2N add must fail
        log.info("Sub-case A: CNG -> N2N collision")
        self._refs.add_cng_ref(src_cluster, dest_cluster, lb_ip, "cng_primary")
        self.assertEqual(len(self._refs.active_refs(rest_src)), 1,
                         "Expected exactly 1 ref after CNG add")
        self._assert_add_remote_cluster_fails(
            rest_src, dest_master.cluster_ip, dest_master.port, "n2n_collision",
            dest_master.rest_username, dest_master.rest_password, certificate=cert())
        post_refs = self._refs.active_refs(rest_src)
        self.assertEqual(len(post_refs), 1,
                         "Collision attempt created a second ref")
        self.assertEqual(post_refs[0]['name'], "cng_primary",
                         "Original CNG ref was replaced by collision")
        cleanup()

        # Sub-case B: N2N ref exists; CNG add must fail
        log.info("Sub-case B: N2N -> CNG collision")
        self._refs.add_standard_ref(src_cluster, dest_cluster, "n2n_primary")
        self.assertEqual(len(self._refs.active_refs(rest_src)), 1,
                         "Expected exactly 1 ref after N2N add")
        self._assert_add_remote_cluster_fails(
            rest_src, "couchbase2://{0}".format(lb_ip),
            HAPROXY_FRONTEND_PORT, "cng_collision",
            dest_master.rest_username, dest_master.rest_password, certificate=cert())
        post_refs = self._refs.active_refs(rest_src)
        self.assertEqual(len(post_refs), 1,
                         "CNG collision attempt created a second ref")
        self.assertEqual(post_refs[0]['name'], "n2n_primary",
                         "Original N2N ref was replaced")
        cleanup()

        # Sub-case C: Remove CNG, immediately add N2N — must succeed
        log.info("Sub-case C: CNG removed, N2N add immediately after")
        self._refs.add_cng_ref(src_cluster, dest_cluster, lb_ip, "cng_ephemeral")
        rest_src.remove_remote_cluster("cng_ephemeral")
        self._refs.add_standard_ref(src_cluster, dest_cluster, "n2n_after_cng")
        self._replication.start_for_buckets(src_cluster, dest_cluster, "n2n_after_cng")
        gen_c = BlobGenerator("collision-c-", "collision-c-",
                              self._value_size, end=items)
        src_cluster.load_all_buckets_from_generator(gen_c)
        self._wait_for_replication_to_catchup(timeout=300)
        self.verify_results()
        cleanup()

        # Sub-case D: Duplicate CNG ref
        log.info("Sub-case D: duplicate CNG ref (different name, same target)")
        self._refs.add_cng_ref(src_cluster, dest_cluster, lb_ip, "cng_first")
        self._assert_add_remote_cluster_fails(
            rest_src, "couchbase2://{0}".format(lb_ip),
            HAPROXY_FRONTEND_PORT, "cng_duplicate",
            dest_master.rest_username, dest_master.rest_password, certificate=cert())
        cleanup()

        # Sub-case E: N2N collision attempted against an in-flight CNG replication
        log.info("Sub-case E: N2N collision during active CNG replication")
        self._refs.add_cng_ref(src_cluster, dest_cluster, lb_ip, "cng_live")
        self._replication.start_for_buckets(src_cluster, dest_cluster, "cng_live")
        tasks_e = self._replication.load_async(src_cluster, count=items)
        self._assert_add_remote_cluster_fails(
            rest_src, dest_master.cluster_ip, dest_master.port, "n2n_during_cng",
            dest_master.rest_username, dest_master.rest_password, certificate=cert())
        self.sleep(90, "Verifying CNG replication unaffected by collision attempt")
        self._expect_connectivity(
            rest_src, "cng_live", CONNECTIVITY_STATUS.RC_OK, timeout=60)
        for task in tasks_e:
            task.result()
        self._wait_for_replication_to_catchup(timeout=300)
        cleanup()

    def test_cng_staged_creds_with_goxdcr_kill(self):
        """Staged credentials survive and promote through a goxdcr kill/restart cycle."""
        items = self._input.param("items", 3000)
        primary_user = self._input.param("primary_user", "cng_kill_primary")
        staged_user = self._input.param("staged_user", "cng_kill_staged")
        user_password = self._input.param("user_password", "Passw0rd!")
        recovery_timeout = self._input.param("recovery_timeout", 180)

        src_cluster, dest_cluster, infra, rest_src = self._setup_single_cng_pair()

        self._create_xdcr_user(dest_cluster, primary_user, user_password)
        self._create_xdcr_user(dest_cluster, staged_user, user_password)

        rc_name = "cng_kill_staged"
        try:
            self._refs.add_cng_ref_with_creds(
                src_cluster, dest_cluster, infra.lb_ip, rc_name,
                primary_user, user_password)
            self._replication.start_for_buckets(src_cluster, dest_cluster, rc_name)

            gen_baseline = BlobGenerator("killstage-base-", "killstage-base-",
                                         self._value_size, end=items)
            src_cluster.load_all_buckets_from_generator(gen_baseline)
            self._wait_for_replication_to_catchup(timeout=300)
            self._expect_connectivity(
                rest_src, rc_name, CONNECTIVITY_STATUS.RC_OK, timeout=60)

            self._refs.stage_credentials(rest_src, rc_name, staged_user, user_password)
            self.kill_goxdcr_on_cluster(src_cluster, wait_to_recover=True)

            staged_clusters = rest_src.get_remote_clusters_with_stage()
            found_staged = any(
                isinstance(rc.get("stage"), dict) and
                rc["stage"].get("username") == staged_user
                for rc in staged_clusters
            )
            if not found_staged:
                log.warning("Staged creds not found after goxdcr restart — "
                            "may have been cleared or API format differs")

            tasks = self._replication.load_async(src_cluster, count=items // 2)
            self._strip_xdcr_roles(dest_cluster, primary_user)
            self._expect_error_connectivity(rest_src, rc_name, timeout=120)
            self._expect_connectivity(
                rest_src, rc_name, CONNECTIVITY_STATUS.RC_OK, timeout=recovery_timeout)

            for task in tasks:
                task.result()
            self._wait_for_replication_to_catchup(timeout=300)

            self.kill_goxdcr_on_cluster(src_cluster, wait_to_recover=True)
            self._expect_connectivity(
                rest_src, rc_name, CONNECTIVITY_STATUS.RC_OK, timeout=60)

            self.verify_results()
        finally:
            self._delete_user(dest_cluster, primary_user)
            self._delete_user(dest_cluster, staged_user)

    def test_cng_remote_ref_churn_under_load(self):
        """Add/remove CNG remote ref repeatedly while docs load; no orphans or panics."""
        items = self._input.param("items", 5000)
        churn_cycles = self._input.param("churn_cycles", 5)

        src_cluster, dest_cluster, infra, rest_src = self._setup_single_cng_pair()

        rc_name = "cng_churn_0"
        self._setup_cng_replication(
            src_cluster, dest_cluster, infra.lb_ip, rc_name=rc_name)
        gen_baseline = BlobGenerator("churn-base-", "churn-base-",
                                     self._value_size, end=items)
        src_cluster.load_all_buckets_from_generator(gen_baseline)
        self._wait_for_replication_to_catchup(timeout=300)
        self._expect_connectivity(
            rest_src, rc_name, CONNECTIVITY_STATUS.RC_OK, timeout=60)

        async_tasks = self._replication.load_async(src_cluster, count=items)

        for i in range(churn_cycles):
            log.info("=== Churn cycle {0}/{1} ===".format(i + 1, churn_cycles))
            # ns_server refuses ref DELETE while replications still reference
            # it ("Cannot delete remote cluster X since it is referenced by
            # replications [...]"), so drop replications first, quiesce
            # briefly so goxdcr clears the spec, then delete the ref.
            rest_src.remove_all_replications()
            self.sleep(15, "Quiescing replications before ref removal, "
                          "churn {0}".format(i + 1))
            rest_src.remove_all_remote_clusters()
            self.sleep(15, "After ref removal, churn {0}".format(i + 1))
            rc_name = "cng_churn_{0}".format(i + 1)
            self._setup_cng_replication(
                src_cluster, dest_cluster, infra.lb_ip, rc_name=rc_name)
            self._expect_connectivity(
                rest_src, rc_name, CONNECTIVITY_STATUS.RC_OK, timeout=120)

        for task in async_tasks:
            task.result()

        self._wait_for_replication_to_catchup(timeout=600)

        active = self._refs.active_refs(rest_src)
        self.assertEqual(
            len(active), 1,
            "Expected exactly 1 active remote ref after churn; "
            "found {0}".format(len(active)))

        for panic_str in ["panic", "fatal error", "nil pointer"]:
            _, count = NodeHelper.check_goxdcr_log(
                src_cluster.get_master_node(), panic_str, print_matches=True)
            self.assertEqual(count, 0,
                             "Found '{0}' in goxdcr.log after ref churn".format(
                                 panic_str))

        self.verify_results()

    def test_cng_n2n_ref_mutual_exclusion(self):
        """CNG and N2N refs to the same target are mutually exclusive.

          Phase A: CNG ref exists -> add_remote_cluster(N2N) must fail.
          Phase B: N2N ref exists -> add_remote_cluster(CNG) must fail.
        """
        src_cluster, dest_cluster, infra, rest_src = self._setup_single_cng_pair()

        lb_ip = infra.lb_ip
        dest_master = dest_cluster.get_master_node()
        certificate = self.get_cluster_certificates(dest_cluster)
        username = dest_master.rest_username
        password = dest_master.rest_password

        # Phase A
        log.info("Phase A: CNG exists, adding N2N must fail")
        self._refs.add_cng_ref(
            src_cluster, dest_cluster, lb_ip, "cng_existing")
        self._expect_connectivity(
            rest_src, "cng_existing", CONNECTIVITY_STATUS.RC_OK, timeout=120)

        self._assert_add_remote_cluster_fails(
            rest_src, dest_master.cluster_ip, dest_master.port,
            "n2n_rejected", username, password, certificate=certificate)
        self._assert_only_ref(rest_src, "cng_existing")

        rest_src.remove_all_remote_clusters()
        self.sleep(15, "Settle after Phase A cleanup")

        # Phase B
        log.info("Phase B: N2N exists, adding CNG must fail")
        self._refs.add_standard_ref(
            src_cluster, dest_cluster, "n2n_existing")
        self._expect_connectivity(
            rest_src, "n2n_existing", CONNECTIVITY_STATUS.RC_OK, timeout=120)

        self._assert_add_remote_cluster_fails(
            rest_src, "couchbase2://{0}".format(lb_ip), HAPROXY_FRONTEND_PORT,
            "cng_rejected", username, password, certificate=certificate)
        self._assert_only_ref(rest_src, "n2n_existing")

        # Sanity: surviving N2N ref must still replicate end-to-end
        self._replication.start_for_buckets(
            src_cluster, dest_cluster, "n2n_existing")
        items = self._input.param("items", 500)
        gen = BlobGenerator("mutex-", "mutex-", self._value_size, end=items)
        src_cluster.load_all_buckets_from_generator(gen)
        self._wait_for_replication_to_catchup(timeout=300)
        self.verify_results()

    def test_cng_flow_control_burst_load(self):
        """Burst source mutations against a CPU/memory-limited CNG pod.

        Asserts gRPC flow control — not connection errors — absorbs the
        pressure: backlog rises, connectivity stays RC_OK, CNG stays up,
        backlog drains post-burst, no data loss, no panics.
        """
        items_per_worker = self._input.param("items", 250000)
        docsize = self._input.param("docsize", 512)
        rate_limit = self._input.param("rate_limit", 50000)
        burst_workers = self._input.param("burst_workers", 4)
        burst_sample_window = self._input.param("burst_sample_window", 120)
        burst_timeout = self._input.param("burst_timeout", 3600)
        recovery_timeout = self._input.param("recovery_timeout", 7200)
        drain_timeout = self._input.param("drain_timeout", 10800)
        drain_poll_interval = self._input.param("drain_poll_interval", 60)
        cpu_quota = self._input.param("cpu_quota", "15%")
        memory_max = self._input.param("memory_max", "256M")

        src_cluster, dest_cluster, infra, rest_src = self._setup_single_cng_pair()

        # Tear down unlimited CNG and restart under CPU/memory caps.
        cng = infra.cng_helpers[0]
        cng.stop()
        cng.start_with_limits(cpu_quota=cpu_quota, memory_max=memory_max)

        rc_name = "cng_flow_control"
        self._setup_cng_replication(
            src_cluster, dest_cluster, infra.lb_ip, rc_name=rc_name)
        self._expect_connectivity(
            rest_src, rc_name, CONNECTIVITY_STATUS.RC_OK, timeout=180)

        src_buckets = src_cluster.get_buckets()
        src_master = src_cluster.get_master_node()

        ckpt_before = self.get_successful_checkpoint_count(rest_src)

        loaders = self._replication.start_burst_loaders(
            src_master, src_buckets, items_per_worker, docsize,
            rate_limit, burst_workers)

        try:
            metrics = self._replication.sample_under_pressure(
                src_cluster, rest_src, rc_name,
                self.get_connectivity_status, CONNECTIVITY_STATUS.RC_OK,
                burst_sample_window)

            log.info("Burst metrics: {0}".format(metrics))

            self.assertGreater(
                metrics["peak_backlog"], 0,
                "Expected replication backlog > 0 under burst — CNG pod "
                "was not sufficiently pressured (try lowering cpu_quota "
                "or memory_max, or raising rate_limit)")

            self.assertEqual(
                metrics["connectivity_drops"], 0,
                "Connectivity dropped {0} time(s) during burst — expected "
                "gRPC flow control to keep the stream in RC_OK".format(
                    metrics["connectivity_drops"]))

            self.assertTrue(
                cng.is_running(),
                "CNG process died under burst load — flow control did not "
                "protect it from exhaustion")

            self._replication.wait_for_burst_completion(loaders, timeout=burst_timeout)
        finally:
            for shell, pid, label in loaders:
                try:                    
                    if pid and pid.isdigit():
                        shell.execute_command(
                            "kill -9 {0} 2>/dev/null || true".format(pid))
                    else:
                        log.warning(
                            "burst loader '{0}' had empty/non-numeric PID "
                            "({1!r}); skipping kill".format(label, pid))
                    shell.disconnect()
                except Exception:
                    pass
        
        self._wait_for_replication_to_catchup(timeout=recovery_timeout)
        final_backlog = self._replication.wait_for_backlog_to_drain(
            src_cluster, timeout=drain_timeout,
            poll_interval=drain_poll_interval)
        self.assertEqual(
            final_backlog, 0,
            "Replication did not fully drain after burst "
            "(final changes_left={0})".format(final_backlog))

        self._assert_checkpoints_progress(
            rest_src, ckpt_before, phase="burst absorption")

        # 5. No panics
        for panic_str in ["panic", "fatal error", "nil pointer"]:
            _, count = NodeHelper.check_goxdcr_log(
                src_master, panic_str, print_matches=True)
            self.assertEqual(
                count, 0,
                "Found '{0}' in goxdcr.log after burst — possible crash".format(
                    panic_str))

        self.verify_results()


class XDCRCNGTopologyTests(CNGXDCRBaseTest):
    """Chain / star / ring / mesh CNG-fronted topologies across 3+ clusters.

    Requires b/resources/12-nodes-template-cng-xdcr.ini
    (3 clusters of 2 nodes + 3 LBs + 3 spare floating for rebalance).
    """

    def test_cng_topology_chain(self):
        """Chain: C1 -> CNG(C2) -> CNG(C3). Transitive propagation across two CNG hops."""
        items = self._input.param("items", 2000)
        cluster_names = ["C1", "C2", "C3"]

        edges = self._topology.build_chain(cluster_names)
        self._expect_all_refs_ok(edges)
        for src, dest, rc_name in edges:
            self._replication.start_for_buckets(src, dest, rc_name)

        self._replication.load_into_source_clusters(edges, items, "chain")
        self._replication.verify_edges_converged(edges, expected_per_source=None)

    def test_cng_topology_star(self):
        """Star: C1 hub -> CNG(C2), CNG(C3); bidirectional option also wires spoke -> CNG(C1)."""
        items = self._input.param("items", 2000)
        bidirectional = self._input.param("bidirectional", False)
        hub, spokes = "C1", ["C2", "C3"]

        edges = self._topology.build_star(hub, spokes, bidirectional=bidirectional)
        self._expect_all_refs_ok(edges)
        for src, dest, rc_name in edges:
            self._replication.start_for_buckets(src, dest, rc_name)

        self._replication.load_into_source_clusters(edges, items, "star")
        self._replication.verify_edges_converged(edges, expected_per_source=None)

    def test_cng_topology_ring(self):
        """Ring: C1 -> CNG(C2) -> CNG(C3) -> CNG(C1). Every cluster is both source and target."""
        items = self._input.param("items", 2000)
        cluster_names = ["C1", "C2", "C3"]

        edges = self._topology.build_ring(cluster_names)
        self._expect_all_refs_ok(edges)
        for src, dest, rc_name in edges:
            self._replication.start_for_buckets(src, dest, rc_name)

        self._replication.load_into_source_clusters(edges, items, "ring")
        self._replication.verify_edges_converged(
            edges, expected_per_source=items * len(cluster_names))

    def test_cng_topology_mesh(self):
        """Full mesh: every cluster has a CNG ref to every other cluster."""
        items = self._input.param("items", 100000)
        cluster_names = ["C1", "C2", "C3"]

        edges = self._topology.build_mesh(cluster_names)
        self._expect_all_refs_ok(edges)
        for src, dest, rc_name in edges:
            self._replication.start_for_buckets(src, dest, rc_name)

        self._replication.load_into_source_clusters(edges, items, "mesh")
        self._replication.verify_edges_converged(
            edges, expected_per_source=items * len(cluster_names))


class XDCRCNGRebalanceFailoverTests(CNGXDCRBaseTest):
    """Source/target rebalance and failover scenarios with CNG replication."""

    def _setup_pair_with_replication(self, multi_backend=False):
        """Single C1->CNG(C2) pair plus replication on every bucket."""
        src, dst, infra, rest_src = self._setup_single_cng_pair(
            multi_backend=multi_backend)
        rc_name = "cng_C1_to_C2"
        self._refs.add_cng_ref(src, dst, infra.lb_ip, rc_name)
        self._replication.start_for_buckets(src, dst, rc_name)
        self._expect_connectivity(
            rest_src, rc_name, CONNECTIVITY_STATUS.RC_OK, timeout=180)
        return src, dst, infra, rest_src, rc_name

    def _rebalance_with_load(self, src_cluster, rest_src, rc_name,
                             rebalance_fn, items, label):
        """Run a rebalance operation while loading; verify connectivity and data."""
        tasks = src_cluster.async_load_all_buckets(items, self._value_size)
        rebalance_task = rebalance_fn()
        if rebalance_task is not None:
            rebalance_task.result()
        for task in tasks:
            task.result()
        self._expect_connectivity(
            rest_src, rc_name, CONNECTIVITY_STATUS.RC_OK, timeout=180)
        self._wait_for_replication_to_catchup(timeout=600)
        log.info("Rebalance phase '{0}' complete".format(label))

    def test_cng_source_rebalance_in(self):
        """Rebalance-in nodes on the source while CNG replication runs."""
        items = self._input.param("items", 5000)
        num_nodes = self._input.param("num_rebalance", 1)
        src, _, _, rest_src, rc_name = self._setup_pair_with_replication()

        # Floating servers popped by async_rebalance_in have no node cert
        # loaded; add_node would fail TLS with "unknown CA". Provision
        # certs on the tail N floating servers before kicking rebalance.
        self._certs.provision_pending_floating_servers(
            num_nodes, FloatingServers._serverlist)

        self._rebalance_with_load(
            src, rest_src, rc_name,
            lambda: src.async_rebalance_in(num_nodes=num_nodes),
            items, "source rebalance-in")
        self.verify_results()

    def test_cng_source_rebalance_out(self):
        """Rebalance-out a non-master source node while CNG replication runs."""
        items = self._input.param("items", 5000)
        src, _, _, rest_src, rc_name = self._setup_pair_with_replication()

        self._rebalance_with_load(
            src, rest_src, rc_name,
            lambda: src.async_rebalance_out(num_nodes=1),
            items, "source rebalance-out")
        self.verify_results()

    def test_cng_source_swap_rebalance(self):
        """Swap-rebalance a non-master source node while CNG replication runs."""
        items = self._input.param("items", 5000)
        src, _, _, rest_src, rc_name = self._setup_pair_with_replication()

        # Swap pops one floating server (the joiner) — it needs a node
        # cert before add_node can TLS-handshake with it.
        self._certs.provision_pending_floating_servers(
            1, FloatingServers._serverlist)

        self._rebalance_with_load(
            src, rest_src, rc_name,
            lambda: src.async_swap_rebalance(),
            items, "source swap-rebalance")
        self.verify_results()

    def test_cng_target_rebalance_in_single_cng(self):
        """Rebalance-in on target while CNG stays pinned to the original master."""
        items = self._input.param("items", 5000)
        num_nodes = self._input.param("num_rebalance", 1)
        src, dst, _, rest_src, rc_name = self._setup_pair_with_replication(
            multi_backend=False)

        self._certs.provision_pending_floating_servers(
            num_nodes, FloatingServers._serverlist)
        self._rebalance_with_load(
            src, rest_src, rc_name,
            lambda: dst.async_rebalance_in(num_nodes=num_nodes),
            items, "target rebalance-in (single-backend)")
        self.verify_results()

    def test_cng_target_rebalance_in_multi_cng(self):
        """Rebalance-in on target; newly-added node gets a CNG pod; HAProxy refreshed."""
        items = self._input.param("items", 5000)
        num_nodes = self._input.param("num_rebalance", 1)
        src, dst, infra, rest_src, rc_name = self._setup_pair_with_replication(
            multi_backend=True)

        self._certs.provision_pending_floating_servers(
            num_nodes, FloatingServers._serverlist)

        tasks = src.async_load_all_buckets(items, self._value_size)

        pre_nodes = {n.ip for n in dst.get_nodes()}
        rebalance_task = dst.async_rebalance_in(num_nodes=num_nodes)
        rebalance_task.result()
        new_nodes = [n for n in dst.get_nodes() if n.ip not in pre_nodes]

        key_local, chain_local = self._certs.refresh_cert_for_cluster(
            dst, infra.lb_ip)
        for node in new_nodes:
            helper = self._infra.install_cng_on_node(node, key_local, chain_local)
            infra.cng_helpers.append(helper)
            infra.node_ips.append(node.ip)
        infra.haproxy_helper.setup_multi(backend_ips=infra.node_ips)

        for task in tasks:
            task.result()
        self._expect_connectivity(
            rest_src, rc_name, CONNECTIVITY_STATUS.RC_OK, timeout=180)
        self._wait_for_replication_to_catchup(timeout=600)
        self.verify_results()

    def test_cng_source_graceful_failover(self):
        """Graceful failover + rebalance of a non-master source node; replication resumes."""
        items = self._input.param("items", 5000)
        src, _, _, rest_src, rc_name = self._setup_pair_with_replication()

        tasks = src.async_load_all_buckets(items, self._value_size)
        src.failover_and_rebalance_nodes(num_nodes=1, graceful=True)
        for task in tasks:
            task.result()
        self._expect_connectivity(
            rest_src, rc_name, CONNECTIVITY_STATUS.RC_OK, timeout=180)
        self._wait_for_replication_to_catchup(timeout=600)
        self.verify_results()

    def test_cng_target_failover_non_cng_node(self):
        """Graceful failover of a non-CNG target node (single-backend mode).

        Exercises the failover-log-driven resume path on the target pipeline.
        """
        items = self._input.param("items", 5000)
        src, dst, _, rest_src, rc_name = self._setup_pair_with_replication(
            multi_backend=False)

        gen = BlobGenerator("fo-base-", "fo-base-",
                            self._value_size, end=items)
        src.load_all_buckets_from_generator(gen)
        self._wait_for_replication_to_catchup(timeout=300)

        pre_before = self._diag.grep_goxdcr_for_pre_replicate(src.get_master_node())

        dst.failover_and_rebalance_nodes(num_nodes=1, graceful=True)

        self._expect_connectivity(
            rest_src, rc_name, CONNECTIVITY_STATUS.RC_OK, timeout=300)

        gen2 = BlobGenerator("fo-post-", "fo-post-",
                             self._value_size, end=items // 2)
        src.load_all_buckets_from_generator(gen2)
        self._wait_for_replication_to_catchup(timeout=600)

        # CNG refs must not emit pre-replicate calls — they validate against
        # CNG's failover-log API locally instead.
        pre_after = self._diag.grep_goxdcr_for_pre_replicate(src.get_master_node())
        self.assertLessEqual(
            pre_after, pre_before,
            "Unexpected new pre-replicate calls after target failover "
            "(before={0}, after={1}) — CNG refs should use failover-log "
            "validation instead".format(pre_before, pre_after))

        self.verify_results()

    def test_cng_target_failover_cng_node_recovery(self):
        """Failover the CNG-fronted target master; CNG dies with the node.

          1. Expect RC_ERROR once the CNG backend is gone.
          2. Restart CNG on the new master and re-aim HAProxy at it.
          3. Replication resumes via the CNG failover-log path and drains.
        """
        items = self._input.param("items", 5000)
        recovery_timeout = self._input.param("recovery_timeout", 300)
        src, dst, infra, rest_src, rc_name = self._setup_pair_with_replication(
            multi_backend=False)

        gen = BlobGenerator("cngfo-base-", "cngfo-base-",
                            self._value_size, end=items)
        src.load_all_buckets_from_generator(gen)
        self._wait_for_replication_to_catchup(timeout=300)

        old_cng_node = infra.cng_helpers[0].server

        infra.cng_helpers[0].stop()

        dst.failover_and_rebalance_master(graceful=True)

        self._expect_error_connectivity(rest_src, rc_name, timeout=180)

        outage_tasks = src.async_load_all_buckets(items // 2, self._value_size)

        new_master = dst.get_master_node()
        key_local, chain_local = self._certs.refresh_cert_for_cluster(
            dst, infra.lb_ip)
        new_helper = self._infra.install_cng_on_node(
            new_master, key_local, chain_local)
        infra.cng_helpers = [new_helper]
        infra.node_ips = [new_master.ip]
        infra.haproxy_helper.setup_multi(backend_ips=infra.node_ips)

        self._expect_connectivity(
            rest_src, rc_name, CONNECTIVITY_STATUS.RC_OK, timeout=recovery_timeout)

        for task in outage_tasks:
            task.result()
        self._wait_for_replication_to_catchup(timeout=600)

        for panic_str in ["panic", "fatal error", "nil pointer"]:
            _, count = NodeHelper.check_goxdcr_log(
                src.get_master_node(), panic_str, print_matches=True)
            self.assertEqual(count, 0,
                             "Panic '{0}' after CNG-node failover recovery".format(
                                 panic_str))

        self.verify_results()
        log.info("Old CNG node (failed over): {0}, new CNG master: {1}".format(
            old_cng_node.ip, new_master.ip))

    def test_cng_target_failover_multi_backend(self):
        """Graceful failover of one target node in multi-backend CNG mode.

        HAProxy round-robins across multiple CNG pods; losing one backend
        must not interrupt replication, and failover-log-driven resume
        handles the target-side topology change.
        """
        items = self._input.param("items", 5000)
        src, dst, infra, rest_src, rc_name = self._setup_pair_with_replication(
            multi_backend=True)

        gen = BlobGenerator("mbfo-base-", "mbfo-base-",
                            self._value_size, end=items)
        src.load_all_buckets_from_generator(gen)
        self._wait_for_replication_to_catchup(timeout=300)

        target_node = dst.get_nodes()[-1]
        target_ip = target_node.ip
        matching_helper = next(
            (h for h in infra.cng_helpers if h.server.ip == target_ip), None)
        if matching_helper:
            matching_helper.stop()

        dst.failover_and_rebalance_nodes(num_nodes=1, graceful=True)

        self._expect_connectivity(
            rest_src, rc_name, CONNECTIVITY_STATUS.RC_OK, timeout=180)

        infra.cng_helpers = [h for h in infra.cng_helpers if h.server.ip != target_ip]
        infra.node_ips = [ip for ip in infra.node_ips if ip != target_ip]
        infra.haproxy_helper.setup_multi(backend_ips=infra.node_ips)

        gen2 = BlobGenerator("mbfo-post-", "mbfo-post-",
                             self._value_size, end=items // 2)
        src.load_all_buckets_from_generator(gen2)
        self._wait_for_replication_to_catchup(timeout=600)
        self.verify_results()
