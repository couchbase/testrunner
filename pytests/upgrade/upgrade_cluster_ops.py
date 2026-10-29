import copy
import json
import socket
import ssl
import time
import urllib.parse

from lib.Cb_constants.CBServer import CbServer
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from remote.remote_util import RemoteMachineShellConnection
from pytests.security.ntonencryptionBase import ntonencryptionBase
from pytests.security.upgrade.multiple_CA_upgrade import LdapContainer, MultipleCAUpgrade
from .upgrade_tests import UpgradeTests


QUOTA_KEYS = ["memoryQuota", "indexMemoryQuota", "ftsMemoryQuota",
              "eventingMemoryQuota", "cbasMemoryQuota"]
QUOTA_MINIMUMS = {"indexMemoryQuota": 256, "ftsMemoryQuota": 256,
                  "eventingMemoryQuota": 256, "cbasMemoryQuota": 1024}
SECURITY_KEYS = ["tlsMinVersion", "clusterEncryptionLevel",
                 "honorCipherOrder", "cipherSuites"]
TLS12_ONLY_SUITE = "NORMAL:-VERS-ALL:+VERS-TLS1.2"
# LDAP client TLS configs checked against the TLS 1.2-only endpoint, as
# (maxTLSVersion, middleboxCompMode). "default" behaves like an unset config:
# offer up to TLS 1.3 with middlebox mode on.
LDAP_TLS_VARIANTS = {"default": ("tlsv1.3", "true"),
                     "tls12": ("tlsv1.2", "false"),
                     "tls12_middlebox": ("tlsv1.2", "true")}
# Until these fix versions, ldap_util always passes
# middlebox_comp_mode (true or false), which Erlang 27+ rejects when only
# TLS 1.2 is allowed, so every maxTLSVersion=tlsv1.2 config fails.
LDAP_TLS12_BROKEN_RANGES = [((7, 6, 8), (7, 6, 11)), ((8, 0, 0), (8, 0, 3))]
# RBAC records rewritten by ns_server menelaus_users:upgrade_props
# when the cluster compat version crosses 7.9 (EE only) and 8.5.
RBAC_79_SUBSTITUTIONS = {
    "security_admin_local": ["security_admin", "user_admin_local"],
    "security_admin_external": ["security_admin", "user_admin_external"],
    "ro_admin": ["ro_admin", "ro_security_admin"]}
# menelaus_old_roles:pre_85_ui_roles(): holders of any of these get ui_access
PRE_85_UI_ROLES = {
    "ro_admin", "security_admin", "ro_security_admin", "user_admin_local",
    "user_admin_external", "cluster_admin", "eventing_admin", "backup_admin",
    "bucket_admin", "views_admin", "replication_admin", "fts_admin",
    "fts_searcher", "query_select", "query_update", "query_insert",
    "query_delete", "query_manage_index", "query_list_index",
    "query_system_catalog", "query_external_access",
    "query_manage_global_functions", "query_execute_global_functions",
    "query_manage_functions", "query_execute_functions",
    "query_manage_global_external_functions",
    "query_execute_global_external_functions",
    "query_manage_external_functions", "query_execute_external_functions",
    "query_manage_sequences", "query_use_sequences",
    "query_manage_system_catalog", "analytics_manager", "analytics_reader",
    "analytics_select", "analytics_admin", "eventing_manage_functions"}
# Records created before the upgrade, as (kind, name, roles). Roles are
# written in their pre-7.9 form and substituted up front on 7.9+ sources,
# where the deprecated roles no longer exist.
RBAC_RECORDS = [
    ("group", "cbqe8818_deprecated",
     ["security_admin_local", "security_admin_external", "ro_admin",
      "eventing_admin", "cluster_admin", "backup_admin"]),
    ("group", "cbqe8818_admin", ["admin"]),
    ("group", "cbqe8818_bucket", ["bucket_admin[*]", "data_reader[*]"]),
    ("group", "cbqe8818_data_only", ["data_reader[*]"]),
    ("local", "cbqe8818_local", ["security_admin_local", "ro_admin"]),
    ("external", "cbqe8818_external", ["security_admin_external"])]
RBAC_PASSWORD = "password"
# ns_server rejection reasons (ns_error_messages.erl)
CLUSTER_TOO_OLD_ERROR = "is not supported"
NODE_TOO_OLD_ERROR = "compatibility mismatch"


class UpgradeClusterOpsTests(UpgradeTests):
    """
    NServ upgrade coverage.

    Event methods plug into test_upgrade through the usual event params
    (initialize_events / before_events / in_between_events / after_events):
        capture_cluster_snapshot            - record topology/settings pre-upgrade
        configure_cluster_settings          - n2n, auto-failover, quotas, TLS
                                              floor, retry-rebalance
        verify_cluster_settings_persisted   - settings + topology unchanged
        verify_mixed_mode_compat            - compat stays at source level
        verify_cluster_health_post_upgrade  - membership/version/compat/
                                              topology/orchestrator
        verify_rebalance_orchestration      - rebalance out/in/swap
        verify_autofailover_orchestration   - auto-failover fires and recovers
        verify_fts_index_post_upgrade       - create_fts_index's index serves
                                              every doc (no ES node needed)
        setup_ldap_tls12_endpoint           - TLS 1.2-only LDAPS server
        verify_ldap_connectivity            - LDAP works on every node for
                                              each ldap_variants config
        verify_old_node_join_rejected       - a join_version node (default:
                                              the source version) cannot
                                              join the upgraded cluster
        configure_rbac_roles                - groups/users holding the roles
                                              ns_server migrates
        verify_rbac_roles                   - roles match the migration for
                                              the current compat version

    A failed event is recorded and the rest of the "-" chain keeps running,
    so one upgrade reports every check. Failures are asserted at the end of
    the test, because an event inside the upgrade thread (in_between_events)
    cannot fail the test on its own.

    Multi-hop: upgrade_path=<hop1>[,<hop2>] upgrades through each hop before
    upgrade_version, for online (swap, failover_recovery, incremental) and
    offline upgrades.
    online_upgrade_type=failover_recovery and incremental are implemented
    here: one node at a time, master last, each node keeping its services.
    """

    def setUp(self):
        self.initial_install_done = False
        super(UpgradeClusterOpsTests, self).setUp()
        self.source_version = self.input.param("initial_version", None)
        self.check_failures = []
        self.pre_upgrade_snapshot = None
        self.expected_settings = None
        self.n2n_level = self.input.param("n2n_level", None)
        self.af_timeout = self.input.param("af_timeout", 30)
        self.af_tolerance = self.input.param("af_tolerance", 60)
        self.af_disk_timeout = self.input.param("af_disk_timeout", 30)
        self.af_max_count = self.input.param("af_max_count", 2)
        self.af_recovery_type = self.input.param("af_recovery_type", "delta")
        self.tls_min_version = self.input.param("tls_min_version", "tlsv1.2")
        self.retry_after = self.input.param("retry_rebalance_after", 120)
        self.retry_attempts = self.input.param("retry_rebalance_attempts", 2)
        # The last variant is left applied and goes through the upgrade
        self.ldap_variants = self.input.param(
            "ldap_variants", "default:tls12:tls12_middlebox").split(":")
        self.ldap_container = None
        self.ldap_shell_server = None
        # True when this test installed docker on ldap_host; tearDown removes it
        self.docker_installed = False
        self.ldap_param = None
        self.ldap_applied = None
        self.rbac_created = None
        self.rbac_source_compat = None
        # Search quota for the source cluster, set before create_fts_index.
        # Unset keeps the cluster default, which is 256 MB on small 7.2
        # nodes: too low for FTS to index on 8.x with 3 KV nodes.
        self.fts_memory_quota = None
        if "fts_quota" in self.input.test_params:
            self.fts_memory_quota = int(self.input.param("fts_quota", 0))

    def tearDown(self):
        if self.ldap_container is not None:
            try:
                shell = RemoteMachineShellConnection(self.ldap_shell_server)
                shell.execute_command("docker rm -f {0}"
                                      .format(self.ldap_container.container_name))
                shell.execute_command("docker network disconnect -f bridge {0}"
                                      .format(self.ldap_container.container_name))
                if self.docker_installed:
                    self.uninstall_docker(shell)
                shell.disconnect()
            except Exception as ex:
                self.log.warning("Could not remove ldap container: {0}".format(ex))
        # strict is undone by the base tearDown (CbServer.use_https)
        if self.n2n_level == "all":
            try:
                ntonencryptionBase().disable_nton_cluster([self.master])
            except Exception as ex:
                self.log.warning("Could not disable n2n encryption: {0}".format(ex))
        super(UpgradeClusterOpsTests, self).tearDown()

    def _install(self, servers):
        super(UpgradeClusterOpsTests, self)._install(servers)
        self.initial_install_done = True

    def _bucket_creation(self):
        # basetestcase.setUp skips bucket creation only for upgrade_tests
        # modules; buckets belong on initial_version, created after install.
        if self.initial_install_done:
            # basetestcase.setUp lowered magmaMinMemoryQuota on the cluster
            # the install replaced; 8.0+ rejects small magma buckets without it
            if self.bucket_storage == 'magma' and self.bucket_type != 'ephemeral':
                RestConnection(self.master).set_internalSetting(
                    "magmaMinMemoryQuota", 100)
            super(UpgradeClusterOpsTests, self)._bucket_creation()

    def test_upgrade(self):
        try:
            super(UpgradeClusterOpsTests, self).test_upgrade()
        except Exception:
            if self.check_failures:
                self.log.error("Upgrade checks failed before the upgrade "
                               "error:\n{0}".format("\n".join(self.check_failures)))
            raise
        self._assert_no_check_failures()

    # ------------------------------------------------------------------ #
    # Multi-hop upgrades
    # ------------------------------------------------------------------ #

    def online_upgrade(self):
        if self.online_upgrade_type == "failover_recovery":
            self._for_each_hop(self._failover_recovery_upgrade)
            return
        if self.online_upgrade_type == "incremental":
            self._for_each_hop(self._incremental_upgrade)
            return
        if len(self.upgrade_versions) > 1 and self.online_upgrade_type != "swap":
            raise Exception("Multi-hop online upgrade is supported for "
                            "online_upgrade_type=swap/failover_recovery/"
                            "incremental only")
        self._for_each_hop(super(UpgradeClusterOpsTests, self).online_upgrade,
                           self._prepare_swap_hop)

    def _failover_recovery_upgrade(self):
        """
        online_upgrade_type=failover_recovery: one node at a time, graceful
        failover (hard for non-kv nodes), in-place package upgrade, add back
        with upgrade_recovery_type (delta for kv nodes, full otherwise),
        rebalance. The test master goes last.
        """
        version = self.upgrade_versions[0]
        self.initial_version = version
        recovery_type = self.input.param("upgrade_recovery_type", "delta")
        nodes = self._cluster_servers()
        for server in nodes[1:] + nodes[:1]:
            peer = next(node for node in nodes if node.ip != server.ip)
            rest = RestConnection(peer)
            otp_node = self._otp_node(rest, server)
            has_kv = "kv" in rest.get_nodes_services().get(
                "{0}:{1}".format(server.ip, server.port), [])
            self.log.info("Failover-upgrade {0} to {1} (graceful={2})"
                          .format(server.ip, version, has_kv))
            self.assertTrue(rest.fail_over(otp_node, graceful=has_kv),
                            "Failover of {0} failed".format(server.ip))
            if has_kv:
                self.assertTrue(rest.monitorRebalance(),
                                "Graceful failover of {0} failed".format(server.ip))
            result = self._upgrade(version, server)
            self.assertIsNotNone(result, "Upgrade of {0} failed".format(server.ip))
            rest.add_back_node(otp_node)
            rest.set_recovery_type(otpNode=otp_node,
                                   recoveryType=recovery_type if has_kv else "full")
            self.assertTrue(self.cluster.rebalance([peer] + [node for node in nodes
                                                             if node.ip != peer.ip],
                                                   [], []),
                            "Recovery rebalance of {0} failed".format(server.ip))
            if self.in_between_events:
                self.finish_events(self.run_event(self.in_between_events))
                self.in_between_events = None

    def _incremental_upgrade(self):
        """
        online_upgrade_type=incremental: one node at a time, rebalance out,
        in-place package upgrade, rebalance back in with the same services.
        The test master goes last. The base online_upgrade_incremental
        re-adds nodes without their services and drops the master.
        """
        version = self.upgrade_versions[0]
        self.initial_version = version
        nodes = self._cluster_servers()
        for server in nodes[1:] + nodes[:1]:
            peer = next(node for node in nodes if node.ip != server.ip)
            others = [peer] + [node for node in nodes
                               if node.ip not in (peer.ip, server.ip)]
            services = RestConnection(peer).get_nodes_services()[
                "{0}:{1}".format(server.ip, server.port)]
            self.log.info("Incremental-upgrade {0} ({1}) to {2}"
                          .format(server.ip, services, version))
            self.assertTrue(self.cluster.rebalance(others + [server], [], [server]),
                            "Rebalance out of {0} failed".format(server.ip))
            result = self._upgrade(version, server)
            self.assertIsNotNone(result, "Upgrade of {0} failed".format(server.ip))
            self.assertTrue(self.cluster.rebalance(others, [server], [],
                                                   services=[",".join(services)]),
                            "Rebalance in of {0} failed".format(server.ip))
            if self.in_between_events:
                self.finish_events(self.run_event(self.in_between_events))
                self.in_between_events = None

    def offline_upgrade(self):
        self._for_each_hop(super(UpgradeClusterOpsTests, self).offline_upgrade)

    def _for_each_hop(self, run_hop, prepare_next_hop=None):
        hops = list(self.upgrade_versions)
        for index, hop in enumerate(hops):
            self.upgrade_versions = [hop]
            if index and prepare_next_hop is not None:
                prepare_next_hop()
            self.log.info("Upgrade hop {0}/{1} -> {2}"
                          .format(index + 1, len(hops), hop))
            run_hop()
            if len(hops) > 1:
                self._verify_hop(hop)
        # Later steps (free node install, health checks) use the final version
        self.upgrade_versions = [hops[-1]]

    def _prepare_swap_hop(self):
        """The swap loop picks spares from self.servers[nodes_init:] and skips
        nodes flagged upgraded, so rebuild both for the next hop."""
        cluster = self._cluster_servers()
        free = self._get_free_nodes()
        if len(free) < self.input.param("swap_num_servers", 1):
            raise Exception("Not enough free nodes for the next swap hop")
        for server in cluster + free:
            server.upgraded = False
        self.servers = cluster + free
        self._new_master(cluster[0])

    def _verify_hop(self, hop):
        rest = RestConnection(self.master)
        versions = rest.get_nodes_versions()
        for version in versions:
            self.assertTrue(version.startswith(hop),
                            "Node at {0} after hop to {1}".format(version, hop))
        self.assertTrue(rest.check_cluster_compatibility(self._major_minor(hop)),
                        "Cluster compat did not reach {0}".format(hop))

    # ------------------------------------------------------------------ #
    # Events
    # ------------------------------------------------------------------ #

    def create_fts_index(self, queue=None):
        if self.fts_memory_quota:
            self._run_check("set_fts_quota", self._set_fts_quota)
        super(UpgradeClusterOpsTests, self).create_fts_index(queue)

    def capture_cluster_snapshot(self, queue=None):
        self._run_check("capture_cluster_snapshot",
                        self._capture_cluster_snapshot, queue)

    def configure_cluster_settings(self, queue=None):
        self._run_check("configure_cluster_settings",
                        self._configure_cluster_settings, queue)

    def verify_cluster_settings_persisted(self, queue=None):
        self._run_check("verify_cluster_settings_persisted",
                        self._verify_cluster_settings_persisted, queue)

    def verify_mixed_mode_compat(self, queue=None):
        self._run_check("verify_mixed_mode_compat",
                        self._verify_mixed_mode_compat, queue)

    def verify_cluster_health_post_upgrade(self, queue=None):
        self._run_check("verify_cluster_health_post_upgrade",
                        self._verify_cluster_health_post_upgrade, queue)

    def verify_rebalance_orchestration(self, queue=None):
        self._run_check("verify_rebalance_orchestration",
                        self._verify_rebalance_orchestration, queue)

    def verify_autofailover_orchestration(self, queue=None):
        self._run_check("verify_autofailover_orchestration",
                        self._verify_autofailover_orchestration, queue)

    def setup_ldap_tls12_endpoint(self, queue=None):
        self._run_check("setup_ldap_tls12_endpoint",
                        self._setup_ldap_tls12_endpoint, queue)

    def verify_ldap_connectivity(self, queue=None):
        self._run_check("verify_ldap_connectivity",
                        self._verify_ldap_connectivity, queue)

    def configure_rbac_roles(self, queue=None):
        self._run_check("configure_rbac_roles",
                        self._configure_rbac_roles, queue)

    def verify_rbac_roles(self, queue=None):
        self._run_check("verify_rbac_roles", self._verify_rbac_roles, queue)

    def verify_fts_index_post_upgrade(self, queue=None):
        self._run_check("verify_fts_index_post_upgrade",
                        self._verify_fts_index_post_upgrade, queue)

    def verify_old_node_join_rejected(self, queue=None):
        self._run_check("verify_old_node_join_rejected",
                        lambda: self._assert_join_rejected(
                            self.input.param("join_version", self.source_version),
                            self.input.param("expected_join_error",
                                             NODE_TOO_OLD_ERROR)),
                        queue)

    def _set_fts_quota(self):
        rest = RestConnection(self.master)
        self.assertTrue(rest.set_service_memoryQuota(
            service="ftsMemoryQuota", memoryQuota=self.fts_memory_quota),
            "Failed to set ftsMemoryQuota to {0}".format(self.fts_memory_quota))
        self.assertEqual(rest.get_pools_default().get("ftsMemoryQuota"),
                         self.fts_memory_quota)
        # cbft restarts on a quota change; wait for it to answer again so
        # create_fts_index does not hit a restarting service. 8.x reports the
        # quota it runs with, 7.2 does not report it at all.
        self.sleep(10, "Waiting for cbft to restart with the new quota")
        fts_node = self.get_nodes_from_services_map(
            "fts", servers=self.get_nodes_in_cluster_after_upgrade())
        fts_rest = RestConnection(fts_node)
        expected = self.fts_memory_quota * 1024000
        ram_quota = None
        for _ in range(24):
            status, content, _ = fts_rest._http_request(
                fts_rest.fts_baseUrl + "api/nsstats")
            if status:
                ram_quota = json.loads(content).get("num_bytes_ram_quota")
                if ram_quota in (None, expected):
                    self.log.info("FTS running with ftsMemoryQuota={0}"
                                  .format(self.fts_memory_quota))
                    return
            self.sleep(5, "Waiting for cbft to restart with the new quota")
        self.fail("cbft on {0} did not pick up ftsMemoryQuota={1} (num_bytes_ram_quota={2})"
                  .format(fts_node.ip, self.fts_memory_quota, ram_quota))

    def _capture_cluster_snapshot(self):
        rest = RestConnection(self.master)
        self.pre_upgrade_snapshot = {
            "topology": self._service_topology(rest),
            "buckets": sorted(bucket.name for bucket in rest.get_buckets()),
        }
        self.log.info("Pre-upgrade snapshot: {0}".format(self.pre_upgrade_snapshot))

    def _configure_cluster_settings(self):
        cluster = self._cluster_servers()
        if self.n2n_level:
            # setup_nton_cluster turns auto-failover off, so it goes first
            ntonencryptionBase().setup_nton_cluster(
                cluster, clusterEncryptionLevel=self.n2n_level)
            if self.n2n_level == "strict":
                CbServer.use_https = True
        rest = RestConnection(self.master)
        self.assertTrue(rest.update_autofailover_settings(
            True, self.af_timeout, enable_disk_failure=True,
            disk_timeout=self.af_disk_timeout, maxCount=self.af_max_count),
            "Failed to update auto-failover settings")
        pools = rest.get_pools_default()
        for key, minimum in QUOTA_MINIMUMS.items():
            if key == "ftsMemoryQuota" and self.fts_memory_quota:
                # keep the requested Search quota as the floor
                minimum = self.fts_memory_quota
            current = pools.get(key)
            # Lowering a service quota always fits on the node
            if current and current - 32 >= minimum:
                if not rest.set_service_memoryQuota(service=key,
                                                    memoryQuota=current - 32):
                    self.log.warning("Could not change {0}".format(key))
        status, content = rest.set_min_tls_version(version=self.tls_min_version)
        self.assertTrue(status, "Failed to set tlsMinVersion: {0}".format(content))
        rest.set_retry_rebalance_settings(
            {"enabled": "true", "afterTimePeriod": self.retry_after,
             "maxAttempts": self.retry_attempts})

        self.expected_settings = self._read_cluster_settings(rest)
        self.log.info("Settings to persist across upgrade: {0}"
                      .format(self.expected_settings))
        autofailover = self.expected_settings["autofailover"]
        self.assertEqual(autofailover["timeout"], self.af_timeout)
        self.assertEqual(autofailover["maxCount"], self.af_max_count)
        self.assertEqual(self.expected_settings["security"].get("tlsMinVersion"),
                         self.tls_min_version)
        if self.n2n_level:
            self.assertEqual(
                self.expected_settings["security"].get("clusterEncryptionLevel"),
                self.n2n_level)

    def _verify_cluster_settings_persisted(self):
        self.assertIsNotNone(self.expected_settings,
                             "configure_cluster_settings must run before upgrade")
        rest = RestConnection(self.master)
        actual = self._read_cluster_settings(rest)
        diffs = []
        for section, expected in self.expected_settings.items():
            if section == "n2n_enabled":
                continue
            if actual.get(section) != expected:
                diffs.append("{0}: expected {1}, got {2}"
                             .format(section, expected, actual.get(section)))
        if self.n2n_level:
            disabled = [ip for ip, enabled in actual["n2n_enabled"].items()
                        if not enabled]
            if disabled:
                diffs.append("node-to-node encryption off on {0}".format(disabled))
        if self.n2n_level == "strict" and \
                not ClusterOperationHelper.check_if_services_obey_tls(
                    self._cluster_servers()):
            diffs.append("non-TLS ports reachable externally in strict mode")
        diffs.extend(self._server_tls_floor_violations())
        self.assertFalse(diffs, "Settings changed across upgrade: {0}"
                         .format("; ".join(diffs)))

    def _verify_mixed_mode_compat(self):
        rest = RestConnection(self.master)
        versions = set(rest.get_nodes_versions())
        if len(versions) < 2:
            self.log.info("Cluster is not in mixed mode ({0}); nothing to check"
                          .format(versions))
            return
        source = self._major_minor(self.source_version)
        target = self._major_minor(self._target_version())
        self.assertTrue(rest.check_cluster_compatibility(source),
                        "Mixed-mode compat dropped below {0}".format(source))
        if source != target:
            self.assertFalse(rest.check_cluster_compatibility(target),
                             "Compat moved to {0} before all nodes upgraded"
                             .format(target))
        # Data path must keep working while versions are mixed
        self._load_all_buckets(self.master, self.gen_update, "update", 0)

    def _verify_cluster_health_post_upgrade(self):
        rest = RestConnection(self.master)
        target = self._target_version()
        nodes = rest.get_nodes(get_all_nodes=True)
        for node in nodes:
            self.assertEqual(node.clusterMembership, "active",
                             "{0} is {1}".format(node.ip, node.clusterMembership))
            self.assertEqual(node.status, "healthy",
                             "{0} is {1}".format(node.ip, node.status))
            if not self.upgrade_to_future_build:
                self.assertTrue(node.version.startswith(target),
                                "{0} is on {1}, expected {2}"
                                .format(node.ip, node.version, target))
        self.assertTrue(rest.check_cluster_compatibility(self._major_minor(target)),
                        "Cluster compat is not {0}".format(self._major_minor(target)))
        self.assertEqual(rest._rebalance_progress_status(), "none",
                         "Rebalance still running after upgrade")
        orchestrator = rest.get_orchestrator()
        self.assertIn(orchestrator, [node.ip for node in nodes],
                      "Orchestrator {0} is not a cluster node".format(orchestrator))
        if self.pre_upgrade_snapshot:
            self.assertEqual(self._service_topology(rest),
                             self.pre_upgrade_snapshot["topology"],
                             "Service topology changed across upgrade")
            buckets = sorted(bucket.name for bucket in rest.get_buckets())
            self.assertEqual(buckets, self.pre_upgrade_snapshot["buckets"],
                             "Buckets changed across upgrade")

    def _verify_fts_index_post_upgrade(self, timeout=600):
        """The index create_fts_index made before the upgrade still serves
        every doc in its bucket. run_fts_query_and_compare needs an ES node."""
        name = getattr(self, "name", None)
        self.assertIsNotNone(name, "create_fts_index must run before upgrade")
        fts_node = self.get_nodes_from_services_map(
            "fts", servers=self._cluster_servers())
        self.assertIsNotNone(fts_node, "No fts node in the cluster")
        rest = RestConnection(fts_node)
        # The index covers _default._default only; bucket curr_items also
        # counts the _system scope collections (7.6+)
        n1ql_rest = RestConnection(self.get_nodes_from_services_map(
            "n1ql", servers=self._cluster_servers()))
        count_query = "SELECT RAW COUNT(*) FROM `{0}`._default._default".format(name)
        end_time = time.time() + timeout
        indexed = expected = last_error = None
        while time.time() < end_time:
            # cbft can answer 404/503 for a while after the upgrade
            try:
                expected = n1ql_rest.query_tool(count_query)["results"][0]
                indexed = rest.get_fts_index_doc_count(name)
                last_error = None
            except Exception as ex:
                last_error = ex
                self.sleep(15, "FTS index {0} not ready: {1}".format(name, ex))
                continue
            if indexed == expected:
                break
            self.sleep(15, "FTS index {0}: {1}/{2} docs".format(
                name, indexed, expected))
        self.assertEqual(indexed, expected, "FTS index {0} has {1} of {2} docs{3}"
                         .format(name, indexed, expected,
                                 " (last error: {0!r})".format(last_error)
                                 if last_error else ""))
        hits, _, _, status = rest.run_fts_query(
            name, {"query": {"match_all": {}}, "size": 0})
        self.assertEqual(hits, expected, "match_all on {0} returned {1}: {2}"
                         .format(name, hits, status))

    def _verify_rebalance_orchestration(self):
        rest = RestConnection(self.master)
        victim, services = self._pick_victim_node(rest)
        cluster = self._cluster_servers()
        self.log.info("Rebalance out {0} ({1})".format(victim.ip, services))
        self.assertTrue(self.cluster.rebalance(cluster, [], [victim]),
                        "Rebalance out failed")
        RestHelper(RestConnection(victim)).is_ns_server_running(300)
        remaining = [server for server in cluster if server.ip != victim.ip]
        self.log.info("Rebalance {0} back in".format(victim.ip))
        self.assertTrue(self.cluster.rebalance(remaining, [victim], [],
                                               services=[",".join(services)]),
                        "Rebalance in failed")
        spare = self._spare_on_target_version()
        if spare is not None:
            self.log.info("Swap {0} out for {1}".format(victim.ip, spare.ip))
            self.assertTrue(self.cluster.rebalance(
                self._cluster_servers(), [spare], [victim],
                services=[",".join(services)]), "Swap rebalance failed")
        self.in_servers_pool = self._convert_server_map(self._cluster_servers())

    def _verify_autofailover_orchestration(self):
        rest = RestConnection(self.master)
        original = rest.get_autofailover_settings()
        timeout = original.timeout if original.enabled else self.af_timeout
        if not original.enabled:
            self.assertTrue(rest.update_autofailover_settings(True, timeout),
                            "Failed to enable auto-failover")
        rest.reset_autofailover()
        victim, _ = self._pick_victim_node(rest)
        otp_node = self._otp_node(rest, victim)
        self.log.info("Stopping {0} to trigger auto-failover (timeout {1}s)"
                      .format(victim.ip, timeout))
        shell = RemoteMachineShellConnection(victim)
        start = time.time()
        try:
            shell.stop_server()
            failed_over = self._wait_for_autofailover(
                rest, victim, timeout + self.af_tolerance)
            elapsed = time.time() - start
        finally:
            shell.start_server()
            shell.disconnect()
        self.assertTrue(failed_over, "{0} was not auto-failed-over within {1}s"
                        .format(victim.ip, timeout + self.af_tolerance))
        self.log.info("Auto-failover of {0} took {1:.0f}s".format(victim.ip, elapsed))
        self.assertEqual(rest.get_autofailover_settings().count, 1,
                         "Auto-failover count is not 1")

        RestHelper(RestConnection(victim)).is_ns_server_running(300)
        rest.add_back_node(otp_node)
        rest.set_recovery_type(otpNode=otp_node, recoveryType=self.af_recovery_type)
        self.assertTrue(self.cluster.rebalance(self._cluster_servers(), [], []),
                        "Rebalance after {0} recovery failed"
                        .format(self.af_recovery_type))
        rest.reset_autofailover()
        if not original.enabled:
            rest.update_autofailover_settings(False, original.timeout)

    # ------------------------------------------------------------------ #
    # Gap 2: version-compat enforcement on join
    # ------------------------------------------------------------------ #

    def test_node_join_compat(self):
        """
        Install join_version (default: the upgrade version) on a free node
        and add it to an initial_version cluster.
        expect_join=False: the join must be rejected with expected_join_error
            in the reason (default: the below-minimum-compat message) and the
            cluster left untouched.
        expect_join=True: the node rebalances in, the cluster stays at the
            older compat level, then the node is rebalanced out.
        Rejection by an upgraded cluster runs as the after-event
        verify_old_node_join_rejected, so it reuses a scenario's upgrade.
        """
        join_version = self.input.param("join_version", self._target_version())
        if not self.input.param("expect_join", False):
            self._assert_join_rejected(
                join_version,
                self.input.param("expected_join_error", CLUSTER_TOO_OLD_ERROR))
            return
        spare, joined, reason = self._attempt_join(join_version)
        self.assertTrue(joined, "Join failed: {0}".format(reason))
        rest = RestConnection(self.master)
        self.assertTrue(self.cluster.rebalance(self._cluster_servers(), [], []),
                        "Rebalance in of {0} failed".format(join_version))
        older = min(self._version_tuple(join_version),
                    self._version_tuple(self.source_version))
        self.assertTrue(rest.check_cluster_compatibility(
            "{0}.{1}".format(older[0], older[1])))
        self.assertTrue(self.cluster.rebalance(self._cluster_servers(), [],
                                               [spare]),
                        "Rebalance out of {0} failed".format(spare.ip))
        for node in rest.get_nodes(get_all_nodes=True):
            self.assertEqual(node.clusterMembership, "active")

    def _attempt_join(self, join_version):
        """Install join_version on a free node and addNode it.
        Returns (spare, joined, rejection reason)."""
        free = self._get_free_nodes()
        self.assertTrue(free, "Join check needs a free node")
        spare = free[0]
        initial_version = self.initial_version
        self.initial_version = join_version
        try:
            self._install([spare])
        finally:
            self.initial_version = initial_version
        rest = RestConnection(self.master)
        try:
            rest.add_node(user=self.master.rest_username,
                          password=self.master.rest_password,
                          remoteIp=spare.ip, port=spare.port, services=["kv"])
        except Exception as ex:
            reason = getattr(ex, "_message", None) or str(ex)
            self.log.info("Join of {0} ({1}) rejected: {2}"
                          .format(spare.ip, join_version, reason))
            return spare, False, reason
        return spare, True, None

    def _assert_join_rejected(self, join_version, expected_error=None):
        rest = RestConnection(self.master)
        topology = self._service_topology(rest)
        spare, joined, reason = self._attempt_join(join_version)
        if joined:
            rest.eject_node(self.master.rest_username,
                            self.master.rest_password,
                            self._otp_node(rest, spare, get_all_nodes=True))
        self.assertFalse(joined, "{0} node joined a cluster on {1}"
                         .format(join_version, rest.get_nodes_versions()))
        if expected_error:
            self.assertIn(str(expected_error).lower(), str(reason).lower(),
                          "Unexpected rejection reason")
        for node in rest.get_nodes(get_all_nodes=True):
            self.assertEqual(node.clusterMembership, "active")
        self.assertEqual(self._service_topology(rest), topology,
                         "Cluster topology changed after join attempt")

    # ------------------------------------------------------------------ #
    # Gap 4: TLS 1.2-only LDAP endpoint across the Erlang/TLS bump
    # ------------------------------------------------------------------ #

    def _setup_ldap_tls12_endpoint(self):
        """
        Start a TLS 1.2-only LDAPS server and point
        the cluster at it. verify_ldap_connectivity then checks it from every
        node before, during and after the upgrade. LDAP user authentication
        is covered by the security suites.
        """
        ldap_ip = self._start_tls12_ldap_endpoint()
        ok, detail = self._tls_handshake(ldap_ip, LdapContainer.ssl_port, "TLSv1_3")
        self.assertFalse(ok, "LDAP endpoint accepted TLS 1.3 ({0}); it must be "
                         "TLS 1.2-only for this test".format(detail))
        # TLS 1.2 acceptance is checked by the readiness poll in
        # _start_tls12_ldap_endpoint
        self.ldap_param = MultipleCAUpgrade.get_ldap_params(
            hosts=ldap_ip, port=str(LdapContainer.ssl_port), encryption="TLS",
            bindDN=LdapContainer.bindDN, bindPass=LdapContainer.bindPass,
            serverCertValidation="false",
            userDNMapping='{"template":"cn=%u,dc=example,dc=org"}')
        self._verify_ldap_connectivity()

    def _start_tls12_ldap_endpoint(self):
        """Run the TLS 1.2-only openldap container on ldap_host (default: the
        initial master; docker survives every upgrade flow used here).
        Docker is installed if the host does not have it."""
        ldap_host = self.input.param("ldap_host", self.master.ip)
        self.ldap_shell_server = next(
            (server for server in self.input.servers if server.ip == ldap_host),
            self.master)
        self.ldap_container = LdapContainer()
        self.ldap_container.container_name = "cbqe9138_ldap"
        shell = RemoteMachineShellConnection(self.ldap_shell_server)
        try:
            self.docker_installed = self.ensure_docker(shell)
            _, e = shell.execute_command("docker pull osixia/openldap:1.5.0")
            o, _ = shell.execute_command(
                "docker image inspect -f '{{.Id}}' osixia/openldap:1.5.0")
            self.assertTrue(o, "Could not pull openldap image on {0}: {1}"
                            .format(ldap_host, e))
            shell.execute_command("docker rm -f {0}"
                                  .format(self.ldap_container.container_name))
            # Docker 18.x can keep the removed container's bridge endpoint,
            # which fails the next run with "endpoint ... already exists"
            shell.execute_command("docker network disconnect -f bridge {0}"
                                  .format(self.ldap_container.container_name))
            self.ldap_container.start_ldap_container(
                shell, tls_cipher_suite=self.input.param("ldap_tls_cipher_suite",
                                                         TLS12_ONLY_SUITE))
            ready, detail = False, None
            end_time = time.time() + 60
            while not ready and time.time() < end_time:
                self.sleep(3)
                try:
                    ready, detail = self._tls_handshake(
                        ldap_host, LdapContainer.ssl_port, "TLSv1_2")
                except (ConnectionRefusedError, socket.timeout) as ex:
                    detail = ex
            if not ready:
                o, _ = shell.execute_command("docker logs --tail 20 {0}".format(
                    self.ldap_container.container_name))
                self.fail("openldap on {0}:{1} did not accept TLS 1.2 within "
                          "60s ({2}); container log: {3}".format(
                              ldap_host, LdapContainer.ssl_port, detail, o))
        finally:
            shell.disconnect()
        return ldap_host

    def _verify_ldap_connectivity(self):
        """
        Check the LDAP config left applied by the previous call persisted,
        then apply each ldap_variants config in turn and validate the
        connection from every node. The last variant stays applied.
        """
        self.assertIsNotNone(self.ldap_param,
                             "setup_ldap_tls12_endpoint must run first")
        rest = RestConnection(self.master)
        failures = []
        if self.ldap_applied is not None:
            _, settings = rest.get_ldap_settings()
            actual = (settings.get("maxTLSVersion"),
                      str(settings.get("middleboxCompMode")).lower())
            if actual != LDAP_TLS_VARIANTS[self.ldap_applied]:
                failures.append("{0} LDAP config not persisted: got {1}"
                                .format(self.ldap_applied, actual))
        nodes = rest.get_nodes()
        for variant in self.ldap_variants:
            max_tls, middlebox = LDAP_TLS_VARIANTS[variant]
            param = dict(self.ldap_param, maxTLSVersion=max_tls,
                         middleboxCompMode=middlebox)
            status, content = rest.setup_ldap(param, "")
            if not status:
                failures.append("could not apply {0}: {1}".format(variant, content))
                continue
            self.ldap_applied = variant
            for node in nodes:
                _, content = RestConnection(
                    self._server_for_ip(node.ip)).ldap_validate_conn()
                result = content.get("result") if isinstance(content, dict) else None
                self.log.info("LDAP {0} from {1} ({2}): {3}"
                              .format(variant, node.ip, node.version, content))
                if result == "success":
                    continue
                if max_tls == "tlsv1.2" and \
                        self._in_mb70769_range(node.version):
                    self.log.info("{0} is in the middlebox_comp_mode range; failure expected"
                                  .format(node.version))
                    continue
                failures.append("{0} on {1} ({2}): {3}".format(
                    variant, node.ip, node.version, content))
        self.assertFalse(failures, "LDAP connectivity failed: {0}"
                         .format(failures))

    def _in_mb70769_range(self, version):
        version = self._version_tuple(version)
        return any(low <= version < high for low, high in LDAP_TLS12_BROKEN_RANGES)

    def _server_tls_floor_violations(self):
        """tlsMinVersion must still be enforced on the server ports."""
        port = int(CbServer.ssl_port_map.get("8091", "18091"))
        expectations = {"TLSv1_1": False, "TLSv1_2": True, "TLSv1_3": True}
        if self.tls_min_version == "tlsv1.3":
            expectations["TLSv1_2"] = False
        violations = []
        for node in RestConnection(self.master).get_nodes():
            for version, expected in expectations.items():
                try:
                    ok, detail = self._tls_handshake(node.ip, port, version)
                except (ConnectionRefusedError, socket.timeout) as ex:
                    violations.append("{0}:{1} unreachable: {2}".format(
                        node.ip, port, ex))
                    break
                if ok is None:
                    self.log.info("Skipping {0} on {1}: {2}"
                                  .format(version, node.ip, detail))
                elif ok != expected:
                    violations.append("{0}:{1} {2} handshake {3}: {4}".format(
                        node.ip, port, version,
                        "accepted" if ok else "refused", detail))
        return violations

    @staticmethod
    def _tls_handshake(host, port, version, timeout=10):
        """Returns (True, negotiated), (False, error) when the server refuses
        the version, or (None, reason) when this client cannot offer it."""
        tls_version = getattr(ssl.TLSVersion, version)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        try:
            context.minimum_version = tls_version
            context.maximum_version = tls_version
            if tls_version < ssl.TLSVersion.TLSv1_2:
                context.set_ciphers("DEFAULT:@SECLEVEL=0")
        except (ValueError, ssl.SSLError) as ex:
            return None, "client cannot offer {0}: {1}".format(version, ex)
        try:
            with socket.create_connection((host, port), timeout=timeout) as sock:
                with context.wrap_socket(sock, server_hostname=host) as tls:
                    return True, tls.version()
        except (ConnectionRefusedError, socket.timeout):
            raise
        except (ssl.SSLError, ConnectionResetError, EOFError, OSError) as ex:
            if "no protocols available" in str(ex).lower():
                return None, str(ex)
            return False, str(ex)

    # ------------------------------------------------------------------ #
    # RBAC role migration
    # ------------------------------------------------------------------ #

    def _configure_rbac_roles(self):
        """
        Create the groups/users from RBAC_RECORDS: deprecated roles ns_server
        rewrites when compat crosses 7.9, roles that gain ui_access at 8.5,
        and controls it must leave alone (admin, data_reader). The local user
        is also a member of cbqe8818_deprecated.
        """
        rest = RestConnection(self.master)
        self.rbac_source_compat = self._cluster_compat(rest)
        self.rbac_created = {}
        for kind, name, roles in RBAC_RECORDS:
            if self.rbac_source_compat >= (7, 9):
                roles = self._substitute_79_roles(roles)
            if kind == "group":
                status, content = rest.add_group_role(name, "RBAC upgrade check",
                                                      ",".join(roles))
                self.assertTrue(status, "Could not create group {0}: {1}"
                                .format(name, content))
            elif kind == "local":
                rest.add_set_builtin_user(name, urllib.parse.urlencode(
                    {"name": name, "password": RBAC_PASSWORD,
                     "roles": ",".join(roles), "groups": "cbqe8818_deprecated"}))
            else:
                rest.add_external_user(name, urllib.parse.urlencode(
                    {"name": name, "roles": ",".join(roles)}))
            self.rbac_created[(kind, name)] = roles
        self._verify_rbac_roles()

    def _verify_rbac_roles(self):
        """
        Roles must match what ns_server's migrations produce for the current
        compat version: unchanged in mixed mode, migrated once compat moves.
        The local user must still be able to list users (user_admin_local,
        which replaced security_admin_local).
        """
        self.assertIsNotNone(self.rbac_created,
                             "configure_rbac_roles must run first")
        rest = RestConnection(self.master)
        compat = self._cluster_compat(rest)
        enterprise = rest.is_enterprise_edition()
        actual = self._read_rbac_roles(rest)
        diffs = []
        for key, created in self.rbac_created.items():
            expected = self._expected_rbac_roles(
                created, self.rbac_source_compat, compat, enterprise)
            if actual.get(key) != expected:
                diffs.append("{0} {1}: expected {2}, got {3}".format(
                    key[0], key[1], expected, actual.get(key)))
        user = copy.deepcopy(self.master)
        user.rest_username = "cbqe8818_local"
        user.rest_password = RBAC_PASSWORD
        user_rest = RestConnection(user)
        status, content, _ = user_rest._http_request(
            user_rest.baseUrl + "settings/rbac/users")
        if not status:
            diffs.append("cbqe8818_local cannot list users: {0}".format(content))
        self.assertFalse(diffs, "RBAC roles at compat {0}: {1}"
                         .format(compat, "; ".join(diffs)))

    @staticmethod
    def _read_rbac_roles(rest):
        """Role names per (kind, name); a user's own roles only, not the
        ones it gets from a group."""
        actual = {}
        for kind, name, _ in RBAC_RECORDS:
            if kind == "group":
                status, content = rest.get_group_details(name)
                if status:
                    actual[(kind, name)] = sorted(
                        {role["role"] for role in content.get("roles", [])})
        for user in rest.retrieve_user_roles():
            key = (user.get("domain"), user.get("id"))
            if not str(key[1]).startswith("cbqe8818_"):
                continue
            actual[key] = sorted(
                {role["role"] for role in user.get("roles", [])
                 if any(origin.get("type") == "user"
                        for origin in role.get("origins", [{"type": "user"}]))})
        return actual

    @staticmethod
    def _expected_rbac_roles(roles, source, current, enterprise):
        """Mirror of ns_server menelaus_users:upgrade_props for roles."""
        names = [role.split("[")[0] for role in roles]
        if enterprise and source < (7, 9) <= current:
            names = UpgradeClusterOpsTests._substitute_79_roles(names)
        if source < (8, 5) <= current and \
                any(name in PRE_85_UI_ROLES for name in names):
            names.append("ui_access")
        return sorted(set(names))

    @staticmethod
    def _substitute_79_roles(roles):
        substituted = []
        for role in roles:
            for new_role in RBAC_79_SUBSTITUTIONS.get(role, [role]):
                if new_role not in substituted:
                    substituted.append(new_role)
        return substituted

    @staticmethod
    def _cluster_compat(rest):
        compat = min(int(node.clusterCompatibility) for node in rest.get_nodes())
        return compat // 65536, compat % 65536

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #

    def _run_check(self, name, check, queue=None):
        try:
            check()
        except Exception as ex:
            self.log.error("{0} failed: {1}".format(name, ex))
            self.check_failures.append("{0}: {1}".format(name, ex))
        # Always report success to run_event_in_sequence, which would
        # otherwise skip the rest of the chain; failures are asserted in
        # _assert_no_check_failures.
        if queue is not None:
            queue.put(True)

    def _assert_no_check_failures(self):
        self.assertFalse(self.check_failures, "Upgrade checks failed:\n{0}"
                         .format("\n".join(self.check_failures)))

    def _read_cluster_settings(self, rest):
        autofailover = rest.get_autofailover_settings()
        pools = rest.get_pools_default()
        _, content, _ = rest.get_security_settings()
        security = json.loads(content)
        retry = json.loads(rest.get_retry_rebalance_settings())
        return {
            "autofailover": {
                "enabled": autofailover.enabled,
                "timeout": autofailover.timeout,
                "maxCount": autofailover.maxCount,
                "diskEnabled": autofailover.failoverOnDataDiskIssuesEnabled,
                "diskTimePeriod": autofailover.failoverOnDataDiskIssuesTimeout},
            "quotas": {key: pools[key] for key in QUOTA_KEYS if key in pools},
            "security": {key: security[key] for key in SECURITY_KEYS
                         if key in security},
            "retry_rebalance": {key: retry.get(key) for key in
                                ("enabled", "afterTimePeriod", "maxAttempts")},
            "topology": self._service_topology(rest),
            "n2n_enabled": {node["hostname"]: node.get("nodeEncryption", False)
                            for node in pools.get("nodes", [])},
        }

    @staticmethod
    def _service_topology(rest):
        """Per-node service sets, without addresses: a swap changes IPs."""
        return sorted(tuple(sorted(services))
                      for services in rest.get_nodes_services().values())

    def _cluster_servers(self):
        cluster = self.get_nodes_in_cluster_after_upgrade()
        cluster.sort(key=lambda server: server.ip != self.master.ip)
        return cluster

    def _server_for_ip(self, ip):
        for server in self.input.servers:
            if server.ip == ip:
                return server
        raise Exception("{0} is not in the ini".format(ip))

    def _otp_node(self, rest, server, get_all_nodes=False):
        for node in rest.get_nodes(get_all_nodes=get_all_nodes):
            if node.ip == server.ip:
                return node.id
        raise Exception("{0} is not in the cluster".format(server.ip))

    def _pick_victim_node(self, rest):
        """A kv node that is neither the test master nor the orchestrator,
        preferring the one with the fewest services."""
        orchestrator = rest.get_orchestrator()
        candidates = []
        for node in rest.get_nodes():
            if node.ip == self.master.ip or node.ip == orchestrator \
                    or "kv" not in node.services:
                continue
            candidates.append((len(node.services), node))
        self.assertTrue(candidates, "No kv node available to stop/rebalance")
        node = sorted(candidates, key=lambda candidate: candidate[0])[0][1]
        return self._server_for_ip(node.ip), node.services

    def _wait_for_autofailover(self, rest, server, timeout):
        end_time = time.time() + timeout
        while time.time() < end_time:
            for node in rest.get_nodes(get_all_nodes=True):
                if node.ip == server.ip and \
                        node.clusterMembership == "inactiveFailed":
                    return True
            time.sleep(5)
        return False

    def _spare_on_target_version(self):
        target = self._target_version()
        for server in self._get_free_nodes():
            try:
                if RestConnection(server).get_nodes_self().version.startswith(target):
                    return server
            except Exception as ex:
                self.log.info("Free node {0} not usable: {1}".format(server.ip, ex))
        return None

    def _target_version(self):
        return self.upgrade_versions[-1]

    @staticmethod
    def _version_tuple(version):
        return tuple(int(part) for part in version.split("-")[0].split(".")[:3])

    @staticmethod
    def _major_minor(version):
        return ".".join(version.split("-")[0].split(".")[:2])
