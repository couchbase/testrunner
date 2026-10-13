import json
import os
import re
import tempfile
import threading
import time

from couchbase_helper.cluster import Cluster
from couchbase_helper.documentgenerator import BlobGenerator
from collection.collections_rest_client import CollectionsRest
from collection.collections_stats import CollectionsStats
from ent_backup_restore.enterprise_backup_restore_base import EnterpriseBackupRestoreBase
from membase.api.exception import ServerUnavailableException
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper
from remote.remote_util import RemoteMachineShellConnection
from upgrade.newupgradebasetest import NewUpgradeBaseTest

UPGRADE_TYPES = ["offline", "failover_full_recovery"]


class EnterpriseBackupRestoreUpgradeCollectionsTest(EnterpriseBackupRestoreBase,
                                                    NewUpgradeBaseTest):
    """
    Collections-aware backup/restore across a cluster upgrade.

    A backup of a bucket with user scopes/collections is taken with the
    cbbackupmgr of 'initial_version'.  The cluster (and the backup host) is then
    upgraded to 'upgrade_version' and the archive is restored with the upgraded
    cbbackupmgr into the upgraded cluster and, optionally, into a fresh
    'upgrade_version' cluster.  Per-collection item counts are compared.

    Params:
        initial_version, upgrade_version: old/new server builds (required)
        upgrade_type: offline (default) or failover_full_recovery
        num_scopes / num_collections: user scopes and collections per scope
        restore_to_fresh_cluster: also restore into a fresh node (needs 3 servers)
        backup_after_upgrade: after the first restore, add data in a new collection,
            take a new backup with the upgraded cbbackupmgr and restore the whole
            repo (old + new backup) into a recreated bucket (default True)
        default-bucket-ram-quota: bucket quota in MiB (default 1024: Magma needs >= 1024 on 8.0+)
    """

    def setUp(self):
        super().setUp()
        for server in [self.backupset.backup_host, self.backupset.restore_cluster_host]:
            conn = RemoteMachineShellConnection(server)
            conn.extract_remote_info()
            conn.terminate_processes(conn.info, ["cbbackupmgr"])
            conn.disconnect()
        self.document_type = self.input.param("document_type", "json")
        self.bucket_quota = self.input.param("default-bucket-ram-quota", 1024)

    def tearDown(self):
        super().tearDown()

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    def _cbbackupmgr_release(self):
        shell = RemoteMachineShellConnection(self.backupset.backup_host)
        output, error = shell.execute_command(
            "{0}/cbbackupmgr --version".format(self.cli_command_location))
        shell.disconnect()
        self.log.info("cbbackupmgr on backup host: {0} {1}".format(output, error))
        match = re.search(r"(\d+\.\d+)\.\d+", " ".join(output + error))
        if not match:
            self.fail("Unable to parse cbbackupmgr version from {0} {1}"
                      .format(output, error))
        return match.group(1)

    def _install(self, servers):
        """ Installs initial_version. On a slow first start ns_server can die
        while it boots (ns_couchdb times out) although the service is "active";
        the install then never answers on 8091 and, as the base setUp needs
        every node up, takes every following test down with it. Restart the
        service once and wait for it before giving up. """
        try:
            super()._install(servers)
            return
        except ServerUnavailableException as e:
            self.log.error("Install left ns_server unreachable ({0}), "
                           "restarting couchbase-server".format(e))
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            shell.execute_command("systemctl restart couchbase-server")
            shell.disconnect()
            deadline = time.time() + 180
            while True:
                try:
                    if RestHelper(RestConnection(server)).is_ns_server_running(30):
                        break
                except ServerUnavailableException:
                    pass
                if time.time() > deadline:
                    self.fail("ns_server on {0} is still not running after a "
                              "restart".format(server.ip))
                self.sleep(5, "Wait for ns_server on {0}".format(server.ip))

    def _ensure_provisioned(self, nodes):
        """ Right after an install of some old versions a node can be up but not
        initialised yet ("Adding nodes to not provisioned nodes is not allowed") """
        for node in nodes:
            rest = RestConnection(node)
            for _ in range(6):
                status, content, _ = rest._http_request(rest.baseUrl + "pools/default", "GET")
                if status or "unknown pool" not in str(content):
                    break
                self.log.info("{0} is not provisioned yet, initialising it".format(node.ip))
                rest.init_node(set_node_services="kv")
                self.sleep(5, "Let the node settle after initialisation")

    def _wait_for_bucket_ready(self, node):
        if not BucketOperationHelper.wait_for_vbuckets_ready_state(node, "default"):
            self.fail("Bucket 'default' is not ready on {0}".format(node.ip))

    def _log_backup_info(self):
        shell = RemoteMachineShellConnection(self.backupset.backup_host)
        output, error = shell.execute_command(
            "{0}/cbbackupmgr info --archive {1} --repo {2} --all"
            .format(self.cli_command_location, self.backupset.directory, self.backupset.name),
            timeout=600)
        shell.disconnect()
        self.log.info("cbbackupmgr info --all:\n{0}\n{1}".format("\n".join(output), error))

    def _create_bucket(self, node):
        rest = RestConnection(node)
        rest.create_bucket(bucket="default", ramQuotaMB=self.bucket_quota,
                           replicaNumber=0)
        self.sleep(5)
        self.buckets = rest.get_buckets()

    def _load_collection(self, scope, collection, prefix):
        """ cbc-pillowfight is shipped with every server build that has collections """
        cmd = ("/opt/couchbase/bin/cbc-pillowfight -U couchbase://localhost/default "
               "-u {0} -P {1} -I {2} -m 256 -M 256 -r 100 -B 100 -t 1 --sequential "
               "-p {3} --populate-only --collection {4}.{5} --json"
               .format(self.master.rest_username, self.master.rest_password,
                       self.num_items, prefix, scope, collection))
        shell = RemoteMachineShellConnection(self.master)
        output, error = shell.execute_command(cmd, timeout=1800)
        shell.log_command_output(output, error)
        shell.disconnect()

    def _collection_counts(self, nodes, layout):
        """ Per-collection item counts summed over nodes (buckets have 0 replicas) """
        counts = {}
        for scope, collection in layout:
            total = 0
            for node in nodes:
                try:
                    total += CollectionsStats(node).get_collection_item_count_cumulative(
                        "default", scope, collection, node=node)
                except Exception as e:
                    self.log.info("No stats for {0}.{1} on {2}: {3}"
                                  .format(scope, collection, node.ip, e))
            counts[(scope, collection)] = total
        return counts

    def _wait_for_counts(self, nodes, layout, expected, stage, timeout=900):
        elapsed = 0
        actual = {}
        while elapsed <= timeout:
            actual = self._collection_counts(nodes, layout)
            if actual == expected:
                self.log.info("{0}: per-collection counts match: {1}"
                              .format(stage, actual))
                return
            self.sleep(15, "{0}: counts {1} != expected {2}".format(stage, actual, expected))
            elapsed += 15
        self.fail("{0}: per-collection counts differ after {1}s.\nExpected: {2}\nActual: {3}"
                  .format(stage, timeout, expected, actual))

    def _restore_with_current_cbbackupmgr(self, host, auto_create_buckets=False):
        cmd = ("{0}/cbbackupmgr restore --archive {1} --repo {2} --cluster http://{3}:{4} "
               "--username {5} --password {6} --force-updates --no-progress-bar"
               .format(self.cli_command_location, self.backupset.directory,
                       self.backupset.name, host.cluster_ip, host.port,
                       host.rest_username, host.rest_password))
        if auto_create_buckets:
            cmd += " --auto-create-buckets"
        shell = RemoteMachineShellConnection(self.backupset.backup_host)
        output, error = shell.execute_command(cmd, timeout=3600)
        shell.log_command_output(output, error)
        shell.disconnect()
        if not self._check_output("Restore completed successfully", output):
            self.fail("Restore into {0} failed.\nOutput: {1}\nError: {2}"
                      .format(host.ip, output, error))

    def _run_upgrade(self, upgrade_version, servers):
        """ Upgrade the servers and verify the package was really installed.

        The package pre-install script can reject the upgrade (for example a
        direct 7.1.x -> 8.5 upgrade is not supported, only 7.2.0 or newer can be
        upgraded to 8.5) yet the upgrade helper still logs the upgrade as
        completed and the node comes back on the old binaries. """
        for th in self._async_update(upgrade_version, servers):
            th.join()
        wrong = []
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            output, _ = shell.execute_command("cat /opt/couchbase/VERSION.txt")
            shell.disconnect()
            installed = " ".join(output).strip()
            self.log.info("{0}: installed version after upgrade is {1}"
                          .format(server.ip, installed))
            if not installed.startswith(upgrade_version[:upgrade_version.find("-")]):
                wrong.append((server.ip, installed))
        if wrong:
            self.fail("Upgrade to {0} did not take effect on {1}. Couchbase Server "
                      "does not allow a direct upgrade from every older release "
                      "(e.g. 7.1.x -> 8.5 is not supported, the package pre-install "
                      "script rejects it; upgrade to 7.2.0 or newer first)."
                      .format(upgrade_version, wrong))

    def _upgrade_nodes(self, upgrade_version, cluster_nodes, extra_nodes):
        if self.upgrade_type == "offline":
            self._run_upgrade(upgrade_version, cluster_nodes + extra_nodes)
            return
        # failover_full_recovery: one node at a time, driven from a node that
        # is not being upgraded
        for node in cluster_nodes:
            driver = [n for n in cluster_nodes if n.ip != node.ip][0]
            rest = RestConnection(driver)
            otp = [s for s in rest.node_statuses() if s.ip == node.ip][0].id
            self.log.info("Failing over {0}".format(node.ip))
            if not rest.fail_over(otp, graceful=False):
                self.fail("Failover of {0} failed".format(node.ip))
            self.sleep(10, "Let failover settle")
            self._run_upgrade(upgrade_version, [node])
            rest.add_back_node(otp)
            rest.set_recovery_type(otp, "full")
            self.cluster.rebalance(cluster_nodes, [], [])
            self.sleep(15, "Let the cluster settle")
        if extra_nodes:
            self._run_upgrade(upgrade_version, extra_nodes)

    # ------------------------------------------------------------------
    # test
    # ------------------------------------------------------------------
    def test_backup_restore_collections_after_upgrade(self):
        """
        1. Install initial_version, create a bucket with scopes/collections and
           load every collection
        2. Back up with the initial_version cbbackupmgr
        3. Upgrade the cluster and the backup host to upgrade_version
        4. Restore with the upgraded cbbackupmgr into the recreated bucket on the
           upgraded cluster and, optionally, into a fresh upgrade_version node
        5. Compare per-collection item counts with those at backup time
        6. Optionally take a new backup on the upgraded cluster after adding a new
           collection, check it succeeds and that the whole repo restores
        """
        upgrade_version = self.input.param("upgrade_version", None)
        if not upgrade_version:
            self.fail("Need param 'upgrade_version'")
        self.upgrade_type = self.input.param("upgrade_type", "offline")
        if self.upgrade_type not in UPGRADE_TYPES:
            self.fail("upgrade_type must be one of {0}".format(UPGRADE_TYPES))
        num_scopes = self.input.param("num_scopes", 2)
        num_collections = self.input.param("num_collections", 2)
        restore_fresh = self.input.param("restore_to_fresh_cluster", True)
        backup_after_upgrade = self.input.param("backup_after_upgrade", True)
        if restore_fresh and len(self.servers) < 3:
            self.fail("restore_to_fresh_cluster needs a third server in [servers]")

        cluster_nodes = self.servers[:2]
        fresh_node = self.servers[2] if restore_fresh else None
        backup_host = self.backupset.backup_host
        extra_nodes = [] if backup_host.ip in [n.ip for n in cluster_nodes] \
            else [backup_host]

        self._install(self.servers + extra_nodes)
        for server in cluster_nodes:
            if not RestHelper(RestConnection(server)).is_ns_server_running(60):
                self.fail("ns_server is not running on {0}".format(server.ip))
        self._ensure_provisioned(cluster_nodes)
        rebalance = self.cluster.async_rebalance(cluster_nodes, [cluster_nodes[1]], [])
        rebalance.result()
        self.sleep(15)
        self.add_built_in_server_user()

        self._create_bucket(self.master)
        self._load_all_buckets(self.master,
                               BlobGenerator("ent-backup", "ent-backup-",
                                             self.value_size, end=self.num_items),
                               "create", 0)
        layout = [("_default", "_default")]
        coll_rest = CollectionsRest(self.master)
        for s in range(num_scopes):
            for c in range(num_collections):
                scope, collection = "scope{0}".format(s), "coll{0}_{1}".format(s, c)
                if c == 0:
                    coll_rest.create_scope_collection("default", scope, collection)
                else:
                    coll_rest.create_collection("default", scope, collection)
                self._load_collection(scope, collection, "{0}-".format(collection))
                layout.append((scope, collection))

        # a backup only holds persisted mutations
        if not RebalanceHelper.wait_for_stats_on_all(self.master, "default",
                                                     "ep_queue_size", 0,
                                                     timeout_in_seconds=300):
            self.fail("Bucket did not persist all items")
        expected = self._collection_counts(cluster_nodes, layout)
        # cbc-pillowfight right after a collection is created can lose a doc
        # while the manifest is still reaching KV; the load is an idempotent upsert
        for _ in range(3):
            short = [key for key in expected
                     if key != ("_default", "_default") and expected[key] != self.num_items]
            if not short:
                break
            self.log.info("Reloading short collections: {0}".format(short))
            for scope, collection in short:
                self._load_collection(scope, collection, "{0}-".format(collection))
            RebalanceHelper.wait_for_stats_on_all(self.master, "default",
                                                  "ep_queue_size", 0,
                                                  timeout_in_seconds=300)
            expected = self._collection_counts(cluster_nodes, layout)
        self.log.info("Per-collection counts at backup time: {0}".format(expected))
        if any(expected[key] != self.num_items for key in expected):
            self.fail("Load did not produce {0} docs in every collection: {1}"
                      .format(self.num_items, expected))

        initial_release = self.initial_version[:3]
        if self._cbbackupmgr_release() != initial_release:
            self.fail("cbbackupmgr on the backup host is not {0}".format(initial_release))
        self.backup_create()
        output, error = self.backup_cluster()
        if not self._check_output(self.bk_printout, output):
            self.fail("Backup failed.\nOutput: {0}\nError: {1}".format(output, error))

        self.log.info("Upgrading to {0} using {1}".format(upgrade_version,
                                                           self.upgrade_type))
        self._upgrade_nodes(upgrade_version, cluster_nodes, extra_nodes)
        if self._cbbackupmgr_release() != upgrade_version[:3]:
            self.fail("cbbackupmgr on the backup host is not {0} after upgrade"
                      .format(upgrade_version[:3]))

        BucketOperationHelper.delete_bucket_or_assert(self.master, "default", self)
        self.sleep(10)
        self._create_bucket(self.master)
        self._restore_with_current_cbbackupmgr(self.master)
        self._wait_for_counts(cluster_nodes, layout, expected,
                              "Restore into upgraded cluster")

        if backup_after_upgrade:
            # new data in a new collection, so the new backup is distinguishable
            coll_rest = CollectionsRest(self.master)
            coll_rest.create_collection("default", "scope0", "post_upgrade")
            self._load_collection("scope0", "post_upgrade", "post_upgrade-")
            if not RebalanceHelper.wait_for_stats_on_all(self.master, "default",
                                                         "ep_queue_size", 0,
                                                         timeout_in_seconds=300):
                self.fail("Bucket did not persist all items after upgrade")
            layout.append(("scope0", "post_upgrade"))
            expected = self._collection_counts(cluster_nodes, layout)
            if expected[("scope0", "post_upgrade")] != self.num_items:
                self.fail("Post-upgrade load produced {0}".format(expected))
            output, error = self.backup_cluster()
            if not self._check_output(self.bk_printout, output):
                self.fail("Backup after upgrade failed.\nOutput: {0}\nError: {1}"
                          .format(output, error))
            BucketOperationHelper.delete_bucket_or_assert(self.master, "default", self)
            self.sleep(10)
            self._create_bucket(self.master)
            self._restore_with_current_cbbackupmgr(self.master)
            self._wait_for_counts(cluster_nodes, layout, expected,
                                  "Restore of post-upgrade backup")

        if restore_fresh:
            self.initial_version = upgrade_version
            self._install([fresh_node])
            self._initialize_nodes(Cluster(), [fresh_node])
            self.add_built_in_server_user(node=fresh_node)
            self._restore_with_current_cbbackupmgr(fresh_node, auto_create_buckets=True)
            self._wait_for_counts([fresh_node], layout, expected,
                                  "Restore into fresh cluster")

    # ------------------------------------------------------------------
    # cbexport / cbimport across an upgrade
    # ------------------------------------------------------------------
    def _run_tool(self, command, what):
        """ Runs a command on the backup host, returns (seconds, stdout) """
        shell = RemoteMachineShellConnection(self.backupset.backup_host)
        start = time.time()
        output, error, exit_code = shell.execute_command(command, timeout=3600,
                                                         get_exit_code=True)
        elapsed = time.time() - start
        shell.disconnect()
        if exit_code != 0:
            self.fail("{0} failed (exit {1}).\nOutput: {2}\nError: {3}"
                      .format(what, exit_code, output, error))
        return elapsed, output

    def _cbexport(self, path, what):
        base = ("{0}/cbexport json -c couchbase://{1} -u {2} -p {3} -b default "
                "-o {4} -f lines".format(self.cli_command_location, self.master.ip,
                                         self.master.rest_username,
                                         self.master.rest_password, path))
        flags = ""
        for attempt in range(2):
            shell = RemoteMachineShellConnection(self.backupset.backup_host)
            start = time.time()
            output, error, exit_code = shell.execute_command(
                "rm -f {0}; {1}{2}".format(path, base, flags), timeout=3600,
                get_exit_code=True)
            elapsed = time.time() - start
            shell.disconnect()
            if exit_code == 0:
                break
            # cbexport of some versions refuses a collection aware cluster without these
            if attempt == 0 and "--scope-field" in " ".join(output + error):
                flags = " --scope-field scope --collection-field collection"
                continue
            self.fail("{0} failed (exit {1}).\nOutput: {2}\nError: {3}"
                      .format(what, exit_code, output, error))
        shell = RemoteMachineShellConnection(self.backupset.backup_host)
        output, _ = shell.execute_command("cat {0}".format(path), timeout=600)
        shell.disconnect()
        docs = {}
        for line in output:
            if line.strip():
                doc = json.loads(line)
                if flags:
                    doc.pop("scope", None)
                    doc.pop("collection", None)
                docs[doc["id"]] = doc
        self.log.info("{0}: exported {1} docs in {2:.1f}s{3}"
                      .format(what, len(docs), elapsed,
                              " (with scope/collection fields)" if flags else ""))
        return elapsed, docs

    def _cbimport(self, path, what):
        cmd = ("{0}/cbimport json -c couchbase://{1} -u {2} -p {3} -b default "
               "-d file://{4} -f lines -g key::%id% -t 4"
               .format(self.cli_command_location, self.master.ip,
                       self.master.rest_username, self.master.rest_password, path))
        elapsed, output = self._run_tool(cmd, what)
        self.log.info("{0}: imported in {1:.1f}s. Output: {2}".format(what, elapsed, output))

    def test_export_import_after_upgrade(self):
        """
        1. Install initial_version, import known JSON docs with the old cbimport
        2. Export with the old cbexport; check the content equals what was imported
        3. Upgrade the cluster and the backup host
        4. Export with the new cbexport; content must equal the pre-upgrade export
           and the export time must stay below a gross ceiling
           (export_slowdown_factor x old time, at least export_floor_secs)
        5. Round trip: recreate the bucket, import the new export with the new
           cbimport, export again and compare
        """
        upgrade_version = self.input.param("upgrade_version", None)
        if not upgrade_version:
            self.fail("Need param 'upgrade_version'")
        self.upgrade_type = self.input.param("upgrade_type", "offline")
        if self.upgrade_type not in UPGRADE_TYPES:
            self.fail("upgrade_type must be one of {0}".format(UPGRADE_TYPES))
        export_items = int(self.input.param("export_items", 20000))
        slowdown = float(self.input.param("export_slowdown_factor", 3))
        floor_secs = float(self.input.param("export_floor_secs", 30))

        cluster_nodes = self.servers[:2]
        backup_host = self.backupset.backup_host
        extra_nodes = [] if backup_host.ip in [n.ip for n in cluster_nodes] \
            else [backup_host]

        self._install(self.servers[:2] + extra_nodes)
        for server in cluster_nodes:
            if not RestHelper(RestConnection(server)).is_ns_server_running(60):
                self.fail("ns_server is not running on {0}".format(server.ip))
        self._ensure_provisioned(cluster_nodes)
        rebalance = self.cluster.async_rebalance(cluster_nodes, [cluster_nodes[1]], [])
        rebalance.result()
        self.sleep(15)
        self.add_built_in_server_user()
        self._create_bucket(self.master)
        self._wait_for_bucket_ready(self.master)

        source = "/tmp/export_source.json"
        expected = {i: {"id": i, "name": "doc-{0}".format(i), "val": i * 7}
                    for i in range(export_items)}
        local_source = os.path.join(tempfile.mkdtemp(), "export_source.json")
        with open(local_source, "w") as f:
            for doc in expected.values():
                f.write(json.dumps(doc) + "\n")
        shell = RemoteMachineShellConnection(self.backupset.backup_host)
        shell.copy_file_local_to_remote(local_source, source)
        shell.disconnect()

        if self._cbbackupmgr_release() != self.initial_version[:3]:
            self.fail("Tools on the backup host are not {0}".format(self.initial_version[:3]))
        self._cbimport(source, "cbimport before upgrade")
        self._wait_for_persisted_count(self.master, export_items, "After cbimport before upgrade")
        old_secs, old_docs = self._cbexport("/tmp/export_old.json", "cbexport before upgrade")
        if old_docs != expected:
            self.fail("Pre-upgrade export differs from imported data: {0} vs {1} docs"
                      .format(len(old_docs), len(expected)))

        self.log.info("Upgrading to {0} using {1}".format(upgrade_version, self.upgrade_type))
        self._upgrade_nodes(upgrade_version, cluster_nodes, extra_nodes)
        if self._cbbackupmgr_release() != upgrade_version[:3]:
            self.fail("Tools on the backup host are not {0} after upgrade"
                      .format(upgrade_version[:3]))

        new_secs, new_docs = self._cbexport("/tmp/export_new.json", "cbexport after upgrade")
        if new_docs != old_docs:
            self.fail("Post-upgrade export differs from pre-upgrade export: {0} vs {1} docs"
                      .format(len(new_docs), len(old_docs)))
        ceiling = max(old_secs * slowdown, floor_secs)
        self.log.info("cbexport time old {0:.1f}s new {1:.1f}s ceiling {2:.1f}s"
                      .format(old_secs, new_secs, ceiling))
        if new_secs > ceiling:
            self.fail("cbexport after upgrade took {0:.1f}s, over the {1:.1f}s ceiling "
                      "({2}x the {3:.1f}s before upgrade)"
                      .format(new_secs, ceiling, slowdown, old_secs))

        BucketOperationHelper.delete_bucket_or_assert(self.master, "default", self)
        self.sleep(10)
        self._create_bucket(self.master)
        self._wait_for_bucket_ready(self.master)
        self._cbimport("/tmp/export_new.json", "cbimport after upgrade")
        self._wait_for_persisted_count(self.master, len(new_docs), "After cbimport after upgrade")
        _, round_trip = self._cbexport("/tmp/export_round_trip.json", "cbexport round trip")
        if round_trip != new_docs:
            self.fail("Round trip differs: {0} vs {1} docs".format(len(round_trip), len(new_docs)))

    # ------------------------------------------------------------------
    # backup during a swap rebalance upgrade
    # ------------------------------------------------------------------
    def _wait_for_item_count(self, node, expected, stage, timeout=900):
        rest = RestConnection(node)
        elapsed = 0
        actual = None
        while elapsed <= timeout:
            actual = rest.get_active_key_count("default")
            if actual == expected:
                self.log.info("{0}: item count {1} matches".format(stage, actual))
                return
            self.sleep(15, "{0}: {1} items, expected {2}".format(stage, actual, expected))
            elapsed += 15
        self.fail("{0}: {1} items after {2}s, expected {3}".format(stage, actual, timeout, expected))

    def _wait_for_persisted_count(self, node, expected, stage):
        """ All docs are in the bucket and persisted; tells a lost import from a short export """
        self._wait_for_item_count(node, expected, stage, timeout=300)
        if not RebalanceHelper.wait_for_stats_on_all(node, "default", "ep_queue_size", 0,
                                                     timeout_in_seconds=300):
            self.fail("{0}: bucket did not persist all items".format(stage))

    def test_backup_during_swap_rebalance_upgrade(self):
        """
        Online swap-rebalance upgrade of a one node cluster (servers[0] old,
        servers[2] new) with the backup host (servers[1]) outside the cluster.

        1. Install initial_version on servers[0] and the backup host and
           upgrade_version on servers[2]; load data and take a baseline backup
           with the old cbbackupmgr
        2. Upgrade the backup host, start an incremental backup with the new
           cbbackupmgr and, while it runs, swap-rebalance servers[0] out and
           servers[2] in
        3. The backup must succeed and overlap the rebalance
        4. Restore the whole repo with the new cbbackupmgr into the recreated
           bucket on the upgraded node and compare the item count
        """
        upgrade_version = self.input.param("upgrade_version", None)
        if not upgrade_version:
            self.fail("Need param 'upgrade_version'")
        if len(self.servers) < 3:
            self.fail("Needs three servers: old node, backup host, new node")
        old_node, backup_host, new_node = self.servers[0], self.servers[1], self.servers[2]
        if self.backupset.backup_host.ip != backup_host.ip:
            self.fail("Backup host must be servers[1] (cluster2 in the ini)")
        initial_version = self.initial_version

        self._install([old_node, backup_host])
        self.initial_version = upgrade_version
        self._install([new_node])
        self.initial_version = initial_version
        for server in [old_node, new_node]:
            if not RestHelper(RestConnection(server)).is_ns_server_running(60):
                self.fail("ns_server is not running on {0}".format(server.ip))
        self._ensure_provisioned([old_node, new_node])
        self.add_built_in_server_user(node=old_node)
        self.add_built_in_server_user(node=new_node)

        self._create_bucket(old_node)
        self._wait_for_bucket_ready(old_node)
        self._load_all_buckets(old_node,
                               BlobGenerator("ent-backup", "ent-backup-",
                                             self.value_size, end=self.num_items),
                               "create", 0)
        if not RebalanceHelper.wait_for_stats_on_all(old_node, "default", "ep_queue_size", 0,
                                                     timeout_in_seconds=300):
            self.fail("Bucket did not persist all items")
        self._wait_for_item_count(old_node, self.num_items, "Source bucket before baseline backup",
                                  timeout=300)
        if self._cbbackupmgr_release() != initial_version[:3]:
            self.fail("cbbackupmgr on the backup host is not {0}".format(initial_version[:3]))
        self.backup_create()
        output, error = self.backup_cluster()
        if not self._check_output(self.bk_printout, output):
            self.fail("Baseline backup failed.\nOutput: {0}\nError: {1}".format(output, error))

        self._run_upgrade(upgrade_version, [backup_host])
        if self._cbbackupmgr_release() != upgrade_version[:3]:
            self.fail("cbbackupmgr on the backup host is not {0} after upgrade"
                      .format(upgrade_version[:3]))

        # more data so the incremental backup has something to do
        self._load_all_buckets(old_node,
                               BlobGenerator("post-upgrade", "post-upgrade-",
                                             self.value_size, end=self.num_items),
                               "create", 0)
        expected_items = 2 * self.num_items
        if not RebalanceHelper.wait_for_stats_on_all(old_node, "default", "ep_queue_size", 0,
                                                     timeout_in_seconds=300):
            self.fail("Bucket did not persist all items")
        # tells a short load from a short backup/restore
        self._wait_for_item_count(old_node, expected_items,
                                  "Source bucket before the backup during the swap", timeout=300)

        window = {}

        def run_backup():
            cmd = ("{0}/cbbackupmgr backup --archive {1} --repo {2} --cluster http://{3}:{4} "
                   "--username {5} --password {6} --no-progress-bar"
                   .format(self.cli_command_location, self.backupset.directory,
                           self.backupset.name, old_node.cluster_ip, old_node.port,
                           old_node.rest_username, old_node.rest_password))
            window["backup_start"] = time.time()
            shell = RemoteMachineShellConnection(backup_host)
            window["output"], window["error"] = shell.execute_command(cmd, timeout=3600)
            window["backup_end"] = time.time()
            shell.disconnect()

        backup_thread = threading.Thread(target=run_backup)
        backup_thread.start()
        self.sleep(3, "Let the backup get going before the swap rebalance")
        window["rebalance_start"] = time.time()
        rebalance = self.cluster.async_rebalance([old_node, new_node], [new_node], [old_node])
        rebalance.result()
        window["rebalance_end"] = time.time()
        backup_thread.join()

        self.log.info("Backup {0:.0f}-{1:.0f}s, rebalance {2:.0f}-{3:.0f}s (relative)".format(
            0, window["backup_end"] - window["backup_start"],
            window["rebalance_start"] - window["backup_start"],
            window["rebalance_end"] - window["backup_start"]))
        if not (window["backup_start"] < window["rebalance_end"]
                and window["backup_end"] > window["rebalance_start"]):
            self.fail("The backup did not overlap the rebalance; use more 'items'")
        if not self._check_output("Backup completed successfully", window["output"]):
            self.fail("Backup during swap rebalance failed.\nOutput: {0}\nError: {1}"
                      .format(window["output"], window["error"]))

        self._log_backup_info()
        BucketOperationHelper.delete_bucket_or_assert(new_node, "default", self)
        self.sleep(10)
        self._create_bucket(new_node)
        self._wait_for_bucket_ready(new_node)
        self._restore_with_current_cbbackupmgr(new_node)
        self._wait_for_item_count(new_node, expected_items,
                                  "Restore after backup during swap rebalance")
