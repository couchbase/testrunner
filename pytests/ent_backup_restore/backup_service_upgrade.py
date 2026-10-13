import threading
import time

import logger
from basetestcase import BaseTestCase
from couchbase_helper.cluster import Cluster
from couchbase_helper.documentgenerator import BlobGenerator
from ent_backup_restore.backup_service_test import BackupServiceTest
from ent_backup_restore.enterprise_backup_restore_base import Backupset
from lib.backup_service_client.models.body2 import Body2
from lib.backup_service_client.models.body4 import Body4
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.rebalance_helper import RebalanceHelper
from remote.remote_util import RemoteMachineShellConnection
from upgrade.newupgradebasetest import NewUpgradeBaseTest

from TestInput import TestInputSingleton


class BackupServiceHook():
    """ A backup service testing class that can be independently inserted into another class.
    """

    def __init__(self, master, servers, backupset, objstore_provider):
        # Use testrunner's default setup and cleanup
        for param in ["default_setup", "default_cleanup"]:
            TestInputSingleton.input.test_params[param] = True

        # Create a backup service test and bootstrap it
        self.backup_service = BackupServiceTest()
        self.backup_service.bootstrap(servers, backupset, objstore_provider)

        # Load APIs
        self.backup_service.preamble(master)

        # Share a folder between the servers
        self.backup_service.create_shared_folder(servers[0], servers)

    def run_test(self):
        """ A one off backup test to check the backup service is working """
        self.backup_service.create_repository_with_default_plan("my_repo")

        self.backup_service.take_one_off_backup("active", "my_repo", True, 20, 20)

    def cleanup(self):
        """ Cleanup

        Delete all plan/repos and remove the shared folder
        """
        self.backup_service.backup_service_cleanup()
        self.backup_service.delete_shared_folder()

class BackupServiceUpgradeTest(NewUpgradeBaseTest):

    def setUp(self):
        self.input = TestInputSingleton.input
        self.log = logger.Logger.get_logger()
        self.make_backupset()
        super().setUp()

    def make_backupset(self):
        self.backupset = Backupset()
        self.backupset.directory = "/tmp/backup_service"

    def reset_servers(self, servers):
        for server in servers:
            remote = RemoteMachineShellConnection(server)
            remote.diag_eval("gen_server:cast(ns_cluster, leave).")
            remote.disconnect()

    def replace_services(self, servers, server, services):
        """ Changes the services of a server
        """
        # Remove server and rebalance
        Cluster().rebalance(servers, [], [server])
        # Add back with new services and rebalance
        Cluster().rebalance(servers, [server], [], services=services)
        # A little sleep for services to warmup
        self.sleep(15)

    def test_online_swap_rebalance_upgrade(self):
        """ Online swap rebalance upgrade test

        The old nodes are removed and the new nodes are added followed by a rebalance.
        """
        # Installs the `self.initial_version` of Couchbase on the first two servers
        self.product = 'couchbase-server'
        self._install(self.input.servers[:2])

        # Check Couchbase is running post installation
        for server in self.input.servers:
            self.assertTrue(RestHelper(RestConnection(server)).is_ns_server_running(60), f"ns_server is not running on {server}")

        # Install the `self.upgrade_versions` on the last 2 nodes
        self.initial_version = self.upgrade_versions[0]
        self._install(self.input.servers[2:])

        # Remove the first two nodes and perform a rebalance
        self.cluster.rebalance(self.servers, self.servers[2:], self.servers[:2], services=["kv", "kv"])

        # Replace the services of the last node with kv and backup
        self.replace_services(self.servers[2:], self.servers[-1], ["kv,backup"])

        # Add the built in user for memcached authentication
        self.add_built_in_server_user(node=self.servers[2])

        # Create the default bucket and update the list of buckets
        rest_conn = RestConnection(self.servers[2])
        rest_conn.create_bucket(bucket='default',
                                ramQuotaMB=self.input.param('default-bucket-ram-quota', 512),
                                compressionMode=self.compression_mode)
        self.buckets = rest_conn.get_buckets()

        # Populate the buckets with data
        self._load_all_buckets(self.servers[2], BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items), "create", 0)

        try:
            backup_service_hook = BackupServiceHook(self.servers[-1], self.servers, self.backupset, None)

            # Wait for the data to be persisted to disk
            for bucket in self.buckets:
                if not RebalanceHelper.wait_for_stats_on_all(backup_service_hook.backup_service.master, bucket.name, 'ep_queue_size', 0, timeout_in_seconds=200):
                    self.fail("Timeout reached while waiting for 'eq_queue_size' to reach 0")

            backup_service_hook.run_test()
        finally:
            backup_service_hook.cleanup()

    def test_backup_during_swap_rebalance_upgrade(self):
        """ Backup service backup running while a swap rebalance upgrades a node

        servers[0] (old, kv) and servers[1] (old, kv + backup service) form the
        cluster; servers[2] runs upgrade_version.  A one off backup is started
        and, while it runs, servers[0] is swapped out for servers[2] (kv).

        The backup service node (servers[1]) stays in the cluster: a backup task
        running on a node that is ejected is killed when the node leaves (its
        bucket is deleted and the DCP streams close), so a backup on the node
        being swapped out cannot be expected to complete.

        The backup must overlap the rebalance and complete, and the repository
        must restore (the cluster is mixed old/new at that point).
        """
        if len(self.servers) < 3:
            self.fail("Needs three servers: old node, old backup node, new node")
        swapped_out_node, backup_node, new_node = self.servers[0], self.servers[1], self.servers[2]
        master, old_backup_node = swapped_out_node, backup_node
        initial_version = self.initial_version

        self.product = 'couchbase-server'
        self._install([master, old_backup_node])
        self.initial_version = self.upgrade_versions[0]
        self._install([new_node])
        self.initial_version = initial_version
        for server in [master, old_backup_node, new_node]:
            self.assertTrue(RestHelper(RestConnection(server)).is_ns_server_running(60),
                            f"ns_server is not running on {server}")

        # Old cluster: master plus a node with the backup service
        self.cluster.rebalance([master, old_backup_node], [old_backup_node], [], services=["kv,backup"])
        self.sleep(15)
        for server in [master, old_backup_node, new_node]:
            self.add_built_in_server_user(node=server)

        rest_conn = RestConnection(master)
        rest_conn.create_bucket(bucket='default', ramQuotaMB=1024, replicaNumber=1)
        self.buckets = rest_conn.get_buckets()
        self._load_all_buckets(master, BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items), "create", 0)
        for bucket in self.buckets:
            if not RebalanceHelper.wait_for_stats_on_all(master, bucket.name, 'ep_queue_size', 0, timeout_in_seconds=300):
                self.fail("Timeout reached while waiting for 'ep_queue_size' to reach 0")

        backup_service_hook = None
        try:
            # The backup service node (and the shared folder host) is the first server
            backup_service_hook = BackupServiceHook(
                backup_node, [backup_node, swapped_out_node, new_node], self.backupset, None)
            service = backup_service_hook.backup_service
            # The random default plan of the helper may not exist on the old version
            service.active_repository_api.cluster_self_repository_active_id_post(
                "my_repo", body=Body2(plan="_daily_backups", archive=self.backupset.directory))

            retries = max(20, self.num_items // 500)
            task_name = service.active_repository_api.cluster_self_repository_active_id_backup_post(
                "my_repo", body=Body4(full_backup=True)).task_name
            window = {"backup_start": time.time()}
            result = {}

            def wait_for_backup():
                # the rebalance can make the REST calls fail for a while; the wait is
                # a poll, retrying is safe
                result["ok"] = False
                try:
                    for attempt in range(10):
                        try:
                            result["ok"] = service.wait_for_backup_task(
                                "active", "my_repo", retries, 20, task_name=task_name)
                            break
                        except Exception as e:
                            self.log.info("Polling the backup task failed ({0}), retry {1}".format(e, attempt + 1))
                            time.sleep(20)
                finally:
                    window["backup_end"] = time.time()

            waiter = threading.Thread(target=wait_for_backup)
            waiter.start()
            self.sleep(3, "Let the backup get going before the swap rebalance")
            window["rebalance_start"] = time.time()
            self.cluster.rebalance([backup_node, swapped_out_node, new_node], [new_node], [swapped_out_node], services=["kv"])
            window["rebalance_end"] = time.time()
            # the first server left the cluster; the base class cleanup works from self.master
            self.master = backup_node
            waiter.join()

            self.log.info("Backup {0:.0f}-{1:.0f}s, rebalance {2:.0f}-{3:.0f}s (relative)".format(
                0, window["backup_end"] - window["backup_start"],
                window["rebalance_start"] - window["backup_start"],
                window["rebalance_end"] - window["backup_start"]))
            self.assertTrue(window["backup_start"] < window["rebalance_end"] and window["backup_end"] > window["rebalance_start"],
                            "The backup did not overlap the rebalance; use more 'items'")
            self.assertTrue(result.get("ok"), f"The backup '{task_name}' running during the swap rebalance did not complete successfully")
            self.sleep(15, "Let the cluster settle")

            # Restore the repository into the recreated bucket
            master = backup_node
            rest_conn = RestConnection(master)
            for bucket in self.buckets:
                rest_conn.delete_bucket(bucket.name)
            self.sleep(15)
            service.take_one_off_restore("active", "my_repo", retries, 20, master)

            elapsed, actual = 0, None
            while elapsed <= 900:
                actual = rest_conn.get_active_key_count("default")
                if actual == self.num_items:
                    break
                self.sleep(15, f"Restore: {actual} items, expected {self.num_items}")
                elapsed += 15
            self.assertEqual(actual, self.num_items, "Item count after restoring the backup taken during the swap rebalance")
        finally:
            if backup_service_hook:
                backup_service_hook.cleanup()

    def tearDown(self):
        super().tearDown()
