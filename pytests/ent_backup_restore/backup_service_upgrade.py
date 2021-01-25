import logger
from basetestcase import BaseTestCase
from couchbase_helper.cluster import Cluster
from couchbase_helper.documentgenerator import BlobGenerator
from ent_backup_restore.backup_service_test import BackupServiceTest
from ent_backup_restore.enterprise_backup_restore_base import Backupset
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

        # Share a folder between the servers
        self.backup_service.create_shared_folder(servers[0], servers)

        # Load APIs
        self.backup_service.preamble(master)

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
        rest_conn.create_bucket(bucket='default', ramQuotaMB=512, compressionMode=self.compression_mode)
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

    def tearDown(self):
        super().tearDown()
