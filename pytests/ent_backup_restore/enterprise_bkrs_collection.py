import re, copy, json, subprocess
from random import randrange, randint
from threading import Thread

from collection.collections_cli_client import Collections_CLI
from collection.collections_rest_client import Collections_Rest
from collection.collections_n1ql_client import CollectionsN1QL
from lib.couchbase_helper.documentgenerator import SDKDataLoader

from couchbase_helper.cluster import Cluster
from membase.helper.rebalance_helper import RebalanceHelper
from couchbase_helper.documentgenerator import BlobGenerator, DocumentGenerator
from ent_backup_restore.enterprise_bkrs_collection_base import EnterpriseBackupRestoreCollectionBase
from membase.api.rest_client import RestConnection, RestHelper, Bucket
from membase.helper.bucket_helper import BucketOperationHelper
from pytests.query_tests_helper import QueryHelperTests
from remote.remote_util import RemoteUtilHelper, RemoteMachineShellConnection
from security.auditmain import audit
from security.rbac_base import RbacBase
from upgrade.newupgradebasetest import NewUpgradeBaseTest
from couchbase.bucket import Bucket
from couchbase_helper.document import View
from eventing.eventing_base import EventingBaseTest
from tasks.future import Future, TimeoutError
from xdcr.xdcrnewbasetests import NodeHelper
from couchbase_helper.stats_tools import StatsCommon
from testconstants import COUCHBASE_DATA_PATH, WIN_COUCHBASE_DATA_PATH, \
                          COUCHBASE_FROM_4DOT6, ENT_BKRS, ENT_BKRS_FTS

AUDITBACKUPID = 20480
AUDITRESTOREID = 20485
SOURCE_CB_PARAMS = {
    "authUser": "default",
    "authPassword": "",
    "authSaslUser": "",
    "authSaslPassword": "",
    "clusterManagerBackoffFactor": 0,
    "clusterManagerSleepInitMS": 0,
    "clusterManagerSleepMaxMS": 20000,
    "dataManagerBackoffFactor": 0,
    "dataManagerSleepInitMS": 0,
    "dataManagerSleepMaxMS": 20000,
    "feedBufferSizeBytes": 0,
    "feedBufferAckThreshold": 0
}
INDEX_DEFINITION = {
    "type": "fulltext-index",
    "name": "",
    "uuid": "",
    "params": {},
    "sourceType": "couchbase",
    "sourceName": "default",
    "sourceUUID": "",
    "sourceParams": SOURCE_CB_PARAMS,
    "planParams": {}
}


class EnterpriseBackupRestoreCollectionTest(EnterpriseBackupRestoreCollectionBase, NewUpgradeBaseTest):
    def setUp(self):
        super().setUp()
        self.users_check_restore = \
              self.input.param("users-check-restore", '').replace("ALL", "*").split(";")
        if '' in self.users_check_restore:
            self.users_check_restore.remove('')
        for server in [self.backupset.backup_host, self.backupset.restore_cluster_host]:
            conn = RemoteMachineShellConnection(server)
            conn.extract_remote_info()
            conn.terminate_processes(conn.info, ["cbbackupmgr"])
            conn.disconnect()
        self.bucket_helper = BucketOperationHelper()

    def tearDown(self):
        super(EnterpriseBackupRestoreCollectionTest, self).tearDown()

    def test_backup_create(self):
        self.backup_create_validate()

    def test_backup_restore_collection_sanity(self):
        """
        1. Create default bucket on the cluster and loads it with given number of items
        2. Perform updates and create backups for specified number of times (test param number_of_backups)
        3. Perform restores for the same number of times with random start and end values
        """
        self.log.info("*** create collection in all buckets")
        self.log.info("*** start to load items to all buckets")
        self.active_resident_threshold = 100
        self.__load_all_buckets(self.backupset.cluster_host)
        self.log.info("*** done to load items to all buckets")
        self.ops_type = self.input.param("ops-type", "update")
        self.expected_error = self.input.param("expected_error", None)
        self.create_scope_cluster_host()
        self.create_collection_cluster_host()
        if self.auto_failover:
            self.log.info("Enabling auto failover on " + str(self.backupset.cluster_host))
            rest_conn = RestConnection(self.backupset.cluster_host)
            rest_conn.update_autofailover_settings(self.auto_failover, self.auto_failover_timeout)
        self.backup_create_validate()
        for i in range(1, self.backupset.number_of_backups + 1):
            if self.ops_type == "update":
                self.log.info("*** start to update items in all buckets")
                self.__load_all_buckets(self.backupset.cluster_host, ratio=0.1)
                self.log.info("*** done update items in all buckets")
            self.sleep(10)
            self.log.info("*** start to validate backup cluster")
            self.backup_cluster_validate()
        self.targetMaster = True
        start = randrange(1, self.backupset.number_of_backups + 1)
        if start == self.backupset.number_of_backups:
            end = start
        else:
            end = randrange(start, self.backupset.number_of_backups + 1)
        self.log.info("*** start to restore cluster")
        restored = {"{0}/{1}".format(start, end): ""}
        for i in range(1, self.backupset.number_of_backups + 1):
            if self.reset_restore_cluster:
                self.log.info("*** start to reset cluster")
                self.backup_reset_clusters(self.cluster_to_restore)
                if self.same_cluster:
                    self._initialize_nodes(Cluster(), self.servers[:self.nodes_init])
                else:
                    shell = RemoteMachineShellConnection(self.backupset.restore_cluster_host)
                    shell.enable_diag_eval_on_non_local_hosts()
                    shell.disconnect()
                    rest = RestConnection(self.backupset.restore_cluster_host)
                    rest.force_eject_node()
                    rest.init_node()
                self.log.info("Done reset cluster")
            self.sleep(10)

            """ Add built-in user cbadminbucket to second cluster """
            self.add_built_in_server_user(node=self.input.clusters[0][:self.nodes_init][0])

            self.backupset.start = start
            self.backupset.end = end
            self.log.info("*** start restore validation")
            self.backup_restore_validate(compare_uuid=False,
                                         seqno_compare_function=">=",
                                         expected_error=self.expected_error)
            if self.backupset.number_of_backups == 1:
                continue
            while "{0}/{1}".format(start, end) in restored:
                start = randrange(1, self.backupset.number_of_backups + 1)
                if start == self.backupset.number_of_backups:
                    end = start
                else:
                    end = randrange(start, self.backupset.number_of_backups + 1)
            restored["{0}/{1}".format(start, end)] = ""

    def _kill_cbbackupmgr(self):
        """
            kill all cbbackupmgr processes
        """
        self.sleep(1, "times need for cbbackupmgr process run")
        shell = RemoteMachineShellConnection(self.backupset.backup_host)
        if self.os_name != "windows":
            cmd = "ps aux | grep cbbackupmgr | gawk '{print $2}' | xargs kill -9"
            output, _ = shell.execute_command(cmd)
        else:
            cmd = "tasklist | grep cbbackupmgr | gawk '{printf$2}'"
            output, _ = shell.execute_command(cmd)
            if output:
                kill_cmd = "taskkill /F /T /pid %d " % int(output[0])
                output, _ = shell.execute_command(kill_cmd)
                if output and "SUCCESS" not in output[0]:
                    self.fail("Failed to kill cbbackupmgr on windows")
        shell.disconnect()