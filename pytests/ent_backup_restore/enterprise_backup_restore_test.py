import re, copy, json, subprocess
from random import randrange, randint
from threading import Thread

from couchbase_helper.cluster import Cluster
from membase.helper.rebalance_helper import RebalanceHelper
from couchbase_helper.documentgenerator import BlobGenerator, DocumentGenerator
from ent_backup_restore.enterprise_backup_restore_base import EnterpriseBackupRestoreBase
from membase.api.rest_client import RestConnection, RestHelper, Bucket
from membase.helper.bucket_helper import BucketOperationHelper
from pytests.query_tests_helper import QueryHelperTests
#from lib.membase.helper.cluster_helper import ClusterOperationHelper
from remote.remote_util import RemoteUtilHelper, RemoteMachineShellConnection
from security.auditmain import audit
from security.rbac_base import RbacBase
from upgrade.newupgradebasetest import NewUpgradeBaseTest
from couchbase.bucket import Bucket
from couchbase_helper.document import View
from eventing.eventing_base import EventingBaseTest
from tasks.future import TimeoutError
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

# TBD: Check with Tony on the below change. Without this, Py3 is failing due to different super class flow.
#class EnterpriseBackupRestoreTest(EnterpriseBackupRestoreBase, NewUpgradeBaseTest, EventingBaseTest):
class EnterpriseBackupRestoreTest(EnterpriseBackupRestoreBase):
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
        super(EnterpriseBackupRestoreTest, self).tearDown()

    def test_backup_create(self):
        self.backup_create_validate()

    def test_backup_restore_sanity(self):
        """
        1. Create default bucket on the cluster and loads it with given number of items
        2. Perform updates and create backups for specified number of times (test param number_of_backups)
        3. Perform restores for the same number of times with random start and end values
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self.log.info("*** start to load items to all buckets")
        self._load_all_buckets(self.master, gen, "create", self.expires)
        self.log.info("*** done to load items to all buckets")
        self.ops_type = self.input.param("ops-type", "update")
        self.expected_error = self.input.param("expected_error", None)
        if self.auto_failover:
            self.log.info("Enabling auto failover on " + str(self.backupset.cluster_host))
            rest_conn = RestConnection(self.backupset.cluster_host)
            rest_conn.update_autofailover_settings(self.auto_failover, self.auto_failover_timeout)
        self.backup_create_validate()
        for i in range(1, self.backupset.number_of_backups + 1):
            if self.ops_type == "update":
                self.log.info("*** start to update items in all buckets")
                self._load_all_buckets(self.master, gen, "update", self.expires)
                self.log.info("*** done update items in all buckets")
            elif self.ops_type == "delete":
                self.log.info("*** start to delete items in all buckets")
                self._load_all_buckets(self.master, gen, "delete", self.expires)
                self.log.info("*** done to delete items in all buckets")
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

    def test_backup_restore_after_rebalance(self):
        """
        1. Create default bucket on the cluster and loads it with given number of items
        2. Does a rebalance on cluster to be backed up with specified number of servers in (test param nodes_in) and
        servers out (test param nodes_out)
        3. Takes a backup
        4. Does a rebalance on cluster to be restored to with specified number of servers in (test param nodes_in) and
        servers out (test param nodes_out)
        5. Performs a restore on the restore cluster
        """
        serv_in = self.servers[self.nodes_init:self.nodes_init + self.nodes_in]
        serv_out = self.servers[self.nodes_init - self.nodes_out:self.nodes_init]
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create_validate()
        self.backupset.number_of_backups = 1
        rebalance = self.cluster.async_rebalance(self.cluster_to_backup, serv_in, serv_out)
        rebalance.result()
        self.backup_cluster_validate()
        if not self.same_cluster:
            self._initialize_nodes(Cluster(), self.input.clusters[0][:self.nodes_init])
            serv_in = self.input.clusters[0][self.nodes_init: self.nodes_init + self.nodes_in]
            serv_out = self.input.clusters[0][self.nodes_init - self.nodes_out: self.nodes_init]
            rebalance = self.cluster.async_rebalance(self.cluster_to_restore, serv_in, serv_out)
        else:
            rebalance = self.cluster.async_rebalance(self.cluster_to_restore, serv_out, serv_in)
        rebalance.result()
        self.backup_restore_validate(compare_uuid=False, seqno_compare_function="<=")

    def test_backup_restore_with_rebalance(self):
        """
        1. Create default bucket on the cluster and loads it with given number of items
        2. Does a rebalance on cluster to be backed up with specified number of servers in (test param nodes_in) and
        servers out (test param nodes_out)
        3. Takes a backup while rebalance is going on
        4. Does a rebalance on cluster to be restored to with specified number of servers in (test param nodes_in) and
        servers out (test param nodes_out)
        5. Performs a restore on the restore cluster while rebalance is going on
        """
        serv_in = self.servers[self.nodes_init:self.nodes_init + self.nodes_in]
        serv_out = self.servers[self.nodes_init - self.nodes_out:self.nodes_init]
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create_validate()
        self.backupset.number_of_backups = 1
        self.cluster.async_rebalance(self.cluster_to_backup, serv_in, serv_out)
        self.sleep(10)
        self.backup_cluster_validate()
        if not self.same_cluster:
            self._initialize_nodes(Cluster(), self.input.clusters[0][:self.nodes_init])
            serv_in = self.input.clusters[0][self.nodes_init: self.nodes_init + self.nodes_in]
            serv_out = self.input.clusters[0][self.nodes_init - self.nodes_out: self.nodes_init]
            self.cluster.async_rebalance(self.cluster_to_restore, serv_in, serv_out)
        else:
            self.cluster.async_rebalance(self.cluster_to_restore, serv_out, serv_in)
        self.sleep(10)
        self.backup_restore_validate(compare_uuid=False, seqno_compare_function="<=")

    def test_backup_restore_with_ops(self):
        """
        1. Create default bucket on the cluster and loads it with given number of items
        2. Perform the specified ops (test param ops-type) and create backups for specified number of times
        (test param number_of_backups)
        3. Perform restores for the same number of times with random start and end values
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        initial_gen = copy.deepcopy(gen)
        initial_keys = []
        for x in initial_gen:
            initial_keys.append(x[0])
        self.log.info("Start to load items to all buckets")
        self._load_all_buckets(self.master, gen, "create", 0)
        self.ops_type = self.input.param("ops-type", "update")
        self.log.info("Create backup repo ")
        self.backup_create()
        for i in range(1, self.backupset.number_of_backups + 1):
            self._backup_restore_with_ops()
        start = randrange(1, self.backupset.number_of_backups + 1)
        if start == self.backupset.number_of_backups:
            end = start
        else:
            end = randrange(start, self.backupset.number_of_backups + 1)

        if self.compact_backup and self.ops_type == "delete":
            self.log.info("Start to compact backup ")
            self.backup_compact_validate()
            self.log.info("Validate deleted keys")
            self.backup_compact_deleted_keys_validation(initial_keys)

        self.log.info("start restore cluster ")
        restored = {"{0}/{1}".format(start, end): ""}
        for i in range(1, self.backupset.number_of_backups + 1):
            self.backupset.start = start
            self.backupset.end = end
            self._backup_restore_with_ops(backup=False, compare_function=">=")
            if self.backupset.number_of_backups == 1:
                continue
            while "{0}/{1}".format(start, end) in restored:
                start = randrange(1, self.backupset.number_of_backups + 1)
                if start == self.backupset.number_of_backups:
                    end = start
                else:
                    end = randrange(start, self.backupset.number_of_backups + 1)
            restored["{0}/{1}".format(start, end)] = ""

    def _backup_restore_with_ops(self, exp=0, backup=True, compare_uuid=False,
                                 compare_function="==", replicas=False,
                                 mode="memory", node=None, repeats=0,
                                 validate_directory_structure=True):
        self.ops_type = self.input.param("ops-type", "update")
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size,
                            end=self.num_items)
        self.log.info("Start doing ops: %s " % self.ops_type)
        if node is None:
            node = self.master
        self._load_all_buckets(node, gen, self.ops_type, exp)
        if backup:
            self.backup_cluster_validate(repeats=repeats,
                                         validate_directory_structure=validate_directory_structure)
        else:
            self.backup_restore_validate(compare_uuid=compare_uuid,
                                         seqno_compare_function=compare_function,
                                         replicas=replicas, mode=mode)

    def test_backup_list(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a backup and validates it
        3. Executes list command on the backupset and validates the output
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self.backup_cluster_validate()
        self.backup_list_validate()

    def test_backup_list_optional_switches(self):
        """
        1. Creates specified buckets on the cluster and loads it with given number of items
           Note: this test should be run with 2 buckets
        2. Creates two backupsets
        3. Creates two backups on each of the backupset
        4. Executes list command with --name and validates
        5. Executes list command with --name and --incr-backup and validates
        6. Executes list command with --name, --incr-backup and --bucket-backup and validates
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self._take_n_backups(n=2)
        self.backupset.name = "backup2"
        self.backup_create(del_old_backup=False)
        self._take_n_backups(n=2)
        incr_names = 0
        backup_name = False
        warnning_mesg = "is either empty or it got interrupted"
        self.backupset.backup_list_name = "backup"
        status, output, message = self.backup_list()
        if not status:
            self.fail(message)
        for line in output:
            if warnning_mesg in line:
                continue
            if self.backupset.backup_list_name in line:
                backup_name = True
            if self.backups[0] in line:
                incr_names += 1
            if self.backups[1] in line:
                incr_names += 1
        self.assertTrue(backup_name, "Expected backup name not found in output")
        self.log.info("Expected backup name found in output")
        self.assertEqual(incr_names, 2, "Expected backups were not listed for --name option")
        self.log.info("Expected backups listed for --name option")
        incr_names = 0
        backup_name = False
        self.backupset.backup_list_name = "backup2"
        status, output, message = self.backup_list()
        if not status:
            self.fail(message)
        for line in output:
            if warnning_mesg in line:
                continue
            if self.backupset.backup_list_name in line:
                backup_name = True
            if self.backups[2] in line:
                incr_names += 1
            if self.backups[3] in line:
                incr_names += 1
        self.assertTrue(backup_name, "Expected backup name not found in output")
        self.log.info("Expected backup name found in output")
        self.assertEqual(incr_names, 2, "Expected backups were not listed for --name option")
        self.log.info("Expected backups listed for --name option")
        buckets = 0
        name = False
        self.backupset.backup_list_name = "backup"
        self.backupset.backup_incr_backup = self.backups[0]
        status, output, message = self.backup_list()
        if not status:
            self.fail(message)
        for line in output:
            if warnning_mesg in line:
                continue
            if self.backupset.backup_incr_backup in line:
                name = True
            if self.buckets[0].name in line:
                buckets += 1
            if self.buckets[1].name in line:
                buckets += 1
        self.assertTrue(name, "Expected incremental backup name not found in output")
        self.log.info("Expected incrmental backup name found in output")
        self.assertEqual(buckets, 2, "Expected buckets were not listed for --incr-backup option")
        self.log.info("Expected buckets were listed for --incr-backup option")
        name = False
        items = 0
        self.backupset.backup_list_name = "backup2"
        self.backupset.backup_incr_backup = self.backups[2]
        self.backupset.bucket_backup = self.buckets[0].name
        status, output, message = self.backup_list()
        if not status:
            self.fail(message)
        for line in output:
            if warnning_mesg in line:
                continue
            if self.buckets[0].name in line:
                name = True
            if "shard" in line.lower():
                split = line.split(" ")
                split = [s for s in split if s]
                items += int(split[1])
        self.assertTrue(name, "Expected bucket not listed for --bucket-backup option")
        self.log.info("Expected bucket listed for --bucket-backup option")
        self.assertEqual(items, self.num_items, "Mismatch in items for --bucket-backup option")
        self.log.info("Expected number of items for --bucket-backup option")

    def test_list_with_large_number_of_backups(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a large number of backups
        3. Executes list command on the backupset and validates the output
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self._take_n_backups(n=25)
        status, output, message = self.backup_list()
        if not status:
            self.fail(message)
        backup_count = 0
        for line in output:
            if re.search("\d{4}-\d{2}-\d{2}T\d{2}_\d{2}_\d{2}.\d+-\d{2}_\d{2}", line):
                backup_name = re.search("\d{4}-\d{2}-\d{2}T\d{2}_\d{2}_\d{2}.\d+-\d{2}_\d{2}", line).group()
                if backup_name in self.backups:
                    backup_count += 1
                    self.log.info("{0} matched in list command output".format(backup_name))
        self.assertEqual(backup_count, len(self.backups), "Number of backups did not match")
        self.log.info("Number of backups matched")

    def _take_n_backups(self, n=1, validate=False):
        for i in range(1, n + 1):
            if validate:
                self.backup_cluster_validate()
            else:
                self.backup_cluster()

    def test_backup_compact(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a backup and validates it
        3. Executes compact command on the backupset and validates the output
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self.backup_cluster_validate()
        self.backup_compact_validate()

    def test_backup_with_purge_interval_set_to_float(self):
        """
           cbbackupmgr should handle case with purge interval set to float number
           return: None
        """
        purgeInterval = 1.5
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self.log.info("Set purge interval to float value '%s'" % purgeInterval)
        rest = RestConnection(self.backupset.cluster_host)
        status, content = rest.set_purge_interval_and_parallel_compaction(purgeInterval)
        if status:
            self.log.info("Done set purge interval value '%s'" % purgeInterval)
            self._load_all_buckets(self.master, gen, "create", 0)
            self.backup_create()
            self.backup_cluster_validate()
            self.backup_restore_validate()
        else:
            self.fail("Failed to set purgeInterval value")

    def test_restore_from_compacted_backup(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a backup and validates it
        3. Executes compact command on the backupset
        4. Restores from the compacted backup and validates it
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self.backup_cluster_validate()
        self.backup_compact()
        self.backup_restore_validate()

    def test_backup_with_compress_flag(self):
        """
            1. Load docs into bucket
            2. Backup without compress flag
            3. Get backup data size
            4. Delete backup repo
            5. Do backup again with compress flag
            6. Compare those data if it flag works
            :return: None
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size,
                            end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self.backupset.backup_compressed = False
        self.backup_cluster()
        no_compression = self.get_database_file_info()
        self.log.info("\nDelete old backup and do backup again with compress flag")
        self.backup_create()
        self.backupset.backup_compressed = self.input.param("backup-compressed", False)
        self.backup_cluster()
        with_compression = self.get_database_file_info()
        self.validate_backup_compressed_file(no_compression, with_compression)

    def test_backup_restore_with_credentials_env(self):
        """
            password will pass as in env variable
            :return: None
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size,
                            end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        output, error = self.backup_cluster()
        if output and not self._check_output("Backup successfully completed", output):
            self.fail("Failed to run with password env %s " % output)
        self.backup_cluster_validate(skip_backup=True)
        self.backup_list()
        self.backup_restore_validate()

    def test_backup_with_update_on_disk_of_snapshot_markers(self):
        """
            This test is for MB-25727 (using cbbackupwrapper)
            Check when cbwrapper will be dropped to remove this test.
            No default bucket, default_bucket=false
            Create bucket0
            Load 100K items to bucket0
            Stop persistence on server via cbepctl
            Load another 100K items.
            Run full backup with cbbackupwrapper
            Load another 100K items.
            Run diff backup. Backup process will hang with error in memcached as shown above
            :return: None
        """
        gen1 = BlobGenerator("ent-backup1", "ent-backup-", self.value_size, end=100000)
        gen2 = BlobGenerator("ent-backup2", "ent-backup-", self.value_size, end=100000)
        gen3 = BlobGenerator("ent-backup3", "ent-backup-", self.value_size, end=100000)
        rest_conn = RestConnection(self.backupset.cluster_host)
        rest_conn.create_bucket(bucket="bucket0", ramQuotaMB=1024)
        self.buckets = rest_conn.get_buckets()
        authentication = "-u Administrator -p password"

        self._load_all_buckets(self.master, gen1, "create", 0)
        self.log.info("Stop persistent")
        cluster_nodes = rest_conn.get_nodes()
        clusters = copy.deepcopy(cluster_nodes)
        shell = RemoteMachineShellConnection(self.backupset.backup_host)
        for node in clusters:
             shell.execute_command("%scbepctl%s %s:11210 -b %s stop %s" % \
                                  (self.cli_command_location,
                                   self.cmd_ext,
                                   node.ip,
                                   "bucket0",
                                   authentication))
        shell.disconnect()
        self.log.info("Load 2nd batch docs")
        self._load_all_buckets(self.master, gen2, "create", 0)
        self.log.info("Run full backup with cbbackupwrapper")
        shell = RemoteMachineShellConnection(self.backupset.backup_host)
        backup_dir = self.tmp_path + "backup" + self.master.ip
        shell.execute_command("rm -rf %s" % backup_dir)
        shell.execute_command("mkdir %s" % backup_dir)
        shell.execute_command("cd %s;./cbbackupwrapper%s http://%s:8091 %s -m full %s"
                                       % (self.cli_command_location, self.cmd_ext,
                                          self.backupset.cluster_host.ip,
                                          backup_dir,
                                          authentication))
        self.log.info("Load 3rd batch docs")
        self._load_all_buckets(self.master, gen3, "create", 0)
        self.log.info("Run diff backup with cbbackupwrapper")
        output, _ = shell.execute_command("cd %s;./cbbackupwrapper%s http://%s:8091 %s -m diff %s"
                                              % (self.cli_command_location, self.cmd_ext,
                                                 self.backupset.cluster_host.ip,
                                                 backup_dir,
                                                 authentication))

        if output and "SUCCESSFULLY COMPLETED" not in output[1]:
            self.fail("Failed to backup as the fix in MB-25727")
        shell.disconnect()


    def test_cbrestoremgr_should_not_change_replica_count_in_restore_bucket(self):
        """
            This test is for MB-25809
            Set default_bucket=False
            Create bucket with 1 replica
            Load 10K items to bucket
            Backup data from bucket
            Create other bucket with 2 replicas in other cluster
            Restore data to bucket with 2 replicas
            Verify data and bucket setting.  It must retain 2 replicas
            :return: None
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=10000)
        if not self.new_replicas:
            self.fail("This test needs to pass param 'new-replicas' to run")
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self.log.info("Start backup cluster")
        self.backup_cluster_validate()
        self.backup_restore_validate()

        self.log.info("replicas from backup bucket: {0}".format(self.num_replicas))
        self.log.info("replica in restore bucket should be {0} after restore"\
                                                   .format(self.new_replicas))
        rest_r = RestConnection(self.backupset.restore_cluster_host)
        for bucket in self.buckets:
            bucket_stats = rest_r.get_bucket_json(bucket.name)
            if self.new_replicas != bucket_stats["replicaNumber"]:
                self.fail("replia number in bucket {0} did change after restore"\
                                                             .format(bucket.name))
            self.log.info("Verified replica in bucket {0}: {1}"\
                                           .format(bucket.name,
                                            bucket_stats["replicaNumber"]))

    def test_restore_with_invalid_bucket_config_json(self):
        """
            When bucket-config.json in latest backup corrupted,
            The merge backups should fail.
            1. Create a bucket and load docs into it.
            2. Create a backup and validate it.
            3. Run full backup
            4. Load more docs into bucket
            5. Run backup (incremental) and verify.
            6. Modify backup-config.json to make invalid json in content
            7. Run restore to other bucket, restore should fail with error
        """
        gen = BlobGenerator("ent-backup_1", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self._take_n_backups(n=self.backupset.number_of_backups)
        status, output, message = self.backup_list()
        if not status:
            self.fail(message)
        backup_count = 0
        for line in output:
            if re.search("\d{4}-\d{2}-\d{2}T\d{2}_\d{2}_\d{2}.\d+-\d{2}_\d{2}", line):
                backup_name = re.search("\d{4}-\d{2}-\d{2}T\d{2}_\d{2}_\d{2}.\d+-\d{2}_\d{2}",
                                        line).group()
                if backup_name in self.backups:
                    backup_count += 1
                    self.log.info("{0} matched in list command output".format(backup_name))
        backup_bucket_config_path = self.backupset.directory + "/backup" + \
                                    "/" + self.backups[self.backupset.number_of_backups - 1] + \
                                    "/" + self.buckets[0].name + "-*" \
                                                                 "/bucket-config.json"
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        self.log.info("Remove } in bucket-config.json to make it invalid json ")
        remote_client.execute_command("sed -i 's/}//' %s " % backup_bucket_config_path)
        self.log.info("Start to merge backup")
        self.backupset.start = randrange(1, self.backupset.number_of_backups)
        self.backupset.end = randrange(self.backupset.start + 1,
                                       self.backupset.number_of_backups + 1)
        result, output, _ = self.backup_merge()
        if result:
            self.log.info("Here is the output from command %s " % output[0])
            self.fail("merge should failed since bucket-config.json is invalid")
        remote_client.disconnect()

    def test_restore_with_non_exist_bucket(self):
        """
            1. Create a bucket A
            2. Load docs to bucket A
            3. Do backup bucket A
            4. Delete bucket A
            5. Restore to bucket A (non exist bucket)
            6. Expect errors throw out
        """
        gen = BlobGenerator("ent-backup1_", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.log.info("Start doing backup")
        self.backup_create()
        self.backup_cluster()
        self.log.info("Start to delete bucket")
        BucketOperationHelper.delete_all_buckets_or_assert([self.master], self)
        output, _ = self.backup_restore()
        if output and "Error restoring cluster" not in output[0]:
            self.fail("Restore to non exist bucket should fail")

    def test_merge_backup_from_old_and_new_bucket(self):
        """
            1. Create a bucket A
            2. Load docs with key 1
            3. Do backup
            4. Delete bucket A
            5. Re-create bucket A
            6. Load docs with key 2
            7. Do backup
            8. Do merge backup.  Verify backup only contain docs key 2
        """
        gen = BlobGenerator("ent-backup1_", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.log.info("Start doing backup")
        self.backup_create()
        self.backup_cluster()
        if self.bucket_delete:
            self.log.info("Start to delete bucket")
            BucketOperationHelper.delete_all_buckets_or_assert([self.master], self)
            BucketOperationHelper.create_bucket(serverInfo=self.master, test_case=self)
        elif self.bucket_flush:
            self.log.info("Start to flush bucket")
            self._all_buckets_flush()
        gen = BlobGenerator("ent-backup2_", "ent-backup-", self.value_size, end=self.num_items)
        self.log.info("Start to load bucket again with different key")
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_cluster()
        self.backupset.number_of_backups += 1
        status, output, message = self.backup_list()
        if not status:
            self.fail(message)
        self.log.info("Start to merge backup")
        self.backupset.start = randrange(1, self.backupset.number_of_backups)
        self.backupset.end = self.backupset.number_of_backups
        self.merged = True
        result, output, _ = self.backup_merge()
        self.backupset.end -= 1
        status, output, message = self.backup_list()
        if not status:
            self.fail(message)
        current_vseqno = self.get_vbucket_seqnos(self.cluster_to_backup, self.buckets,
                                                 self.skip_consistency, self.per_node)
        self.log.info("*** Start to validate data in merge backup ")
        self.validate_backup_data(self.backupset.backup_host, [self.master],
                                  "ent-backup", False, False, "memory",
                                  self.num_items, "ent-backup1")
        self.backup_cluster_validate(skip_backup=True)

    def test_merge_backup_with_merge_kill_and_re_merge(self):
        """
            1. Create a bucket A
            2. Load docs
            3. Do backup
            4. Load docs
            5. Do backup
            6. Merge backup
            7. Kill merge process
            8. Merge backup again
            Result:  2nd merge should run ok
        """
        gen = BlobGenerator("ent-backup1", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self._take_n_backups(n=self.backupset.number_of_backups)
        status, output, message = self.backup_list()
        if not status:
            self.fail(message)
        self.log.info("Start to merge backup")
        self.backupset.start = randrange(1, self.backupset.number_of_backups)
        self.backupset.end = 2

        self.merged = True
        merge_threads = []
        merge_thread = Thread(target=self.backup_merge)
        merge_threads.append(merge_thread)
        merge_thread.start()
        merge_kill_thread = Thread(target=self._kill_cbbackupmgr)
        merge_threads.append(merge_kill_thread)
        merge_kill_thread.start()
        for merge_thread in merge_threads:
            merge_thread.join()
        status, output, message = self.backup_list()
        if not status:
            self.fail(message)
        result, output, _ = self.backup_merge()
        status, output, message = self.backup_list()
        if not status:
            self.fail(message)

    def test_merge_backup_with_partial_backup(self):
        """
            1. Create a bucket A
            2. Load docs
            3. Do backup
            4. Load docs
            5. Do backup and kill backup process
            6. Merge backup.  Merge should fail
        """
        gen = BlobGenerator("ent-backup1", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self._take_n_backups(n=self.backupset.number_of_backups)
        status, output, message = self.backup_list()
        if not status:
            self.fail(message)
        backup_threads = []
        backup_thread = Thread(target=self.backup_cluster)
        backup_threads.append(backup_thread)
        backup_thread.start()
        backup_kill_thread = Thread(target=self._kill_cbbackupmgr)
        backup_threads.append(backup_kill_thread)
        backup_kill_thread.start()
        for backup_thread in backup_threads:
            backup_thread.join()
        self.backupset.number_of_backups += 1
        self.log.info("Start to merge backup")
        self.backupset.start = randrange(1, self.backupset.number_of_backups)
        self.backupset.end = 3
        self.merged = True
        status, output, _ = self.backup_merge()
        if status:
            self.fail("This merge should fail due to last backup killed, not complete yet")
        elif "Error merging data: Unable to merge" in output[0]:
            self.log.info("Test failed as expected as last backup failed to complete")
        status, output, message = self.backup_list()
        if not status:
            self.fail(message)

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

    def test_merge_backup_with_purge_deleted_keys(self):
        """
           1. Load 100K docs to a bucket A with key 1
           2. Delete 50K docs from bucket A
           3. Load 50K docs with key 2 to bucket A
           4. Take backup
           5. Run compaction on each vbucket to purge all delete keys
           6. Load again 25K docs with key 3
           7. Run backup again
           8. Load another 25K docs with key 4
           9. Run backup.  It should not fail
        """
        self.log.info("Load 1st batch docs")
        create_gen1 = BlobGenerator("ent-backup1", "ent-backup-", self.value_size,
                                    end=self.num_items)
        self._load_all_buckets(self.master, create_gen1, "create", 0)
        self.log.info("Delete half docs of 1st batch")
        delete_gen = BlobGenerator("ent-backup1", "ent-backup-", self.value_size,
                                   end=self.num_items // 2)
        self._load_all_buckets(self.master, delete_gen, "delete", 0)
        self.log.info("Load 2nd batch docs")
        create_gen2 = BlobGenerator("ent-backup2", "ent-backup-", self.value_size,
                                    end=self.num_items // 2)
        self._load_all_buckets(self.master, create_gen2, "create", 0)
        self.log.info("Start backup")
        self.backup_create()
        self.backup_cluster()
        nodes = []
        upto_seq = 100000
        self.log.info("Start compact each vbucket in bucket")

        rest = RestConnection(self.master)
        cluster_nodes = rest.get_nodes()
        for bucket in RestConnection(self.master).get_buckets():
            found = self.get_info_in_database(self.backupset.cluster_host, bucket, "deleted")
            if found:
                shell = RemoteMachineShellConnection(self.backupset.cluster_host)
                shell.compact_vbuckets(len(bucket.vbuckets), cluster_nodes, upto_seq)
                shell.disconnect()
            found = self.get_info_in_database(self.backupset.cluster_host, bucket, "deleted")
            if not found:
                self.log.info("Load another docs to bucket %s " % bucket.name)
                create_gen3 = BlobGenerator("ent-backup3", "ent-backup-", self.value_size,
                                            end=self.num_items // 4)
                self._load_bucket(bucket, self.master, create_gen3, "create",
                                                                 self.expire_time)
                self.backup_cluster()
                create_gen4 = BlobGenerator("ent-backup3", "ent-backup-", self.value_size,
                                            end=self.num_items // 4)
                self._load_bucket(bucket, self.master, create_gen4, "create",
                                                                 self.expire_time)
                self.backup_cluster()
                self.backupset.end = 3
                status, output, message = self.backup_merge()
                if not status:
                    self.fail(message)
            else:
                self.fail("cbcompact failed to purge deleted key")

    def test_merge_backup_with_failover_logs(self):
        """
            1. Load 100K docs into bucket.
            2. Wait for all docs persisted.
            3. Stop persistence.
            4. Load another 100K docs to bucket.
            5. Kill memcached will generate about 4 failover logs.
               ./cbstats localhost:11210 -u username -p pass failovers | grep num_entries
            6. Take backup.
            7. Load another 100K docs
            8. Take backup again.
            Verify:
               Only 1st backup is full backup
               All backup after would be incremental backup
            In 4.5.1, all backups would be full backup
        """
        self.log.info("Load 1st batch docs")
        create_gen1 = BlobGenerator("ent-backup1", "ent-backup-", self.value_size,
                                    end=self.num_items)
        self._load_all_buckets(self.master, create_gen1, "create", 0)
        failed_persisted_bucket = []
        rest = RestConnection(self.master)
        cluster_nodes = rest.get_nodes()
        for bucket in self.buckets:
            ready = RebalanceHelper.wait_for_stats_on_all(self.backupset.cluster_host,
                                                          bucket.name, 'ep_queue_size',
                                                          0, timeout_in_seconds=120)
            if not ready:
                failed_persisted_bucket.append(bucket.name)
        if failed_persisted_bucket:
            self.fail("Buckets %s did not persisted." % failed_persisted_bucket)
        self.log.info("Stop persistence at each node")
        clusters = copy.deepcopy(cluster_nodes)
        shell = RemoteMachineShellConnection(self.backupset.backup_host)
        for bucket in self.buckets:
            for node in clusters:
                shell.execute_command("%scbepctl%s %s:11210 -b %s stop" % \
                                      (self.cli_command_location,
                                       self.cmd_ext,
                                       node.ip,
                                       bucket.name))
        shell.disconnect()
        self.log.info("Load 2nd batch docs")
        create_gen2 = BlobGenerator("ent-backup2", "ent-backup-", self.value_size,
                                    end=self.num_items)
        self._load_all_buckets(self.master, create_gen2, "create", 0)
        self.sleep(5)
        self.log.info("Crash cluster via kill memcached")
        for node in clusters:
            for server in self.servers:
                if node.ip == server.ip:
                    num_entries = 4
                    reach_num_entries = False
                    while not reach_num_entries:
                        shell = RemoteMachineShellConnection(server)
                        shell.kill_memcached()
                        ready = False
                        while not ready:
                            if not RestHelper(RestConnection(server)).is_ns_server_running():
                                self.sleep(10)
                            else:
                                ready = True
                        cmd = "%scbstats%s %s:11210 failovers -u %s -p %s | grep num_entries " \
                              "| gawk%s '{printf $2}' | grep -m 5 '4\|5\|6\|7'" \
                              % (self.cli_command_location, self.cmd_ext, server.ip,
                                 "cbadminbucket", "password", self.cmd_ext)
                        output, error = shell.execute_command(cmd)
                        shell.disconnect()
                        if output:
                            self.log.info("number failover logs entries reached. %s " % output)
                            reach_num_entries = True
        self.backup_create()
        self.log.info("Start backup data")
        self.backup_cluster()
        status, output, message = self.backup_list()
        if not status:
            self.fail(message)
        self.log.info("Load 3rd batch docs")
        create_gen3 = BlobGenerator("ent-backup3", "ent-backup-", self.value_size,
                                    end=self.num_items)
        self._load_all_buckets(self.master, create_gen3, "create", 0)
        self.backup_cluster()
        status, output, message = self.backup_list()
        if not status:
            self.fail(message)

    def test_backupmgr_with_short_option(self):
        """
            Test short option flags at each option
        """
        cmd = "%scbbackupmgr%s " % (self.cli_command_location, self.cmd_ext)
        cmd += "%s " % self.input.param("command", "backup")
        options = " -%s %s " % (self.input.param("repo", "-repo"),
                                self.backupset.name)
        options += " -%s %s" % (self.input.param("archive", "-archive"),
                                self.backupset.directory)
        if self.input.param("command", "backup") != "list":
            options += " -%s http://%s:%s" % (self.input.param("cluster", "-cluster"),
                                              self.backupset.cluster_host.ip,
                                              self.backupset.cluster_host.port)
            options += " -%s Administrator" % self.input.param("bkusername", "-username")
            options += " -%s password" % self.input.param("bkpassword", "-password")
        self.backup_create()
        shell = RemoteMachineShellConnection(self.backupset.backup_host)
        output, error = shell.execute_command("%s %s " % (cmd, options))
        shell.log_command_output(output, error)
        shell.disconnect()
        if error:
            self.fail("There is a error in %s " % error)

    def test_backupmgr_help_display(self):
        """
           Test display help manual in each option
           We do not test compare the whole content but only
           few first lines to make sure manual page displayed.
        """
        display_option = self.input.param("display", "-h")

        if self.input.param("subcommand", None) is None:
            subcommand = ""
        else:
            subcommand = self.input.param("subcommand", None)
        cmd = "{0}cbbackupmgr{1} ".format(self.cli_command_location, self.cmd_ext)
        if display_option == "--help":
            display_option = self.long_help_flag
        elif display_option == "-h":
            self.long_help_flag = self.short_help_flag
        cmd += " {0} {1} ".format(subcommand, display_option)

        shell = RemoteMachineShellConnection(self.backupset.cluster_host)
        output, error = shell.execute_command("{0} ".format(cmd))
        self.log.info("Verify print out help message")
        if display_option == "-h":
            if subcommand == "":
                content = ['cbbackupmgr [<command>] [<args>]', '',
                           '  backup    Backup a Couchbase cluster']
            elif subcommand == "help":
                content = ['cbbackupmgr help [<command>] [<args>]', '',
                           '  archivelayout   View the archive directory layout structure']
            else:
                content = ['cbbackupmgr {0} [<args>]'.format(subcommand), '',
                           'Required Flags:']
            self.validate_help_content(output[:3], content)
        elif display_option == "--help":
            content = None
            if subcommand == "":
                content = \
                    ['CBBACKUPMGR(1) Backup Manual CBBACKUPMGR(1)']
                self.validate_help_content(output, content)
            else:
                subcmd_cap = subcommand.upper()
                content = \
                    ['CBBACKUPMGR-{0}(1) Backup Manual CBBACKUPMGR-{1}(1)'\
                     .format(subcmd_cap, subcmd_cap)]
                self.validate_help_content(output, content)
            if self.bkrs_flag is not None:
                self.assertTrue(self._check_output(self.bkrs_flag, output),
                                 "Missing flag {0} in help content".format(self.bkrs_flag))
        shell.disconnect()

    def test_backup_restore_with_optional_flags(self):
        """
            1. Create a bucket
            2. Load docs to bucket
            3. Backup with optional flags like no-ssl-verify, secure-conn
            4. Verify backup data in backup file
        """
        self.log.info("Load 1st batch docs")
        create_gen1 = BlobGenerator("ent-backup", "ent-backup-", self.value_size,
                                    end=self.num_items)
        self._load_all_buckets(self.master, create_gen1, "create", 0)
        self.backup_create()
        verify_data = True
        output, error = self.backup_cluster()
        if self.backupset.secure_conn:
            if self.backupset.bk_no_cert:
                if self._check_output("Backup successfully completed", output):
                    self.fail("Taking cluster backup failed.")
                elif self._check_output("Error", output):
                    verify_data = False
            else:
                if not self._check_output("Backup successfully completed", output):
                    self.fail("Taking cluster backup failed.")

        if verify_data:
            self.validate_backup_data(self.backupset.backup_host,
                                      self.servers[:self.nodes_init],
                                      "ent-backup", False, False, "memory",
                                      self.num_items, None)
        if self.do_restore:
            self.log.info("Restore with secure connection")
            self.backup_restore()

    def test_restore_with_filter_regex(self):
        """
            1. Create a bucket
            2. Load docs to bucket with key patterned
            3. Backup docs
            4. Delete bucket
            5. Restore docs with regex
            6. Verify only key or value in regex restored to bucket
        """
        key_name = "ent-backup"
        if self.backupset.random_keys:
            key_name = "random_keys"
        self.validate_keys = self.input.param("validate_keys", False)
        if self.validate_keys:
            gen = BlobGenerator(key_name, "ent-backup-", self.value_size,
                                end=self.num_items)
        else:
            gen = DocumentGenerator('random_keys', '{{"age": {0}}}', range(100),
                                    start=0, end=self.num_items)

        self._load_all_buckets(self.master, gen, "create", 0)
        self.log.info("Start backup")
        self.backup_create()
        self.backup_cluster()
        self.backup_restore()
        self.merged = False
        regex_check = self.backupset.filter_keys
        if not self.backupset.filter_keys:
            regex_check = self.backupset.filter_values
        self.validate_backup_data(self.backupset.backup_host,
                                  [self.backupset.restore_cluster_host],
                                  key_name, False, False, "memory",
                                  self.num_items, None,
                                  validate_keys=self.validate_keys,
                                  regex_pattern=regex_check)

    def test_backup_with_rbac(self):
        """
            1. Create a cluster
            2. Create a bucket and load date
            3. Create a user with specific role
                param in conf: new_user
                param in conf: new_role
                Roles:
                  admin, ro_admin, cluster_admin, bucket_full_access[*], bucket_admin[*],
                  views_admin[*],
                  replication_admin, roadmin_no_access, cluster_admin_no_access,
                  bucket_admin_no_access, view_admin_no_access, replication_admin_no_access,
                  view_replication_admin, replication_ro_admin, bucket_view_replication_admin,
            4. Run backup with new user created
            5. Verify if backup command handles user role correctly
        """
        all_buckets = self.input.param("all_buckets", False)
        if self.create_fts_index:
            gen = DocumentGenerator('test_docs', '{{"age": {0}}}', range(100), start=0,
                                    end=self.num_items)
            index_definition = INDEX_DEFINITION
            index_name = index_definition['name'] = "age"
            rest_fts = RestConnection(self.master)
            try:
                self.log.info("Create fts index")
                rest_fts.create_fts_index(index_name, index_definition)
            except Exception as ex:
                self.fail(ex)
        else:
            gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size,
                                end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        if self.create_views:
            self._create_views()
        self.backup_create()

        if all_buckets:
            self.cluster_new_role = self.cluster_new_role + "[*]"

        self.log.info("\n***** Create new user: %s with role: %s to do backup *****"
                      % (self.cluster_new_user, self.cluster_new_role))
        testuser = [{"id": "%s" % self.cluster_new_user,
                     "name": "%s" % self.cluster_new_user,
                     "password": "password"}]
        rolelist = [{"id": "%s" % self.cluster_new_user,
                     "name": "%s" % self.cluster_new_user,
                     "roles": "%s" % self.cluster_new_role}]
        users_can_backup_all = ["admin", "bucket_full_access[*]",
                                "data_backup[*]"]
        users_can_not_backup_all = ["views_admin[*]", "replication_admin",
                                    "replication_target[*]", "data_monitoring[*]",
                                    "data_writer[*]", "data_reader[*]",
                                    "data_dcp_reader[*]", "fts_searcher[*]",
                                    "fts_admin[*]", "query_manage_index[*]",
                                    "ro_admin", "bucket_admin[*]", "cluster_admin"]

        try:
            status = self.add_built_in_server_user(testuser, rolelist)
            if not status:
                self.fail("Fail to add user: %s with role: %s " \
                          % (self.cluster_new_user,
                             self.cluster_new_role))
            output, error = self.backup_cluster()
            success_msg = 'Backup successfully completed'
            fail_msg = "Error backing up cluster:"
            if self.cluster_new_role in users_can_backup_all:
                if not self._check_output(success_msg, output):
                    self.fail("User %s failed to backup data.\n"
                              "Here is the output %s " % \
                              (self.cluster_new_role, output))
            elif self.cluster_new_role in users_can_not_backup_all:
                if not self._check_output(fail_msg, output):
                    self.fail("cbbackupmgr failed to block user to backup")

            status, _, message = self.backup_list()
            if not status:
                self.fail(message)
            if self.do_verify:
                current_vseqno = self.get_vbucket_seqnos(self.cluster_to_backup,
                                                         self.buckets,
                                                         self.skip_consistency,
                                                         self.per_node)
                self.log.info("*** Start to validate data in merge backup ")
                result = self.validate_backup_data(self.backupset.backup_host,
                                                   [self.master],
                                                   "ent-backup", False, False, "memory",
                                                   self.num_items, None)
                self.validate_backup_views()
        except Exception as e:
            if e:
                print("Exception error:   ", e)
            if self.cluster_new_role in users_can_not_backup_all:
                error_found = False
                error_messages = ["Error backing up cluster: Forbidden",
                                  "Could not find file shard_0.sqlite",
                                  "Error backing up cluster: Invalid permissions",
                                  "Database file is empty",
                                  "Error backing up cluster: Unable to find the latest vbucket"]
                if self.do_verify:
                    if str(e) in error_messages:
                        error_found = True
                    if not error_found:
                        raise Exception("cbbackupmgr does not block user role: %s to backup" \
                                        % self.cluster_new_role)
                    if self.cluster_new_role == "views_admin[*]" and self.create_views:
                        status, mesg = self.validate_backup_views(self.backupset.backup_host)
                        if not status:
                            raise Exception(mesg)
                if "Expected error message not thrown" in str(e):
                    raise Exception("cbbackupmgr does not block user role: %s to backup" \
                                    % self.cluster_new_role)
            if self.cluster_new_role in users_can_backup_all:
                if not self._check_output(success_msg, output):
                    self.fail(e)

        finally:
            self.log.info("Delete new create user: %s " % self.cluster_new_user)
            shell = RemoteMachineShellConnection(self.backupset.backup_host)
            curl_path = ""
            if self.os_name == "windows":
                curl_path = self.cli_command_location
            cmd = "%scurl%s -g -X %s -u %s:%s http://%s:8091/settings/rbac/users/local/%s" \
                  % (curl_path,
                     self.cmd_ext,
                     "DELETE",
                     self.master.rest_username,
                     self.master.rest_password,
                     self.backupset.cluster_host.ip,
                     self.cluster_new_user)
            output, error = shell.execute_command(cmd)
            shell.disconnect()

    def test_restore_with_rbac(self):
        """
            1. Create a backupdata set.
            2. Setup cluster.
            3. Restore data back to cluster

            Important:
            This test need to copy entbackup.zip and entbackup-fts.zip
            to /root or /cygdrive/c/Users/Administrator in backup host.
            Files location: 172.23.121.227:/root/entba*.zip
        """
        all_buckets = self.input.param("all_buckets", False)
        self.log.info("Copy backup dataset to tmp dir")
        shell = RemoteMachineShellConnection(self.backupset.backup_host)
        shell.execute_command("rm -rf %s " % self.backupset.directory)
        fts = ""
        backup_file = ENT_BKRS
        if self.create_fts_index:
            backup_file = ENT_BKRS_FTS
            fts = "-fts"
        backup_dir_found = False
        backup_dir = "entbackup" + fts
        output, error = shell.execute_command("ls | grep entbackup")
        self.log.info("check if %s dir exists on this server " % backup_dir)
        if output:
            for x in output:
                if x == backup_dir:
                    backup_dir_found = True
        if not backup_dir_found:
            self.log.info("%s dir does not exist on this server.  Downloading.. "
                                                                   % backup_dir)
            shell.execute_command("%s -q %s --no-check-certificate " % (self.wget, backup_file))
            shell.execute_command("tar -zxvf %s.tgz " % backup_dir)
        if "-" in self.cluster_new_role:
            self.cluster_new_role = self.cluster_new_role.replace("-", ",")
        shell.check_cmd("unzip")
        shell.execute_command("cp -r entbackup%s %s/entbackup" % (fts, self.tmp_path))
        output, error = shell.execute_command("cd %s/backup/*/*/data; " \
                                              "unzip shar*.zip" \
                                              % self.backupset.directory)
        shell.log_command_output(output, error)
        shell.execute_command("echo '' > {0}/logs/backup.log" \
                              .format(self.backupset.directory))
        shell.disconnect()
        status, _, message = self.backup_list()
        if not status:
            self.fail(message)

        self.log.info("Restore data from backup files")

        if all_buckets:
            self.cluster_new_role = self.cluster_new_role + "[*]"

        self.log.info("\n***** Create new user: %s with role: %s to do backup *****"
                      % (self.cluster_new_user, self.cluster_new_role))
        testuser = [{"id": "%s" % self.cluster_new_user,
                     "name": "%s" % self.cluster_new_user,
                     "password": "password"}]
        rolelist = [{"id": "%s" % self.cluster_new_user,
                     "name": "%s" % self.cluster_new_user,
                     "roles": "%s" % self.cluster_new_role}]
        try:
            status = self.add_built_in_server_user(testuser, rolelist)
            if not status:
                self.fail("Fail to add user: %s with role: %s " \
                          % (self.cluster_new_user,
                             self.cluster_new_role))

            users_can_restore_all = ["admin", "bucket_full_access[*]",
                                     "data_backup[*]"]
            users_can_not_restore_all = ["views_admin[*]", "ro_admin",
                                         "replication_admin", "data_monitoring[*]",
                                         "data_writer[*]", "data_reader[*]",
                                         "data_dcp_reader[*]", "fts_searcher[*]",
                                         "fts_admin[*]", "query_manage_index[*]",
                                         "replication_target[*]", "cluster_admin",
                                         "bucket_admin[*]"]
            if self.cluster_new_role in users_can_not_restore_all:
                self.should_fail = True
            output, error = self.backup_restore()
            success_msg = 'Restore completed successfully'
            fail_msg = "Error restoring cluster:"

            failed_persisted_bucket = []
            ready = RebalanceHelper.wait_for_stats_on_all(self.backupset.cluster_host,
                                                          "default", 'ep_queue_size',
                                                          0, timeout_in_seconds=120)
            if not ready:
                failed_persisted_bucket.append("default")
            if failed_persisted_bucket:
                self.fail("Buckets %s did not persisted." % failed_persisted_bucket)

            self.sleep(3)
            rest = RestConnection(self.master)
            actual_keys = rest.get_active_key_count("default")
            print("\nActual keys in default bucket: %s \n" % actual_keys)
            if self.cluster_new_role in users_can_restore_all:
                if not self._check_output(success_msg, output):
                    self.fail("User with roles: %s failed to restore data.\n"
                              "Here is the output %s " % \
                              (self.cluster_new_role, output))

            roles = []
            if "," in self.cluster_new_role:
                roles = self.cluster_new_role.split(",")
                if set(roles) & set(users_can_not_restore_all) and \
                                set(roles) & set(users_can_restore_all):
                    if not self._check_output(success_msg, output):
                        self.fail("User: %s failed to restore data with roles: %s. " \
                                  "Here is the output %s " % \
                                  (self.cluster_new_user, roles, output))
                    if int(actual_keys) != 1000:
                        self.fail("User: %s failed to restore data with roles: %s. " \
                                  "Here is the actual docs in bucket %s " % \
                                  (self.cluster_new_user, roles, actual_keys))
            elif self.cluster_new_role in users_can_not_restore_all:
                if int(actual_keys) == 1000:
                    self.fail("User: %s with role: %s should not allow to restore data" \
                              % (self.cluster_new_user,
                                 self.cluster_new_role))
                if not self._check_output(fail_msg, output):
                    self.fail("cbbackupmgr failed to block user to restore")
        finally:
            self.log.info("Delete new create user: %s " % self.cluster_new_user)
            shell = RemoteMachineShellConnection(self.backupset.backup_host)
            curl_path = ""
            if self.os_name == "windows":
                curl_path = self.cli_command_location
            cmd = "%scurl%s -g -X %s -u %s:%s http://%s:8091/settings/rbac/users/local/%s" \
                  % (curl_path,
                     self.cmd_ext,
                     "DELETE",
                     self.master.rest_username,
                     self.master.rest_password,
                     self.backupset.cluster_host.ip,
                     self.cluster_new_user)
            output, error = shell.execute_command(cmd)
            shell.disconnect()

    def test_backup_restore_with_nodes_reshuffle(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Enlists the default zone of current cluster - backsup the cluster and validates
        3. Creates a new zone - shuffles cluster host to new zone
        4. Restores to cluster host and validates
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        rest_conn = RestConnection(self.backupset.cluster_host)
        zones = list(rest_conn.get_zone_names().keys())
        source_zone = zones[0]
        target_zone = "test_backup_restore"
        self.log.info("Current nodes in group {0} : {1}".format(source_zone,
                                                                str(list(rest_conn.get_nodes_in_zone(source_zone).keys()))))
        self.log.info("Taking backup with current groups setup")
        self.backup_create()
        self.backup_cluster_validate()
        self.log.info("Creating new zone " + target_zone)
        rest_conn.add_zone(target_zone)
        self.log.info("Moving {0} to new zone {1}".format(self.backupset.cluster_host.ip, target_zone))
        rest_conn.shuffle_nodes_in_zones(["{0}".format(self.backupset.cluster_host.ip)], source_zone, target_zone)
        self.log.info("Restoring to {0} after group change".format(self.backupset.cluster_host.ip))
        try:
            self.backup_restore_validate()
        except Exception as ex:
            self.fail(str(ex))
        finally:
            self.log.info("Moving {0} back to old zone {1}".format(self.backupset.cluster_host.ip, source_zone))
            rest_conn.shuffle_nodes_in_zones(["{0}".format(self.backupset.cluster_host.ip)], target_zone, source_zone)
            self.log.info("Deleting new zone " + target_zone)
            rest_conn.delete_zone(target_zone)

    def test_backup_restore_with_firewall(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates backupset on backup host
        3. Enables firewall on cluster host and validates if backup cluster command throws expected error
        4. Disables firewall on cluster host, takes backup and validates
        5. Enables firewall on restore host and validates if backup restore command throws expected error
        6. Disables firewall on restore host, restores and validates
        """
        if self.os_name == "windows" or self.nonroot:
            self.log.info("This firewall test does not run on windows or nonroot user")
            return
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self.log.info("Enabling firewall on cluster host before backup")
        RemoteUtilHelper.enable_firewall(self.backupset.cluster_host)
        self.enable_firewall = True
        try:
            output, error = self.backup_cluster()
            self.assertTrue(self._check_output("getsockopt: connection refused", output),
                            "Expected error not thrown by backup cluster when firewall enabled")
        finally:
            self.log.info("Disabling firewall on cluster host to take backup")
            conn = RemoteMachineShellConnection(self.backupset.cluster_host)
            conn.disable_firewall()
            conn.disconnect()
            self.enable_firewall = False
        self.log.info("Trying backup now")
        self.backup_cluster_validate()
        self.log.info("Enabling firewall on restore host before restore")
        RemoteUtilHelper.enable_firewall(self.backupset.restore_cluster_host)
        self.enable_firewall = True
        """ reset restore cluster to same services as backup cluster """
        try:
            output, error = self.backup_restore()
            mesg = "getsockopt: connection refused"
            if self.skip_buckets:
                mesg = "Error restoring cluster:"
            self.assertTrue(self._check_output(mesg, output),
                            "Expected error not thrown by backup restore when firewall enabled")
        finally:
            self.log.info("Disabling firewall on restore host to restore")
            conn = RemoteMachineShellConnection(self.backupset.restore_cluster_host)
            conn.disable_firewall()
            conn.disconnect()
            self.enable_firewall = False
        self.log.info("Trying restore now")
        self.skip_buckets = False
        """ Need to reset restore node with services the same as in backup cluster """
        rest = RestConnection(self.backupset.restore_cluster_host)
        rest.force_eject_node()

        master_services = self.get_services([self.backupset.cluster_host],
                                            self.services_init, start_node=0)
        info = rest.get_nodes_self()
        if info.memoryQuota and int(info.memoryQuota) > 0:
            self.quota = info.memoryQuota
        rest.init_node()
        self.sleep(10)
        self.backup_restore_validate()

    def test_backup_restore_with_audit(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates backupset on backup host
        3. Creates a backup of the cluster host - verifies if corresponding entry was created in audit log
        4. Restores data on to restore host - verifies if corresponding entry was created in audit log
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        audit_obj = audit(AUDITBACKUPID, self.backupset.cluster_host)
        status = audit_obj.getAuditStatus()
        self.log.info("Audit status on {0} is {1}".format(self.backupset.cluster_host.ip, status))
        if not status:
            self.log.info("Enabling audit on {0}".format(self.backupset.cluster_host.ip))
            audit_obj.setAuditEnable('true')
        self.backup_create()
        self.backup_cluster()
        field_verified, value_verified = audit_obj.validateEvents(self._get_event_expected_results(action='backup'))
        self.assertTrue(field_verified, "One of the fields is not matching")
        self.assertTrue(value_verified, "Values for one of the fields is not matching")
        audit_obj = audit(AUDITBACKUPID, self.backupset.restore_cluster_host)
        status = audit_obj.getAuditStatus()
        self.log.info("Audit status on {0} is {1}".format(self.backupset.restore_cluster_host.ip, status))
        if not status:
            self.log.info("Enabling audit on {0}".format(self.backupset.restore_cluster_host.ip))
            audit_obj.setAuditEnable('true')
        self.backup_restore()
        audit_obj = audit(AUDITRESTOREID, self.backupset.restore_cluster_host)
        field_verified, value_verified = audit_obj.validateEvents(self._get_event_expected_results(action='restore'))
        self.assertTrue(field_verified, "One of the fields is not matching")
        self.assertTrue(value_verified, "Values for one of the fields is not matching")

    def _get_event_expected_results(self, action):
        if action == 'backup':
            expected_results = {
                "real_userid:source": "memcached",
                "real_userid:user": "default",
                "name": "opened DCP connection",
                "id": AUDITBACKUPID,
                "description": "opened DCP connection",
                "timestamp": "{0}".format(self.backups[0]),
                "bucket": "{0}".format(self.buckets[0].name),
                "sockname": "{0}:11210".format(self.backupset.cluster_host.ip),
                "peername": "{0}".format(self.backupset.backup_host.ip)
            }
        elif action == 'restore':
            expected_results = {
                "real_userid:source": "memcached",
                "real_userid:user": "default",
                "name": "authentication succeeded",
                "id": AUDITRESTOREID,
                "description": "Authentication to the cluster succeeded",
                "timestamp": "{0}".format(self.backups[0]),
                "bucket": "{0}".format(self.buckets[0].name),
                "sockname": "{0}:11210".format(self.backupset.restore_cluster_host.ip),
                "peername": "{0}".format(self.backupset.backup_host.ip)
            }
        return expected_results

    def test_backup_restore_with_lesser_nodes(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Adds another node to restore cluster and rebalances - note the test has to be run with nodes_init >= 3 so
           that cluster host had more nodes than restore host
        3. Creates backupset on backup host
        4. Creates backup of cluster host with 3 or more number of nodes and validates
        5. Restores to restore host with lesser number of nodes (2) and validates
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        rest_conn = RestConnection(self.backupset.restore_cluster_host)
        rest_conn.add_node(self.input.clusters[0][1].rest_username, self.input.clusters[0][1].rest_password,
                           self.input.clusters[0][1].ip)
        rebalance = self.cluster.async_rebalance(self.cluster_to_restore, [], [])
        rebalance.result()
        self.backup_create()
        self.backup_cluster_validate()
        self.backup_restore_validate()

    def test_backup_with_full_disk(self):
        """
        Things to be done before running this testcase:
            - scripts/install.py has to be run with init_nodes=False
            - scripts/cbqe3043.py has to be run against the ini file - this script will mount a 20MB partition on the
              nodes required for the test
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Sets backup directory to the 20MB partition and creates a backupset
        3. Fills up 20MB partition
        4. Keeps taking backup until no space left on device error is hit
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backupset.directory = "/cbqe3043/entbackup"
        self.backup_create()
        conn = RemoteMachineShellConnection(self.backupset.backup_host)
        output, error = conn.execute_command("dd if=/dev/zero of=/cbqe3043/file bs=256M count=50")
        conn.log_command_output(output, error)
        output, error = self.backup_cluster()
        while self._check_output("Backup successfully completed", output):
            gen = BlobGenerator("ent-backup{0}{0}".format(randint(1, 10000)), "ent-backup-",
                                 self.value_size, end=self.num_items)
            self._load_all_buckets(self.master, gen, "create", 0)
            output, error = self.backup_cluster()
        error_msg = "Error backing up cluster: Unable to read data because range.json is corrupt,"
        self.assertTrue(self._check_output(error_msg, output),
                        "Expected error message not thrown by backup when disk is full")
        self.log.info("Expected error thrown by backup command")
        conn.execute_command("rm -rf /cbqe3043/file")
        conn.disconnect()

    def test_backup_and_restore_with_map_buckets(self):
        """
        1. Creates specified buckets on the cluster and loads it with given number
           of items - memcached bucket has to be created for this test
            (memcached_buckets=1)
        2. Creates a backupset, takes backup of the cluster host and validates
        3. Executes list command on the backup and validates that memcached bucket
           has been skipped
        4. Restores the backup and validates
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size,
                            end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        if self.create_gsi:
            self.create_indexes()
        self.backup_create()
        self.backup_cluster()
        status, output, message = self.backup_list()
        if not status:
            self.fail("Getting backup list to validate memcached buckets failed.")
        for line in output:
            self.assertTrue("memcached_bucket0" not in line,
                            "Memcached bucket found in backup list output after backup")
        self.log.info("Memcached bucket not found in backup list output after backup as expected")
        self.backup_restore()

    def test_backup_with_erlang_crash_and_restart(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number
           of items
        2. Creates a backupset on the backup host
        3. Initiates a backup - while backup is going on kills and restarts
           erlang process
        4. Validates backup output
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        backup_result = self.cluster.async_backup_cluster(cluster_host=self.backupset.cluster_host,
                                                          backup_host=self.backupset.backup_host,
                                                          directory=self.backupset.directory, name=self.backupset.name,
                                                          resume=self.backupset.resume, purge=self.backupset.purge,
                                                          no_progress_bar=self.no_progress_bar,
                                                          cli_command_location=self.cli_command_location,
                                                          cb_version=self.cb_version)
        self.sleep(10)
        conn = RemoteMachineShellConnection(self.backupset.cluster_host)
        conn.kill_erlang()
        conn.start_couchbase()
        output = backup_result.result(timeout=200)
        self.assertTrue(self._check_output("Backup successfully completed", output),
                        "Backup failed with erlang crash and restart within 180 seconds")
        self.log.info("Backup succeeded with erlang crash and restart within 180 seconds")
        conn.disconnect()

    def test_backup_with_couchbase_stop_and_start(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a backupset on the backup host
        3. Initiates a backup - while backup is going on kills and restarts couchbase server
        4. Validates backup output
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        backup_result = self.cluster.async_backup_cluster(cluster_host=self.backupset.cluster_host,
                                                          backup_host=self.backupset.backup_host,
                                                          directory=self.backupset.directory, name=self.backupset.name,
                                                          resume=self.backupset.resume, purge=self.backupset.purge,
                                                          no_progress_bar=self.no_progress_bar,
                                                          cli_command_location=self.cli_command_location,
                                                          cb_version=self.cb_version)
        self.sleep(10)
        conn = RemoteMachineShellConnection(self.backupset.cluster_host)
        conn.stop_couchbase()
        conn.start_couchbase()
        conn.disconnect()
        output = backup_result.result(timeout=200)
        self.assertTrue(self._check_output("Backup successfully completed", output),
                        "Backup failed with couchbase stop and start within 180 seconds")
        self.log.info("Backup succeeded with couchbase stop and start within 180 seconds")

    def test_backup_with_memcached_crash_and_restart(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a backupset on the backup host
        3. Initiates a backup - while backup is going on kills and restarts memcached process
        4. Validates backup output
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        backup_result = self.cluster.async_backup_cluster(
                                            cluster_host=self.backupset.cluster_host,
                                            backup_host=self.backupset.backup_host,
                                            directory=self.backupset.directory,
                                            name=self.backupset.name,
                                            resume=self.backupset.resume,
                                            purge=self.backupset.purge,
                                            no_progress_bar=self.no_progress_bar,
                                            cli_command_location=self.cli_command_location,
                                            cb_version=self.cb_version)
        self.sleep(10)
        conn = RemoteMachineShellConnection(self.backupset.cluster_host)
        conn.pause_memcached()
        conn.unpause_memcached()
        conn.disconnect()
        output = backup_result.result(timeout=200)
        self.assertTrue(self._check_output("Backup successfully completed", output),
                        "Backup failed with memcached crash and restart within 180 seconds")
        self.log.info("Backup succeeded with memcached crash and restart within 180 seconds")

    def test_backup_with_erlang_crash(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a backupset on the backup host
        3. Initiates a backup - while backup is going on kills erlang process
        4. Waits for 200s and Validates backup error
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        try:
            backup_result = self.cluster.async_backup_cluster(
                                                cluster_host=self.backupset.cluster_host,
                                                backup_host=self.backupset.backup_host,
                                                directory=self.backupset.directory,
                                                name=self.backupset.name,
                                                resume=self.backupset.resume,
                                                purge=self.backupset.purge,
                                                no_progress_bar=self.no_progress_bar,
                                                cli_command_location=self.cli_command_location,
                                                cb_version=self.cb_version)
            if self.os_name != "windows":
                self.sleep(10)
            conn = RemoteMachineShellConnection(self.backupset.cluster_host)
            conn.kill_erlang(self.os_name)
            output = backup_result.result(timeout=200)
            if self.debug_logs:
                print("Raw output from backup run: ", output)
            error_mesgs = ["Error backing up cluster: Not all data was backed up due to",
                "No connection could be made because the target machine actively refused it."]
            error_found = False
            for error in error_mesgs:
                if self._check_output(error, output):
                    error_found = True
            if not error_found:
                raise("Expected error message not thrown by Backup 180 seconds after erlang crash")
        except Exception as ex:
            self.fail(str(ex))
        finally:
            conn.start_couchbase()
            conn.disconnect()
            self.sleep(30)

    def test_backup_with_couchbase_stop(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a backupset on the backup host
        3. Initiates a backup - while backup is going on kills couchbase server
        4. Waits for 200s and Validates backup error
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        try:
            backup_result = self.cluster.async_backup_cluster(cluster_host=self.backupset.cluster_host,
                                                              backup_host=self.backupset.backup_host,
                                                              directory=self.backupset.directory,
                                                              name=self.backupset.name,
                                                              resume=self.backupset.resume, purge=self.backupset.purge,
                                                              no_progress_bar=self.no_progress_bar,
                                                              cli_command_location=self.cli_command_location,
                                                              cb_version=self.cb_version)
            self.sleep(10)
            conn = RemoteMachineShellConnection(self.backupset.cluster_host)
            conn.stop_couchbase()
            output = backup_result.result(timeout=200)
            self.assertTrue(self._check_output(
                "Error backing up cluster: Not all data was backed up due to connectivity issues.", output),
                "Expected error message not thrown by Backup 180 seconds after couchbase-server stop")
            self.log.info("Expected error message thrown by Backup 180 seconds after couchbase-server stop")
        except Exception as ex:
            self.fail(str(ex))
        finally:
            conn.start_couchbase()
            conn.disconnect()
            self.sleep(30)

    def test_backup_with_memcached_crash(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a backupset on the backup host
        3. Initiates a backup - while backup is going on kills memcached process
        4. Waits for 200s and Validates backup error
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        try:
            conn = RemoteMachineShellConnection(self.backupset.cluster_host)
            conn.pause_memcached(self.os_name)
            self.sleep(17, "time needs for memcached process completely stopped")
            backup_result = self.cluster.async_backup_cluster(
                                                cluster_host=self.backupset.cluster_host,
                                                backup_host=self.backupset.backup_host,
                                                directory=self.backupset.directory,
                                                name=self.backupset.name,
                                                resume=self.backupset.resume,
                                                purge=self.backupset.purge,
                                                no_progress_bar=self.no_progress_bar,
                                                cli_command_location=self.cli_command_location,
                                                cb_version=self.cb_version)

            self.sleep(10)
            output = backup_result.result(timeout=200)
            mesg = "Error backing up cluster: Unable to find the latest vbucket sequence numbers."
            self.assertTrue(self._check_output(mesg, output),
                "Expected error message not thrown by Backup 180 seconds after memcached crash")
            self.log.info("Expected error thrown by Backup 180 seconds after memcached crash")
        except Exception as ex:
            self.fail(str(ex))
        finally:
            conn.unpause_memcached(self.os_name)
            self.sleep(30)
            conn.disconnect()

    def test_restore_with_erlang_crash_and_restart(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a backupset on the backup host and backsup data
        3. Initiates a restore - while restore is going on kills and restarts erlang process
        4. Validates restore output
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self.backup_cluster()
        rest_conn = RestConnection(self.backupset.restore_cluster_host)
        rest_conn.create_bucket(bucket="default", ramQuotaMB=512)
        restore_result = self.cluster.async_restore_cluster(
                                                restore_host=self.backupset.restore_cluster_host,
                                                backup_host=self.backupset.backup_host,
                                                backups=self.backups, start=self.backupset.start,
                                                end=self.backupset.end,
                                                directory=self.backupset.directory,
                                                name=self.backupset.name,
                                                force_updates=self.backupset.force_updates,
                                                no_progress_bar=self.no_progress_bar,
                                                cli_command_location=self.cli_command_location,
                                                cb_version=self.cb_version)
        self.sleep(10)
        conn = RemoteMachineShellConnection(self.backupset.restore_cluster_host)
        conn.kill_erlang(self.os_name)
        conn.start_couchbase()
        conn.disconnect()
        timeout_now = 400
        if self.os_name == "windows":
            timeout_now = 600
        output = restore_result.result(timeout=timeout_now)
        self.assertTrue(self._check_output("Restore completed successfully", output),
                        "Restore failed with erlang crash and restart within 180 seconds")
        self.log.info("Restore succeeded with erlang crash and restart within 180 seconds")

    def test_restore_with_couchbase_stop_and_start(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a backupset on the backup host and backsup data
        3. Initiates a restore - while restore is going on kills and restarts couchbase process
        4. Validates restore output
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self.backup_cluster()
        rest_conn = RestConnection(self.backupset.restore_cluster_host)
        rest_conn.create_bucket(bucket="default", ramQuotaMB=512)
        restore_result = self.cluster.async_restore_cluster(
                                                restore_host=self.backupset.restore_cluster_host,
                                                backup_host=self.backupset.backup_host,
                                                backups=self.backups, start=self.backupset.start,
                                                end=self.backupset.end,
                                                directory=self.backupset.directory,
                                                name=self.backupset.name,
                                                force_updates=self.backupset.force_updates,
                                                no_progress_bar=self.no_progress_bar,
                                                cli_command_location=self.cli_command_location,
                                                cb_version=self.cb_version)
        self.sleep(10)
        conn = RemoteMachineShellConnection(self.backupset.restore_cluster_host)
        conn.stop_couchbase()
        self.sleep(10)
        conn.start_couchbase()
        conn.disconnect()
        output = restore_result.result(timeout=500)
        self.assertTrue(self._check_output("Restore completed successfully", output),
                        "Restore failed with couchbase stop and start within 180 seconds")
        self.log.info("Restore succeeded with couchbase stop and start within 180 seconds")

    def test_restore_with_memcached_crash_and_restart(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a backupset on the backup host and backsup data
        3. Initiates a restore - while restore is going on kills and restarts memcached process
        4. Validates restore output
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self.backup_cluster()
        rest_conn = RestConnection(self.backupset.restore_cluster_host)
        rest_conn.create_bucket(bucket="default", ramQuotaMB=512)
        restore_result = self.cluster.async_restore_cluster(restore_host=self.backupset.restore_cluster_host,
                                                            backup_host=self.backupset.backup_host,
                                                            backups=self.backups, start=self.backupset.start,
                                                            end=self.backupset.end, directory=self.backupset.directory,
                                                            name=self.backupset.name,
                                                            force_updates=self.backupset.force_updates,
                                                            no_progress_bar=self.no_progress_bar,
                                                            cli_command_location=self.cli_command_location,
                                                            cb_version=self.cb_version)
        self.sleep(10)
        conn = RemoteMachineShellConnection(self.backupset.restore_cluster_host)
        conn.pause_memcached(self.os_name)
        conn.unpause_memcached(self.os_name)
        conn.disconnect()
        output = restore_result.result(timeout=600)
        self.assertTrue(self._check_output("Restore completed successfully", output),
                        "Restore failed with memcached crash and restart within 400 seconds")
        self.log.info("Restore succeeded with memcached crash and restart within 400 seconds")

    def test_restore_with_erlang_crash(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a backupset on the backup host and backsup data
        3. Initiates a restore - while restore is going on kills erlang process
        4. Waits for 200s and Validates restore output
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self.backup_cluster()
        rest_conn = RestConnection(self.backupset.restore_cluster_host)
        rest_conn.create_bucket(bucket="default", ramQuotaMB=512)
        try:
            restore_result = self.cluster.async_restore_cluster(
                                            restore_host=self.backupset.restore_cluster_host,
                                            backup_host=self.backupset.backup_host,
                                            backups=self.backups, start=self.backupset.start,
                                            end=self.backupset.end,
                                            directory=self.backupset.directory,
                                            name=self.backupset.name,
                                            force_updates=self.backupset.force_updates,
                                            no_progress_bar=self.no_progress_bar,
                                            cli_command_location=self.cli_command_location,
                                            cb_version=self.cb_version)
            conn = RemoteMachineShellConnection(self.backupset.restore_cluster_host)
            conn.kill_erlang(self.os_name)
            output = restore_result.result(timeout=300)
            self.assertTrue(self._check_output(
                "Error restoring cluster: Not all data was sent to Couchbase", output),
                "Expected error message not thrown by Restore 180 seconds after erlang crash")
            self.log.info("Expected error thrown by Restore 180 seconds after erlang crash")
        except Exception as ex:
            self.fail(str(ex))
        finally:
            conn.start_couchbase()
            conn.disconnect()
            self.sleep(30)

    def test_restore_with_couchbase_stop(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a backupset on the backup host and backsup data
        3. Initiates a restore - while restore is going on kills couchbase server
        4. Waits for 200s and Validates restore output
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self.backup_cluster()
        rest_conn = RestConnection(self.backupset.restore_cluster_host)
        rest_conn.create_bucket(bucket="default", ramQuotaMB=512)
        try:
            restore_result = self.cluster.async_restore_cluster(restore_host=self.backupset.restore_cluster_host,
                                                                backup_host=self.backupset.backup_host,
                                                                backups=self.backups, start=self.backupset.start,
                                                                end=self.backupset.end,
                                                                directory=self.backupset.directory,
                                                                name=self.backupset.name,
                                                                force_updates=self.backupset.force_updates,
                                                                no_progress_bar=self.no_progress_bar,
                                                                cli_command_location=self.cli_command_location,
                                                                cb_version=self.cb_version)
            self.sleep(10)
            conn = RemoteMachineShellConnection(self.backupset.restore_cluster_host)
            conn.stop_couchbase()
            output = restore_result.result(timeout=300)
            self.assertTrue(self._check_output(
                "Error restoring cluster: Not all data was sent to Couchbase due to connectivity issues.", output),
                "Expected error message not thrown by Restore 180 seconds after couchbase-server stop")
            self.log.info("Expected error message thrown by Restore 180 seconds after couchbase-server stop")
        except Exception as ex:
            self.fail(str(ex))
        finally:
            conn.start_couchbase()
            conn.disconnect()
            self.sleep(30)

    def test_restore_with_memcached_crash(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a backupset on the backup host and backsup data
        3. Initiates a restore - while restore is going on kills memcached process
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self.backup_cluster()
        rest_conn = RestConnection(self.backupset.restore_cluster_host)
        rest_conn.create_bucket(bucket="default", ramQuotaMB=512)
        try:
            conn = RemoteMachineShellConnection(self.backupset.restore_cluster_host)
            conn.pause_memcached(self.os_name)
            output, error = self.backup_restore()
            self.assertTrue(self._check_output(
                "Error restoring cluster: failed to connect", output),
                "Expected error message not thrown by Restore 180 seconds after memcached crash")
            self.log.info("Expected error thrown by Restore 180 seconds after memcached crash")
        except Exception as ex:
            self.fail(str(ex))
        finally:
            conn.unpause_memcached(self.os_name)
            conn.disconnect()
            self.sleep(30)

    def test_backup_merge(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Takes specified number of backups (param number_of_backups - should be atleast 2 for this test case)
        3. Executes list command and validates if all backups are present
        4. Randomly selects a start and end and merges the backups
        5. Executes list command again and validates if the new merges set of backups are listed
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self._take_n_backups(n=self.backupset.number_of_backups)
        status, output, message = self.backup_list()
        if not status:
            self.fail(message)
        backup_count = 0
        """ remove last 6 chars of offset time in backup name"""
        if self.backups and self.backups[0][-3:] == "_00":
            strip_backupset = [s[:-6] for s in self.backups]

        for line in output:
            if re.search("\d{4}-\d{2}-\d{2}T\d{2}_\d{2}_\d{2}.\d+", line):
                backup_name = re.search("\d{4}-\d{2}-\d{2}T\d{2}_\d{2}_\d{2}.\d+", line).group()
                if self.debug_logs:
                    print("backup name ", backup_name)
                    print("backup set  ", strip_backupset)
                if backup_name in strip_backupset:
                    backup_count += 1
                    self.log.info("{0} matched in list command output".format(backup_name))
        self.assertEqual(backup_count, len(strip_backupset), "Initial number of backups did not match")
        self.log.info("Initial number of backups matched")
        self.backupset.start = randrange(1, self.backupset.number_of_backups)
        self.backupset.end = randrange(self.backupset.start + 1, self.backupset.number_of_backups + 1)
        status, output, message = self.backup_merge()
        if not status:
            self.fail(message)
        status, output, message = self.backup_list()
        if not status:
            self.fail(message)
        backup_count = 0
        """ remove last 6 chars of offset time in backup name"""
        if self.backups and self.backups[0][-3:] == "_00":
            strip_backupset = [s[:-6] for s in self.backups]

        for line in output:
            if re.search("\d{4}-\d{2}-\d{2}T\d{2}_\d{2}_\d{2}.\d+", line):
                backup_name = re.search("\d{4}-\d{2}-\d{2}T\d{2}_\d{2}_\d{2}.\d+", line).group()
                if self.debug_logs:
                    print("backup name ", backup_name)
                    print("backup set  ", strip_backupset)
                if backup_name in strip_backupset:
                    backup_count += 1
                    self.log.info("{0} matched in list command output".format(backup_name))
        self.assertEqual(backup_count, len(strip_backupset), "Merged number of backups did not match")
        self.log.info("Merged number of backups matched")

    def test_backup_merge_with_restore(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Takes two backups - restores from the backups and validates
        3. Merges both the backups - restores from merged backup and validates
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self._take_n_backups(n=2)
        self.backupset.start = 1
        self.backupset.end = 2
        output, error = self.backup_restore()
        if error:
            self.fail("Restoring backup failed: {0}".format(error))
        self.log.info("Finished restoring backup before merging")
        status, output, message = self.backup_merge()
        if not status:
            self.fail(message)
        self.backupset.start = 1
        self.backupset.end = 1
        output, error = self.backup_restore()
        if error:
            self.fail("Restoring backup failed")
        self.log.info("Finished restoring backup after merging")

    def test_backup_merge_with_unmerged(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Takes two backups - merges them into one
        3. Takes 2 more backups - merges the new backups with already merged ones and validates
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self._take_n_backups(n=2)
        self.backupset.start = 1
        self.backupset.end = 2
        self.log.info("Merging existing incremental backups")
        status, output, message = self.backup_merge()
        if not status:
            self.fail(message)
        self.log.info("Taking more backups")
        self._take_n_backups(n=2)
        self.backupset.start = 1
        self.backupset.end = 3
        self.log.info("Merging new backups into already merged backup")
        status, output, message = self.backup_merge()
        if not status:
            self.fail(message)
        self.log.info("Successfully merged new backups with already merged backup")

    def test_merge_backup_with_multi_threads(self):
        """
            1. Create a cluster with default bucket
            2. Load default bucket with key1
            3. Create backup with default one thread
            4. Load again to bucket with key2
            5. Create backup with 2 threads
            6. Merge backup.  All backup should contain doc key1 and key2
        """
        gen = BlobGenerator("ent-backup1", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.log.info("Start doing backup")
        self.backup_create()
        self.backup_cluster()
        gen = BlobGenerator("ent-backup2", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_cluster(self.threads_count)
        self.backupset.number_of_backups += 1
        status, output, message = self.backup_list()
        if not status:
            self.fail(message)
        self.log.info("Start to merge backup")
        self.backupset.start = randrange(1, self.backupset.number_of_backups)
        if int(self.backupset.number_of_backups) == 2:
            self.backupset.end = 2
        elif int(self.backupset.number_of_backups) > 2:
            self.backupset.end = randrange(self.backupset.start,
                                       self.backupset.number_of_backups + 1)
        self.merged = True
        status, output, _ = self.backup_merge()
        self.backupset.end -= 1
        status, output, message = self.backup_list()
        if not status:
            self.fail(message)
        current_vseqno = self.get_vbucket_seqnos(self.cluster_to_backup, self.buckets,
                                                 self.skip_consistency, self.per_node)
        self.log.info("*** Start to validate data in merge backup ")
        self.validate_backup_data(self.backupset.backup_host, [self.master],
                                  "ent-backup", False, False, "memory",
                                  self.num_items, None)
        self.backup_cluster_validate(skip_backup=True)

    def test_backup_purge(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a backupset
        3. Initiates a backup and kills the erlang server while backup is going on
        4. Waits for the backup command to timeout
        5. Executes backup command again with purge option
        6. Validates the old backup is deleted and new backup is created successfully
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        old_backup_name = ""
        new_backup_name = ""
        backup_result = self.cluster.async_backup_cluster(cluster_host=self.backupset.cluster_host,
                                                          backup_host=self.backupset.backup_host,
                                                          directory=self.backupset.directory, name=self.backupset.name,
                                                          resume=self.backupset.resume, purge=self.backupset.purge,
                                                          no_progress_bar=self.no_progress_bar,
                                                          cli_command_location=self.cli_command_location,
                                                          cb_version=self.cb_version)
        self.sleep(10)
        conn = RemoteMachineShellConnection(self.backupset.cluster_host)
        conn.kill_erlang()
        output = backup_result.result(timeout=200)
        self.log.info(str(output))
        status, output, message = self.backup_list()
        if not status:
            self.fail(message)
        for line in output:
            if re.search("\d{4}-\d{2}-\d{2}T\d{2}_\d{2}_\d{2}.\d+-\d{2}_\d{2}", line):
                old_backup_name = re.search("\d{4}-\d{2}-\d{2}T\d{2}_\d{2}_\d{2}.\d+-\d{2}_\d{2}", line).group()
                self.log.info("Backup name before purge: " + old_backup_name)
        conn.start_couchbase()
        conn.disconnect()
        self.sleep(30)
        output, error = self.backup_cluster()
        if error or not self._check_output("Backup successfully completed", output):
            self.fail("Taking cluster backup failed.")
        status, output, message = self.backup_list()
        if not status:
            self.fail(message)
        for line in output:
            if re.search("\d{4}-\d{2}-\d{2}T\d{2}_\d{2}_\d{2}.\d+-\d{2}_\d{2}", line):
                new_backup_name = re.search("\d{4}-\d{2}-\d{2}T\d{2}_\d{2}_\d{2}.\d+-\d{2}_\d{2}", line).group()
                self.log.info("Backup name after purge: " + new_backup_name)
        self.assertNotEqual(old_backup_name, new_backup_name,
                            "Old backup name and new backup name are same when purge is used")
        self.log.info("Old backup name and new backup name are not same when purge is used")

    def test_backup_resume(self):
        """
        1. Creates specified bucket on the cluster and loads it with given
           number of items
        2. Creates a backupset
        3. Initiates a backup and kills the erlang server while backup is going on
        4. Waits for the backup command to timeout
        5. Executes backup command again with resume option
        6. Validates the old backup is resumes and backup is completed successfully
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size,
                            end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self.bk_with_stop_and_resume()

    def test_backup_restore_with_deletes(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a backupset - backsup data and validates
        3. Perform deletes
        4. Restore data and validate
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self.backup_cluster_validate()
        self._load_all_buckets(self.master, gen, "delete", 0)
        self.backup_restore_validate(compare_uuid=False, seqno_compare_function=">=")

    def test_backup_restore_with_failover(self):
        """
        1. Test should be run with 2 nodes in cluster host (param: nodes_init = 2)
        2. Creates specified bucket on the cluster and loads it with given number of items
        3. Creates a backupset - backsup data and validates
        4. Fails over the second node with specified type (param: graceful = True | False)
        5. Sets recovery type to specified value (param: recoveryType = full | delta)
        6. Adds back the failed over node and rebalances
        7. Restores data and validates
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self.backup_cluster_validate()
        rest = RestConnection(self.backupset.cluster_host)
        nodes_all = rest.node_statuses()
        for node in nodes_all:
            if node.ip == self.servers[1].ip:
                rest.fail_over(otpNode=node.id, graceful=self.graceful)
                self.sleep(30)
                rest.set_recovery_type(otpNode=node.id, recoveryType=self.recoveryType)
                rest.add_back_node(otpNode=node.id)
        rebalance = self.cluster.async_rebalance(self.servers, [], [])
        rebalance.result()
        self.backup_restore_validate(compare_uuid=False, seqno_compare_function=">=")

    def test_backup_restore_after_offline_upgrade(self):
        """
            1. Test has to be supplied initial_version to be installed, create
               default bucket and load data to this bucket.
            2. Backup cluster and verify data and delete default bucket
            3. Upgrades cluster to upgrade_version re-reates default bucket
            4. Restores data and validates
        """
        upgrade_version = self.input.param("upgrade_version", "5.0.0-3330")
        if upgrade_version == "5.0.0-3330":
            self.fail("\n *** Need param 'upgrade_version=' to run")

        self._install(self.servers)
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size,
                            end=self.num_items)
        rebalance = self.cluster.async_rebalance(self.servers[:2], [self.servers[1]],
                                                 [])
        rebalance.result()
        self.add_built_in_server_user()
        RestConnection(self.master).create_bucket(bucket='default', ramQuotaMB=512)
        self.buckets = RestConnection(self.master).get_buckets()
        self.total_buckets = len(self.buckets)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self.backup_cluster_validate()
        self.sleep(5)
        BucketOperationHelper.delete_bucket_or_assert(self.master, "default", self)

        """ Start to upgrade """
        if self.force_version_upgrade:
            upgrade_version = self.force_version_upgrade
        upgrade_threads = self._async_update(upgrade_version=upgrade_version,
                                             servers=self.servers[:2])
        for th in upgrade_threads:
            th.join()
        self.log.info("Upgraded to: {ver}".format(ver=upgrade_version))
        self.sleep(30)

        """ Re-create default bucket on upgrade cluster """
        RestConnection(self.master).create_bucket(bucket='default', ramQuotaMB=512)
        self.sleep(5)

        """ Only server from Spock needs build in user
            to access bucket and other tasks
        """
        if "5" <= RestConnection(self.master).get_nodes_version()[:1]:
            self.add_built_in_server_user()
            for user in self.users_check_restore:
                user_name = user.replace('[', '_').replace(']', '_')
                testuser = [{'id': user_name, 'name': user_name,
                             'password': 'password'}]
                rolelist = [{'id': user_name, 'name': user_name,
                             'roles': user}]

                self.log.info("**** add built-in '%s' user to node %s ****" % (testuser[0]["name"],
                                                                               self.master.ip))
                RbacBase().create_user_source(testuser, 'builtin', self.master)

                self.log.info("**** add '%s' role to '%s' user ****" % (rolelist[0]["roles"],
                                                                        testuser[0]["name"]))
                RbacBase().add_user_role(rolelist, RestConnection(self.master), 'builtin')

        backupsets = [self.backupset]
        if "5" <= RestConnection(self.master).get_nodes_version()[:1]:
            for user in self.users_check_restore:
                new_backupset = copy.deepcopy(self.backupset)
                new_backupset.restore_cluster_host_username = user.replace('[', '_').replace(']', '_')
                backupsets.append(new_backupset)
        for backupset in backupsets:
            self.backupset = backupset
            self.backup_restore_validate(compare_uuid=False, seqno_compare_function=">=")
            BucketOperationHelper().delete_bucket_or_assert(self.backupset.cluster_host,
                                                       "default", self)

    def test_backup_restore_after_online_upgrade(self):
        """
            1. Test has to be supplied initial_version to be installed and
               upgrade_version to be upgraded to
            2. Installs initial_version on the servers
            3. Load data and backup in pre-upgrade
            4. Install upgrade version on 2 nodes.  Use swap rebalance to upgrade
               cluster
            5. Operation after upgrade cluster
            6. Restores data and validates
        """
        servers = copy.deepcopy(self.servers)
        self.vbuckets = self.initial_vbuckets
        if len(servers) != 4:
            self.fail("\nThis test needs exactly 4 nodes to run! ")

        self._install(servers)
        count = 0
        nodes_fail_to_install = []
        for server in servers:
            ready = RestHelper(RestConnection(server)).is_ns_server_running(60)
            if ready:
                count += 1
            else:
                nodes_fail_to_install.append(server.ip)
        if count < len(servers):
            self.fail("Some servers may not install Couchbase server: {0}"\
                                          .format(nodes_fail_to_install))

        if not self.disable_diag_eval_on_non_local_host:
            self.enable_diag_eval_on_non_local_hosts()
        cmd =  'curl -g {0}:8091/diag/eval -u {1}:{2} '.format(self.master.ip,
                                                              self.master.rest_username,
                                                              self.master.rest_password)
        cmd += '-d "path_config:component_path(bin)."'
        bin_path  = subprocess.check_output(cmd, shell=True)
        if "bin" not in bin_path:
            self.fail("Check if cb server install on %s" % self.master.ip)
        else:
            self.cli_command_location = bin_path.replace('"', '') + "/"

        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size,
                            end=self.num_items)
        rebalance = self.cluster.async_rebalance(servers[:self.nodes_init],
                                                 [servers[int(self.nodes_init) - 1]], [])
        rebalance.result()
        self.sleep(15)
        self.add_built_in_server_user()
        rest = RestConnection(self.master)
        cb_version = rest.get_nodes_version()
        initial_compression_mode = "off"
        if 5.5 > float(cb_version[:3]):
            self.compression_mode = initial_compression_mode
        if cb_version[:5] in COUCHBASE_FROM_4DOT6:
            self.cluster_flag = "--cluster"

        rest.create_bucket(bucket='default', ramQuotaMB=512,
                           compressionMode=self.compression_mode)
        self.buckets = rest.get_buckets()
        self._load_all_buckets(self.master, gen, "create", 0)

        """ create index """
        if self.create_gsi:
            if "5" > rest.get_nodes_version()[:1]:
                if self.gsi_type == "forestdb":
                    self.fail("Need to set param self.gsi_type=memory_optimized")
                rest.set_indexer_storage_mode(storageMode="memory_optimized")
            else:
                rest.set_indexer_storage_mode(storageMode="plasma")
            self.create_indexes()
        self.backup_create()
        if self.backupset.number_of_backups > 1:
            self.log.info("Start doing multiple backup")
            for i in range(1, self.backupset.number_of_backups + 1):
                self._backup_restore_with_ops()
        else:
            self.backup_cluster_validate()
        start = randrange(1, self.backupset.number_of_backups + 1)
        if start == self.backupset.number_of_backups:
            end = start
        else:
            end = randrange(start, self.backupset.number_of_backups + 1)
        self.sleep(5)
        self.backup_list()

        """ Start to online upgrade using swap rebalance """
        self.initial_version = self.upgrade_versions[0]
        if self.force_version_upgrade:
            self.initial_version = self.force_version_upgrade
        self.sleep(self.sleep_time,
                   "Pre-setup of old version is done. Wait for online upgrade to: "
                   "{0} version".format(self.initial_version))
        self.product = 'couchbase-server'
        self._install(servers[2:])
        self.sleep(self.sleep_time,
                   "Installation of new version is done. Wait for rebalance")
        self.log.info(
            "Rebalanced in upgraded nodes and rebalanced out nodes with old version")
        add_node_services = [self.add_node_services]
        if "-" in self.add_node_services:
            add_node_services = self.add_node_services.split("-")

        self.cluster.rebalance(servers, servers[2:], servers[:2],
                               services=add_node_services)
        self.sleep(15)
        self.backupset.cluster_host = servers[2]
        """ Upgrade is done """
        self.log.info("** Upgrade is done **")
        healthy = False
        timeout = 0
        while not healthy:
            healthy = RestHelper(RestConnection(self.backupset.cluster_host)).is_cluster_healthy()
            if not healthy:
                if timeout == 120:
                    self.fail("Node %s is not ready after 2 mins" % self.backupset.cluster_host)
                else:
                    self.sleep(5, "Wait for server up ")
                    timeout += 5
            else:
                healthy = True
        if "5" <= RestConnection(servers[2]).get_nodes_version()[:1]:
            for user in self.users_check_restore:
                user_name = user.replace('[', '_').replace(']', '_')
                testuser = [{'id': user_name, 'name': user_name,
                             'password': 'password'}]
                rolelist = [{'id': user_name, 'name': user_name,
                             'roles': user}]

                self.log.info("**** add built-in '%s' user to node %s ****" % (testuser[0]["name"],
                                                                               servers[2].ip))
                RbacBase().create_user_source(testuser, 'builtin', servers[2])
                
                self.log.info("**** add '%s' role to '%s' user ****" % (rolelist[0]["roles"],
                                                                        testuser[0]["name"]))
                status = RbacBase().add_user_role(rolelist, RestConnection(servers[2]), 'builtin')
                self.log.info(status)
        if self.backupset.number_of_backups_after_upgrade:
            self.backupset.number_of_backups += \
                self.backupset.number_of_backups_after_upgrade
            if "5" <= RestConnection(servers[2]).get_nodes_version()[:1]:
                self.add_built_in_server_user(node=servers[2])
            for i in range(1, self.backupset.number_of_backups_after_upgrade + 2):
                self.log.info("_backup_restore_with_ops #{0} started...".format(i))
                validate_dir_struct = True
                if i > 2:
                    validate_dir_struct = False
                self._backup_restore_with_ops(node=self.backupset.cluster_host, repeats=1,
                                              validate_directory_structure=validate_dir_struct)
            self.backup_list()

        """ merged after upgrade """
        if self.after_upgrade_merged:
            self.backupset.start = 1
            self.backupset.end = len(self.backups)
            self.backup_merge_validate()
            self.backup_list()

        backupsets = [self.backupset]
        if "5" <= RestConnection(servers[2]).get_nodes_version()[:1]:
            for user in self.users_check_restore:
                new_backupset = copy.deepcopy(self.backupset)
                new_backupset.restore_cluster_host_username = user.replace('[', '_').replace(']', '_')
                backupsets.append(new_backupset)
        for backupset in backupsets:
            self.backupset = backupset
            if self.bucket_flush:
                self.log.info("Start to flush bucket")
                rest = RestConnection(servers[2])
                rest.flush_bucket()
            else:
                self.bucket_helper.delete_bucket_or_assert(self.backupset.cluster_host,
                                                           "default", self)
                """ Re-create default bucket on upgrade cluster """
                RestConnection(servers[2]).create_bucket(bucket='default',
                                                         ramQuotaMB=512,
                                                         compressionMode=self.compression_mode)
            self.sleep(5)
            self.total_buckets = len(self.buckets)

            if self.after_upgrade_merged:
                self.backupset.end = 1

            """ restore back to cluster """
            self.backup_restore_validate(compare_uuid=False, seqno_compare_function=">=")
            if self.create_gsi:
                self.verify_gsi()

    def test_backup_restore_with_python_sdk(self):
        """
        1. Note that python sdk has to be installed on all nodes before running this test
        2. Connects to default bucket on cluster host using Python SDK
           - loads specifed number of items
        3. Creates a backupset, backsup data and validates
        4. Restores data and validates
        5. Connects to default bucket on restore host using Python SDK
        6. Retrieves cas and flgas of each doc on both cluster and restore host
           - validates if they are equal
        """
        testuser = [{'id': 'default', 'name': 'default', 'password': 'password'}]
        rolelist = [{'id': 'default', 'name': 'default', 'roles': 'admin'}]
        self.add_built_in_server_user(testuser, rolelist)
        try:
            cb = Bucket('couchbase://' + self.backupset.cluster_host.ip + '/default',
                                                                 password="password")
            if cb is not None:
                self.log.info("Established connection to bucket on cluster host"
                              " using python SDK")
            else:
                self.fail("Failed to establish connection to bucket on cluster host"
                          " using python SDK")
        except Exception as ex:
            self.fail(str(ex))
        self.log.info("Loading bucket with data using python SDK")
        for i in range(1, self.num_items + 1):
            cb.upsert("doc" + str(i), "value" + str(i))
        cluster_host_data = {}
        for i in range(1, self.num_items + 1):
            key = "doc" + str(i)
            value_obj = cb.get(key=key)
            cluster_host_data[key] = {}
            cluster_host_data[key]["cas"] = str(value_obj.cas)
            cluster_host_data[key]["flags"] = str(value_obj.flags)
        self.backup_create()
        self.backup_cluster_validate()
        self.backup_restore_validate(compare_uuid=False, seqno_compare_function=">=")

        self.add_built_in_server_user(testuser, rolelist, self.backupset.restore_cluster_host)
        try:
            cb = Bucket('couchbase://' + self.backupset.restore_cluster_host.ip + '/default',
                                                                         password="password")
            if cb is not None:
                self.log.info("Established connection to bucket on restore host " \
                              "using python SDK")
            else:
                self.fail("Failed to establish connection to bucket on restore " \
                          "host using python SDK")
        except Exception as ex:
            self.fail(str(ex))
        restore_host_data = {}
        for i in range(1, self.num_items + 1):
            key = "doc" + str(i)
            value_obj = cb.get(key=key)
            restore_host_data[key] = {}
            restore_host_data[key]["cas"] = str(value_obj.cas)
            restore_host_data[key]["flags"] = str(value_obj.flags)
        self.log.info("Comparing cluster host data cas and flags against restore host data")
        for i in range(1, self.num_items + 1):
            key = "doc" + str(i)
            if cluster_host_data[key]["cas"] != restore_host_data[key]["cas"]:
                self.fail("CAS mismatch for key: {0}".format(key))
            if cluster_host_data[key]["flags"] != restore_host_data[key]["flags"]:
                self.fail("Flags mismatch for key: {0}".format(key))
        self.log.info("Successfully validated cluster host data cas and flags " \
                      "against restore host data")

    def test_backup_restore_with_flush(self):
        """
        1. Test should be run with same-cluster=True
        2. Creates specified bucket on the cluster and loads it with given number of items
        3. Creates a backupset - backsup data and validates
        4. Flushes the bucket
        5. Restores data and validates
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self.backup_cluster_validate()
        rest = RestConnection(self.backupset.cluster_host)
        rest.flush_bucket()
        self.log.info("Flushed default bucket - restoring data now..")
        self.backup_restore_validate(compare_uuid=False, seqno_compare_function=">=")

    def test_backup_restore_with_recreate(self):
        """
        1. Test should be run with same-cluster=True
        2. Creates specified bucket on the cluster and loads it with given number of items
        3. Creates a backupset - backsup data and validates
        4. Deletes the bucket and recreates it
        5. Restores data and validates
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self.backup_cluster_validate()
        rest = RestConnection(self.backupset.cluster_host)
        rest.delete_bucket()
        bucket_name = "default"
        rest_helper = RestHelper(rest)
        rest.create_bucket(bucket=bucket_name, ramQuotaMB=512)
        bucket_ready = rest_helper.vbucket_map_ready(bucket_name)
        if not bucket_ready:
            self.fail("Bucket {0} is not created after 120 seconds.".format(bucket_name))
        self.log.info("Deleted {0} bucket and recreated it - restoring it now.."\
                                                                .format(bucket_name))
        self.backup_restore_validate(compare_uuid=False, seqno_compare_function=">=")

    def test_backup_create_negative_args(self):
        """
        Validates error messages for negative inputs of create command
        """
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        cmd = "config"
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        # ['cbbackupmgr config [<args>]', '', 'Required Flags:', '', '  -a,--archive                The archive directory to use', '  -r,--repo                   The name of the backup repository to create and', '                              configure', '', 'Optional Flags:', '', '     --exclude-buckets        A comma separated list of buckets to exclude from', '                              backups. All buckets except for the ones specified', '                              will be backed up.', '     --include-buckets        A comma separated list of buckets to back up. Only', '                              buckets in this list are backed up.', '     --disable-bucket-config  Disables backing up bucket configuration', '                              information', '     --disable-views          Disables backing up view definitions', '     --disable-gsi-indexes    Disables backing up GSI index definitions', '     --disable-ft-indexes     Disables backing up Full Text index definitions', '     --disable-data           Disables backing up cluster data', '  -h,--help                   Prints the help message', '']
        self.assertEqual(output[0], "cbbackupmgr config [<args>]", "Expected error message not thrown")
        cmd = "config --archive"
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], "Expected argument for option: --archive", "Expected error message not thrown")
        cmd = "config --archive {0}".format(self.backupset.directory)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], "Flag required, but not specified: -r/--repo", "Expected error message not thrown")
        cmd = "config --archive {0} --repo".format(self.backupset.directory)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], "Expected argument for option: --repo", "Expected error message not thrown")
        self.backup_create()
        cmd = "config --archive {0} --repo {1}".format(self.backupset.directory, self.backupset.name)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        remote_client.disconnect()
        self.assertEqual(output[0], "Backup repository creation failed: Backup Repository `backup` exists",
                         "Expected error message not thrown")

    def test_backup_cluster_restore_negative_args(self):
        """
        Validates error messages for negative inputs of cluster or restore command - command parameter
        decides which command to test
        """
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        self.backup_create()
        cmd_to_test = self.input.param("command", "backup")
        if cmd_to_test == "restore":
            cmd = cmd_to_test + " --archive {0} --repo {1} --host http://{2}:{3} --username {4} \
                                  --password {5}".format(self.backupset.directory,
                                                         self.backupset.name,
                                                         self.backupset.cluster_host.ip,
                                                         self.backupset.cluster_host.port,
                                                         self.backupset.cluster_host_username,
                                                         self.backupset.cluster_host_password)
            command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
            output, error = remote_client.execute_command(command)
            remote_client.log_command_output(output, error)
            self.assertTrue("Error restoring cluster: Backup backup doesn't contain any backups" in output[-1],
                            "Expected error message not thrown")
            self.backup_cluster()
        cmd = cmd_to_test
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        cmd_test = cmd_to_test
        if cmd_to_test.startswith('"') and cmd_to_test.endswith('"'):
            cmd_test = cmd_to_test[1:-1]
        self.assertEqual(output[0], "cbbackupmgr {} [<args>]".format(cmd_test))
        cmd = cmd_to_test + " --archive"
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], "Expected argument for option: --archive", "Expected error message not thrown")
        cmd = cmd_to_test + " --archive abc -c http://localhost:8091 -u Administrator -p password -r aa"
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], "Error: Archive directory `abc` doesn't exist", "Expected error message not thrown")
        cmd = cmd_to_test + " --archive {0} -c http://localhost:8091 -u Administrator -p password".format(
            self.backupset.directory)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], "Flag required, but not specified: -r/--repo", "Expected error message not thrown")
        cmd = cmd_to_test + " --archive {0} --repo".format(self.backupset.directory)
        command = "{0}/cbbackupmgr {1} -c http://localhost:8091 -u Administrator -p password -r".format(
            self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], "Expected argument for option: --repo", "Expected error message not thrown")
        cmd = cmd_to_test + " --archive {0} --repo {1} -u Administrator -p password".format(self.backupset.directory,
                                                                                            self.backupset.name)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], "Flag required, but not specified: -c/--cluster",
                         "Expected error message not thrown")
        cmd = cmd_to_test + " --archive {0} --repo {1} -c  -u Administrator -p password -r repo".format(
            self.backupset.directory, self.backupset.name)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], "Expected argument for option: -c", "Expected error message not thrown")
        cmd = cmd_to_test + " --archive {0} --repo {1} -c http://{2}:{3}".format(self.backupset.directory,
                                                                                 self.backupset.name,
                                                                                 self.backupset.cluster_host.ip,
                                                                                 self.backupset.cluster_host.port)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], "Flag required, but not specified: -u/--username",
                         "Expected error message not thrown")
        cmd = cmd_to_test + " --archive {0} --repo {1} --cluster http://{2}:{3} \
                              --username".format(self.backupset.directory,
                                                 self.backupset.name,
                                                 self.backupset.cluster_host.ip,
                                                 self.backupset.cluster_host.port)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], "Expected argument for option: --username", "Expected error message not thrown")
        cmd = cmd_to_test + " --archive {0} --repo {1} --cluster http://{2}:{3} \
                              --username {4}".format(self.backupset.directory,
                                                     self.backupset.name,
                                                     self.backupset.cluster_host.ip,
                                                     self.backupset.cluster_host.port,
                                                     self.backupset.cluster_host_username)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], "Flag required, but not specified: -p/--password",
                         "Expected error message not thrown")
        cmd = cmd_to_test + " --archive {0} --repo abc --cluster http://{1}:{2} --username {3} \
                              --password {4}".format(self.backupset.directory,
                                                     self.backupset.cluster_host.ip,
                                                     self.backupset.cluster_host.port,
                                                     self.backupset.cluster_host_username,
                                                     self.backupset.cluster_host_password)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        part_message = "backing up"
        if cmd_to_test.startswith('"') and cmd_to_test.endswith('"'):
            cmd_test = cmd_to_test[1:-1]
        if cmd_test == "restore":
            part_message = 'restoring'
        self.assertTrue("Error {0} cluster: Backup Repository `abc` not found"\
                        .format(part_message) in output[-1],
                        "Expected error message not thrown. Actual output %s " % output[-1])
        cmd = cmd_to_test + " --archive {0} --repo {1} --cluster abc --username {2} \
                              --password {3}".format(self.backupset.directory,
                                                     self.backupset.name,
                                                     self.backupset.cluster_host_username,
                                                     self.backupset.cluster_host_password)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertTrue("Error {0} cluster: dial tcp:".format(part_message) in output[-1],
                        "Expected error message not thrown")
        cmd = cmd_to_test + " --archive {0} --repo {1} --cluster http://{2}:{3} --username abc \
                              --password {4}".format(self.backupset.directory,
                                                     self.backupset.name,
                                                     self.backupset.cluster_host.ip,
                                                     self.backupset.cluster_host.port,
                                                     self.backupset.cluster_host_password)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertTrue("check username and password" in output[-1], "Expected error message not thrown")
        cmd = cmd_to_test + " --archive {0} --repo {1} --cluster http://{2}:{3} --username {4} \
                              --password abc".format(self.backupset.directory,
                                                     self.backupset.name,
                                                     self.backupset.cluster_host.ip,
                                                     self.backupset.cluster_host.port,
                                                     self.backupset.cluster_host_username)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        remote_client.disconnect()
        self.assertTrue("check username and password" in output[-1], "Expected error message not thrown")

    def test_backup_list_negative_args(self):
        """
        Validates error messages for negative inputs of list command
        """
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        self.backup_create()
        cmd = "list"
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], "cbbackupmgr list [<args>]", "Expected error message not thrown")
        cmd = "list --archive"
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], "Expected argument for option: --archive", "Expected error message not thrown")
        cmd = "list --archive abc".format(self.backupset.directory)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        remote_client.disconnect()
        self.assertTrue("Error: Archive directory `abc` doesn't exist" in output[-1],
                        "Expected error message not thrown")

    def test_backup_compact_negative_args(self):
        """
        Validates error messages for negative inputs of compact command
        """
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        self.backup_create()
        self.backup_cluster()
        cmd = "compact"
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], "cbbackupmgr compact [<args>]",
                         "Expected error message not thrown")
        cmd = "compact --archive"
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], "Expected argument for option: --archive",
                         "Expected error message not thrown")
        cmd = "compact --archive {0}".format(self.backupset.directory)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], "Flag required, but not specified: -r/--repo",
                         "Expected error message not thrown")
        cmd = "compact --archive {0} --repo".format(self.backupset.directory)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], "Expected argument for option: --repo",
                         "Expected error message not thrown")
        cmd = "compact --archive {0} --repo {1}".format(self.backupset.directory,
                                                        self.backupset.name)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], "Flag required, but not specified: -/--backup",
                         "Expected error message not thrown")
        cmd = "compact --archive {0} --repo {1} --backup" \
            .format(self.backupset.directory, self.backupset.name)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], "Expected argument for option: --backup",
                         "Expected error message not thrown")
        cmd = "compact --archive abc --repo {0} --backup {1}" \
            .format(self.backupset.name, self.backups[0])
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertTrue("Error opening archive at abc due to `Not an archive directory" \
                        in output[-1],
                        "Expected error message not thrown")
        cmd = "compact --archive {0} --repo abc --backup {1}" \
            .format(self.backupset.directory, self.backups[0])
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertTrue("Backup Repository `abc` not found" in output[-1],
                        "Expected error message not thrown")
        cmd = "compact --archive {0} --repo {1} --backup abc".format(self.backupset.directory,
                                                                     self.backupset.name)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        remote_client.disconnect()
        self.assertTrue("Compacting incr backup `abc` of backup `backup` failed:" in output[-1],
                        "Expected error message not thrown")

    def test_backup_merge_negative_args(self):
        """
        Validates error messages for negative inputs of merge command
        """
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        self.backup_create()
        cmd = "merge"
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], "cbbackupmgr merge [<args>]", "Expected error message not thrown")
        cmd = "merge --archive -c http://localhost:8091 -u Administrator -p password -r aa"
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], "Expected argument for option: --archive", "Expected error message not thrown")
        cmd = "merge --archive {0}".format(self.backupset.directory)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], "Flag required, but not specified: -r/--repo", "Expected error message not thrown")
        cmd = "merge --archive {0} --repo".format(self.backupset.directory)
        command = "{0}/cbbackupmgr {1} -r".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], "Expected argument for option: --repo", "Expected error message not thrown")
        cmd = "merge --archive {0} --repo {1}".format(self.backupset.directory, self.backupset.name)
        command = "{0}/cbbackupmgr {1} --start start --end end".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], "Error merging data: Backup backup doesn't contain any backups",
                         "Expected error message not thrown")
        self._take_n_backups(n=2)
        cmd = "merge --archive {0} --repo {1}".format(self.backupset.directory, self.backupset.name)
        command = "{0}/cbbackupmgr {1} --start bbb --end end".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], "Error merging data: Error restoring data, `bbb` is invalid start point",
                         "Expected error message not thrown")
        cmd = "merge --archive {0} --repo {1} --start".format(self.backupset.directory, self.backupset.name)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], "Expected argument for option: --start", "Expected error message not thrown")
        cmd = "merge --archive {0} --repo {1} --start {2}".format(self.backupset.directory,
                                                                  self.backupset.name, self.backups[0])
        command = "{0}/cbbackupmgr {1} --end aa".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], "Error merging data: Error restoring data, `aa` is invalid end point",
                         "Expected error message not thrown")
        cmd = "merge --archive {0} --repo {1} --start {2} --end".format(self.backupset.directory,
                                                                        self.backupset.name, self.backups[0])
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], "Expected argument for option: --end", "Expected error message not thrown")
        cmd = "merge --archive abc --repo {0} --start {1} --end {2}".format(self.backupset.name,
                                                                            self.backups[0], self.backups[1])
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertTrue("Error: Archive directory `abc` doesn't exist" in output[-1],
                        "Expected error message not thrown")
        cmd = "merge --archive {0} --repo abc --start {1} --end {2}".format(self.backupset.directory,
                                                                            self.backups[0], self.backups[1])
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertTrue("Error merging data: Backup Repository `abc` not found" in output[-1],
                        "Expected error message not thrown")
        cmd = "merge --archive {0} --repo {1} --start abc --end {2}".format(self.backupset.directory,
                                                                            self.backupset.name, self.backups[1])
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertTrue("Error merging data: Error restoring data, `abc` is invalid start point" in output[-1],
                        "Expected error message not thrown")
        cmd = "merge --archive {0} --repo {1} --start {2} --end abc".format(self.backupset.directory,
                                                                            self.backupset.name, self.backups[0])
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertTrue("Error merging data: Error restoring data, `abc` is invalid end point" in output[-1],
                        "Expected error message not thrown")
        cmd = "merge --archive {0} --repo {1} --start {2} --end {3}".format(self.backupset.directory,
                                                                            self.backupset.name,
                                                                            self.backups[1], self.backups[0])
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        remote_client.disconnect()
        self.assertTrue("Error merging data: start point `{0}` is after end point `{1}`".format
                        (self.backups[1], self.backups[0]) in output[-1],
                        "Expected error message not thrown")

    def test_backup_remove_negative_args(self):
        """
        Validates error messages for negative inputs of remove command
        """
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        self.backup_create()
        self.backup_cluster()
        cmd = "remove"
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], "cbbackupmgr remove [<args>]", "Expected error message not thrown")
        cmd = "remove --archive -c http://localhost:8091 -u Administrator -p password -r aa"
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], "Expected argument for option: --archive", "Expected error message not thrown")
        cmd = "remove --archive {0}".format(self.backupset.directory)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], "Flag required, but not specified: -r/--repo", "Expected error message not thrown")
        cmd = "remove --archive {0} --repo".format(self.backupset.directory)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], "Expected argument for option: --repo", "Expected error message not thrown")
        cmd = "remove --archive abc --repo {0}".format(self.backupset.name)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertTrue("Error: Archive directory `abc` doesn't exist" in output[-1],
                        "Expected error message not thrown")
        cmd = "remove --archive {0} --repo abc".format(self.backupset.directory)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        remote_client.disconnect()
        self.assertTrue("Backup Repository `abc` not found" in output[-1],
                        "Expected error message not thrown")

    def test_backup_restore_with_views(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a backupset
        3. Creates a simple view on source cluster
        4. Backsup data and validates
        5. Restores data ans validates
        6. Ensures that same view is created in restore cluster
        """
        if "ephemeral" in self.input.param("bucket_type", 'membase'):
            self.log.info("\n****** view does not support on ephemeral bucket ******")
            return
        rest_src = RestConnection(self.backupset.cluster_host)
        rest_src.add_node(self.servers[1].rest_username, self.servers[1].rest_password,
                          self.servers[1].ip, services=['index', 'kv'])
        rebalance = self.cluster.async_rebalance(self.cluster_to_backup, [], [])
        rebalance.result()
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        default_map_func = "function (doc) {\n  emit(doc._id, doc);\n}"
        default_view_name = "test"
        default_ddoc_name = "ddoc_test"
        prefix = "dev_"
        query = {"full_set": "true", "stale": "false", "connection_timeout": 60000}
        view = View(default_view_name, default_map_func)
        task = self.cluster.async_create_view(self.backupset.cluster_host,
                                              default_ddoc_name, view, "default")
        task.result()
        self.backup_cluster_validate()
        rest_target = RestConnection(self.backupset.restore_cluster_host)
        rest_target.add_node(self.input.clusters[0][1].rest_username,
                             self.input.clusters[0][1].rest_password,
                             self.input.clusters[0][1].ip, services=['kv', 'index'])
        rebalance = self.cluster.async_rebalance(self.cluster_to_restore, [], [])
        rebalance.result()
        self.backup_restore_validate(compare_uuid=False, seqno_compare_function=">=")
        try:
            result = self.cluster.query_view(self.backupset.restore_cluster_host,
                                             prefix + default_ddoc_name,
                                             default_view_name, query, timeout=30)
            self.assertEqual(len(result['rows']), self.num_items,
                             "Querying view on restore cluster did not return expected number of items")
            self.log.info("Querying view on restore cluster returned expected number of items")
        except TimeoutError:
            self.fail("View could not be queried in restore cluster within timeout")

    def test_backup_restore_with_gsi(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a backupset
        3. Creates a GSI index on source cluster
        4. Backsup data and validates
        5. Restores data ans validates
        6. Ensures that same gsi index is created in restore cluster
        """
        rest_src = RestConnection(self.backupset.cluster_host)
        self.cluster_storage_mode = \
                     rest_src.get_index_settings()["indexer.settings.storage_mode"]
        self.log.info("index storage mode: {0}".format(self.cluster_storage_mode))
        rest_src.add_node(self.servers[1].rest_username, self.servers[1].rest_password,
                          self.servers[1].ip, services=['kv', 'index'])
        rebalance = self.cluster.async_rebalance(self.cluster_to_backup, [], [])
        rebalance.result()
        self.test_storage_mode = self.cluster_storage_mode
        if "ephemeral" in self.bucket_type:
            self.log.info("ephemeral bucket needs to set backup cluster to memopt for gsi.")
            self.test_storage_mode = "memory_optimized"
            self._reset_storage_mode(rest_src, self.test_storage_mode)
            rest_src.add_node(self.servers[1].rest_username, self.servers[1].rest_password,
                          self.servers[1].ip, services=['kv', 'index'])
            rebalance = self.cluster.async_rebalance(self.cluster_to_backup, [], [])
            rebalance.result()
            rest_src.create_bucket(bucket='default', ramQuotaMB=int(self.quota) - 1,
                                   bucketType=self.bucket_type,
                                   evictionPolicy="noEviction")
            self.add_built_in_server_user(node=self.backupset.cluster_host)

        gen = DocumentGenerator('test_docs', '{{"age": {0}}}', range(100),
                                start=0, end=self.num_items)
        self.buckets = rest_src.get_buckets()
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()

        cmd = "cbindex -type create -bucket default -using %s -index age -fields=age " \
              " -auth %s:%s" % (self.test_storage_mode,
                                self.master.rest_username,
                                self.master.rest_password)
        shell = RemoteMachineShellConnection(self.backupset.cluster_host)
        command = "{0}/{1}".format(self.cli_command_location, cmd)
        output, error = shell.execute_command(command)
        shell.log_command_output(output, error)
        shell.disconnect()
        if error or "Index created" not in output[-1]:
            self.fail("GSI index cannot be created")
        self.backup_cluster_validate()

        rest_target = RestConnection(self.backupset.restore_cluster_host)
        rest_target.add_node(self.input.clusters[0][1].rest_username,
                             self.input.clusters[0][1].rest_password,
                             self.input.clusters[0][1].ip, services=['kv', 'index'])
        rebalance = self.cluster.async_rebalance(self.cluster_to_restore, [], [])
        rebalance.result()
        self.backup_restore_validate(compare_uuid=False, seqno_compare_function=">=")

        cmd = "cbindex -type list -auth %s:%s" % (self.master.rest_username,
                                                  self.master.rest_password)
        shell = RemoteMachineShellConnection(self.backupset.restore_cluster_host)
        command = "{0}/{1}".format(self.cli_command_location, cmd)
        output, error = shell.execute_command(command)
        shell.log_command_output(output, error)
        shell.disconnect()

        try:
            if len(output) > 1:
                self.assertTrue("Index:default/age" in output[1],
                                "GSI index not created in restore cluster as expected")
                self.log.info("GSI index created in restore cluster as expected")
            else:
                self.fail("GSI index not created in restore cluster as expected")
        finally:
            if "ephemeral" in self.bucket_type:
                self.log.info("reset storage mode back to original")
                shell = RemoteMachineShellConnection(self.backupset.cluster_host)
                shell.enable_diag_eval_on_non_local_hosts()
                shell.disconnect()
                shell = RemoteMachineShellConnection(self.backupset.restore_cluster_host)
                shell.enable_diag_eval_on_non_local_hosts()
                shell.disconnect()
                self._reset_storage_mode(rest_src, self.cluster_storage_mode)
                self._reset_storage_mode(rest_target, self.cluster_storage_mode)


    def test_backup_merge_restore_with_gsi(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a backupset
        3. Creates a GSI index on source cluster
        4. Backsup data and validates
        5. Restores data ans validates
        6. Ensures that same gsi index is created in restore cluster
        """
        rest_src = RestConnection(self.backupset.cluster_host)
        rest_src.add_node(self.servers[1].rest_username,
                          self.servers[1].rest_password,
                          self.servers[1].ip, services=['index'])
        rebalance = self.cluster.async_rebalance(self.cluster_to_backup, [],
                                                 [])
        rebalance.result()
        gen = DocumentGenerator('test_docs', '{{"Num1": {0}, "Num2": {1}}}',
                                range(100), range(100),
                                start=0, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        cmd = "cbindex -type create -bucket default -using forestdb -index " \
              "num1 -fields=Num1"
        remote_client = RemoteMachineShellConnection(
            self.backupset.cluster_host)
        command = "{0}/{1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        remote_client.disconnect()
        if error or "Index created" not in output[-1]:
            self.fail("GSI index cannot be created")
        self.backup_cluster_validate()
        cmd = "cbindex -type create -bucket default -using forestdb -index " \
              "num2 -fields=Num2"
        remote_client = RemoteMachineShellConnection(
            self.backupset.cluster_host)
        command = "{0}/{1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        remote_client.disconnect()
        if error or "Index created" not in output[-1]:
            self.fail("GSI index cannot be created")
        self.backup_cluster_validate()
        self.backupset.start = 1
        self.backupset.end = len(self.backups)
        self.backup_merge_validate()
        rest_target = RestConnection(self.backupset.restore_cluster_host)
        rest_target.add_node(self.input.clusters[0][1].rest_username,
                             self.input.clusters[0][1].rest_password,
                             self.input.clusters[0][1].ip, services=['index'])
        rebalance = self.cluster.async_rebalance(self.cluster_to_restore, [],
                                                 [])
        rebalance.result()
        start = self.number_of_backups_taken
        end = self.number_of_backups_taken
        self.backupset.start = start
        self.backupset.end = end
        self.backup_restore_validate(compare_uuid=False,
                                     seqno_compare_function=">=")
        cmd = "cbindex -type list"
        remote_client = RemoteMachineShellConnection(
            self.backupset.restore_cluster_host)
        command = "{0}/{1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        remote_client.disconnect()
        if len(output) > 1:
            self.assertTrue("Index:default/Num1" in output[1],
                            "GSI index not created in restore cluster as expected")
            self.log.info("GSI index created in restore cluster as expected")
        else:
            self.fail("GSI index not created in restore cluster as expected")

    def test_backup_restore_with_fts(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Creates a backupset
        3. Creates a simple FTS index on source cluster
        4. Backsup data and validates
        5. Restores data ans validates
        6. Ensures that same FTS index is created in restore cluster
        """
        rest_src = RestConnection(self.backupset.cluster_host)
        rest_src.add_node(self.servers[1].rest_username, self.servers[1].rest_password,
                          self.servers[1].ip, services=['kv', 'fts'])
        rebalance = self.cluster.async_rebalance(self.cluster_to_backup, [], [])
        rebalance.result()
        gen = DocumentGenerator('test_docs', '{{"age": {0}}}', range(100), start=0,
                                end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()

        rest_src_fts = RestConnection(self.servers[1])
        try:
            from pytests.fts.fts_callable import FTSCallable
            fts_obj = FTSCallable(nodes=self.servers, es_validate=False)
            index = fts_obj.create_default_index(
                index_name="index_default",
                bucket_name="default")
            fts_obj.wait_for_indexing_complete()
            alias = fts_obj.create_alias(target_indexes=[index])
        except Exception as ex:
            self.fail(ex)
        self.backup_cluster_validate()
        rest_target = RestConnection(self.backupset.restore_cluster_host)
        rest_target.add_node(self.input.clusters[0][1].rest_username,
                             self.input.clusters[0][1].rest_password,
                             self.input.clusters[0][1].ip, services=['kv', 'fts'])
        rebalance = self.cluster.async_rebalance(self.cluster_to_restore, [], [])
        rebalance.result()
        self.backup_restore_validate(compare_uuid=False, seqno_compare_function=">=")
        rest_target_fts = RestConnection(self.input.clusters[0][1])
        status = False
        try:
            status, content = rest_target_fts.get_fts_index_definition(index.name)
            self.assertTrue(status and content['status'] == 'ok',
                            "FTS index not found in restore cluster as expected")
            self.log.info("FTS index found in restore cluster as expected")
            status, content = rest_target_fts.get_fts_index_definition(alias.name)
            self.assertTrue(status and content['status'] == 'ok',
                            "FTS alias not found in restore cluster as expected")
            self.log.info("FTS alias found in restore cluster as expected")
        finally:
            rest_src_fts.delete_fts_index(index.name)
            rest_src_fts.delete_fts_index(alias.name)
            if status:
                rest_target_fts.delete_fts_index(index.name)
                rest_target_fts.delete_fts_index(alias.name)

    def test_backup_restore_with_xdcr(self):
        """
        1. Creates a XDCR replication between first two servers
        2. Creates specified bucket on the cluster and loads it with given number of items
        3. Backsup data and validates while replication is going on
        4. Restores data and validates while replication is going on
        """
        rest_src = RestConnection(self.backupset.cluster_host)
        rest_dest = RestConnection(self.servers[1])

        try:
            rest_src.remove_all_replications()
            rest_src.remove_all_remote_clusters()
            rest_src.add_remote_cluster(self.servers[1].ip, self.servers[1].port, self.backupset.cluster_host_username,
                                        self.backupset.cluster_host_password, "C2")
            rest_dest.create_bucket(bucket='default', ramQuotaMB=512)
            self.sleep(10)
            repl_id = rest_src.start_replication('continuous', 'default', "C2")
            if repl_id is not None:
                self.log.info("Replication created successfully")
            gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
            tasks = self._async_load_all_buckets(self.master, gen, "create", 0)
            self.sleep(10)
            self.backup_create()
            self.backup_cluster_validate()
            self.backup_restore_validate(compare_uuid=False, seqno_compare_function=">=")
            for task in tasks:
                task.result()
        finally:
            rest_dest.delete_bucket()

    def test_backup_restore_with_warmup(self):
        """
        1. Creates specified bucket on the cluster and loads it with given number of items
        2. Warmsup the cluster host
        2. Backsup data and validates while warmup is on
        3. Restores data and validates while warmup is on
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        NodeHelper.do_a_warm_up(self.backupset.cluster_host)
        self.sleep(30)
        self.backup_cluster_validate()
        self.backup_restore_validate(compare_uuid=False, seqno_compare_function=">=")
        """ only membase bucket has warmup state """
        if self.bucket_type == "membase":
            NodeHelper.wait_warmup_completed([self.backupset.cluster_host])

    def stat(self, key):
        stats = StatsCommon.get_stats([self.master], 'default', "", key)
        val = list(stats.values())[0]
        if val.isdigit():
            val = int(val)
        return val

    def load_to_dgm(self, active=75, ttl=0):
        """
        decides how many items to load to enter active% dgm state
        where active is an integer value between 0 and 100
        """
        doc_size = 1024
        curr_active = self.stat('vb_active_perc_mem_resident')

        # go into heavy dgm
        while curr_active > active:
            curr_items = self.stat('curr_items')
            gen_create = BlobGenerator('dgmkv', 'dgmkv-', doc_size, start=curr_items + 1, end=curr_items + 50000)
            try:
                self._load_all_buckets(self.master, gen_create, "create", ttl)
            except:
                pass
            curr_active = self.stat('vb_active_perc_mem_resident')

    def test_backup_restore_with_dgm(self):
        """
        1. Creates specified bucket on the cluster and loads it until dgm
        2. Creates a backup set
        3. Backsup data and validates
        4. Restores data and validates
        """
        self.load_to_dgm()
        self.backup_create()
        self.backup_cluster_validate()
        self.backup_restore_validate(compare_uuid=False, seqno_compare_function=">=")

    def test_backup_restore_with_auto_compaction(self):
        """
        1. Creates specified bucket on the cluster and loads it
        2. Updates auto compaction settings
        3. Validates backup and restore
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        rest = RestConnection(self.backupset.cluster_host)
        rest.set_auto_compaction(dbFragmentThresholdPercentage=80,
                                 dbFragmentThreshold=100,
                                 viewFragmntThresholdPercentage=80,
                                 viewFragmntThreshold=100,
                                 bucket="default")
        self.backup_create()
        self.backup_cluster_validate()
        self.backup_restore_validate(compare_uuid=False, seqno_compare_function=">=")

    def test_backup_restore_with_update_notifications(self):
        """
        1. Creates specified bucket on the cluster and loads it
        2. Updates notification settings
        3. Validates backup and restore
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        rest = RestConnection(self.backupset.cluster_host)
        rest.update_notifications("true")
        self.backup_create()
        self.backup_cluster_validate()
        self.backup_restore_validate(compare_uuid=False, seqno_compare_function=">=")

    def test_backup_restore_with_alerts(self):
        """
        1. Creates specified bucket on the cluster and loads it
        2. Updates alerts settings
        3. Validates backup and restore
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        rest = RestConnection(self.backupset.cluster_host)
        rest.set_alerts_settings('couchbase@localhost', 'root@localhost', 'user', 'pwd')
        self.backup_create()
        self.backup_cluster_validate()
        self.backup_restore_validate(compare_uuid=False, seqno_compare_function=">=")

    def test_merge_with_crash(self):
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self._take_n_backups(n=5)
        try:
            merge_result = self.cluster.async_merge_cluster(backup_host=self.backupset.backup_host,
                                                            backups=self.backups,
                                                            start=1, end=5,
                                                            directory=self.backupset.directory,
                                                            name=self.backupset.name,
                                                            cli_command_location=self.cli_command_location)
            self.sleep(10)
            self._kill_cbbackupmgr()
            merge_result.result(timeout=400)
        except TimeoutError:
            status, output, message = self.backup_list()
            if not status:
                self.fail(message)
            backup_count = 0
            for line in output:
                if re.search("\d{4}-\d{2}-\d{2}T\d{2}_\d{2}_\d{2}.\d+-\d{2}_\d{2}", line):
                    backup_name = re.search("\d{4}-\d{2}-\d{2}T\d{2}_\d{2}_\d{2}.\d+-\d{2}_\d{2}", line).group()
                    if backup_name in self.backups:
                        backup_count += 1
                        self.log.info("{0} matched in list command output".format(backup_name))
            self.assertEqual(backup_count, len(self.backups), "Number of backups after merge crash did not match")
            self.log.info("Number of backups after merge crash matched")

    def test_compact_with_crash(self):
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self.backup_cluster()
        status, output_before_compact, message = self.backup_list()
        if not status:
            self.fail(message)
        try:
            compact_result = self.cluster.async_compact_cluster(backup_host=self.backupset.backup_host,
                                                                backups=self.backups,
                                                                backup_to_compact=self.backupset.backup_to_compact,
                                                                directory=self.backupset.directory,
                                                                name=self.backupset.name,
                                                                cli_command_location=self.cli_command_location)
            self.sleep(10)
            self._kill_cbbackupmgr()
            compact_result.result(timeout=400)
        except TimeoutError:
            status, output_after_compact, message = self.backup_list()
            if not status:
                self.fail(message)
            status, message = self.validation_helper.validate_compact_lists(output_before_compact,
                                                                            output_after_compact,
                                                                            is_approx=True)
            if not status:
                self.fail(message)
            self.log.info(message)

    def test_backup_restore_misc(self):
        """
        Misc scenarios for backup and restore
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backupset.name = "!@#$%^&"
        output, error = self.backup_create()
        self.assertTrue("Backup `!@#$%^` created successfully" in output[0],
                        "Backup could not be created with special characters")
        self.log.info("Backup created with special characters")
        self.backupset.name = "backup"
        self.backup_create()
        self.backup_cluster()
        conn = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "ls -tr {0}/{1}/{2} | tail".format(self.backupset.directory, self.backupset.name, self.backups[0])
        o, e = conn.execute_command(command)
        data_dir = o[0]
        conn.execute_command("dd if=/dev/zero of=/tmp/entbackup/backup/" +
                             str(self.backups[0]) +
                             "/" + data_dir + "/data/shard_0.sqlite" +
                             " bs=1024 count=100 seek=10 conv=notrunc")
        output, error = self.backup_restore()
        self.assertTrue("Restore failed due to an internal issue, see logs for details" in output[-1],
                        "Expected error not thrown when file is corrupt")
        self.log.info("Expected error thrown when file is corrupted")
        conn.execute_command("mv /tmp/entbackup/backup /tmp/entbackup/backup2")
        conn.disconnect()
        output, error = self.backup_restore()
        self.assertTrue("Backup Repository `backup` not found" in output[-1], "Expected error message not thrown")
        self.log.info("Expected error message thrown")

    """ cbbackup restore enhancement only from vulcan """
    def test_cbbackupmgr_collect_logs(self):
        """
           cbbackupmgr collect-logs will collect logs to archive or
           output to any path supplied with flag -o
           CB_ARCHIVE_PATH
           ex: cbbackupmgr collect-logs -a /tmp/backup
               cbbackupmgr collect-logs -a /tmp/backup -o /tmp/logs
        """
        if "5.5" > self.cb_version[:3]:
            self.fail("This test is only for cb version 5.5 and later. ")
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self._collect_logs()

    def test_cbbackupmgr_restore_with_ttl(self):
        """
           cbbackupmgr restore --replace-ttl will replace ttl
           value with flag --replace-ttl-with
           ex: cbbackupmgr restore --replace-ttl all --replace-ttl-with 0
        """
        if "5.5" > self.cb_version[:3]:
            self.fail("This restore with ttl test is only for cb version 5.5 and later. ")
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        if self.replace_ttl == "expired":
            if self.bk_with_ttl:
                self._load_all_buckets(self.master, gen, "create", int(self.bk_with_ttl))
            else:
                self._load_all_buckets(self.master, gen, "create", 0)
        else:
            self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self.backup_cluster_validate()
        if self.bk_with_ttl:
            self.sleep(int(self.bk_with_ttl) + 10, "wait items to be expired in backup")
        compare_function = "=="
        if self.replace_ttl_with:
            compare_function = "<="
        if self.should_fail:
            self.backup_restore()
        else:
            self.backup_restore_validate(compare_uuid=False,
                                         seqno_compare_function=compare_function)

    def test_cbbackupmgr_restore_with_vbuckets_filter(self):
        """
           cbbackupmgr restore --vbuckets-filter 2,3,4,5,6
           it may require to get minimum 2 nodes servers to run this test
        """
        if "5.5" > self.cb_version[:3]:
            self.fail("This test is only for cb version 5.5 and later. ")
        self.num_items = 1000
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self.backup_cluster_validate()
        if self.should_fail:
            self.backup_restore()
        else:
            self.backup_restore_validate()

    def test_cbbackupmgr_with_eventing(self):
        """
            Create backup cluster with saslbucket (default_bucket=False).
            Create events
            Backup cluster
            Create restore cluster
            Restore data back to restore cluster
            Verify events restored back
        """
        if "5.5" > self.cb_version[:3]:
            self.fail("This eventing test is only for cb version 5.5 and later. ")
        from pytests.eventing.eventing_constants import HANDLER_CODE
        from lib.testconstants import STANDARD_BUCKET_PORT

        self.src_bucket_name = self.input.param('src_bucket_name', 'src_bucket')
        self.eventing_log_level = self.input.param('eventing_log_level', 'INFO')
        self.dst_bucket_name = self.input.param('dst_bucket_name', 'dst_bucket')
        self.dst_bucket_name1 = self.input.param('dst_bucket_name1', 'dst_bucket1')
        self.metadata_bucket_name = self.input.param('metadata_bucket_name', 'metadata')
        self.create_functions_buckets = self.input.param('create_functions_buckets', True)
        self.docs_per_day = self.input.param("doc-per-day", 1)
        self.use_memory_manager = self.input.param('use_memory_manager', True)
        bucket_params = self._create_bucket_params(server=self.master, size=128,
                                                       replicas=self.num_replicas)
        self.cluster.create_standard_bucket(name=self.src_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
        self.buckets = RestConnection(self.master).get_buckets()
        self.src_bucket = RestConnection(self.master).get_buckets()
        self.cluster.create_standard_bucket(name=self.dst_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
        self.cluster.create_standard_bucket(name=self.metadata_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
        self.buckets = RestConnection(self.master).get_buckets()
        self.gens_load = self.generate_docs(self.docs_per_day)
        self.expiry = 3

        self.restServer = self.get_nodes_from_services_map(service_type="eventing")
        self.rest = RestConnection(self.restServer)


        self.load(self.gens_load, buckets=self.buckets, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        function_name = "Function_{0}_{1}".format(randint(1, 1000000000), self._testMethodName)
        self.function_name = function_name[0:90]
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE, worker_count=3)
        bk_events_created = False
        rs_events_created = False
        try:
            self.deploy_function(body)
            bk_events_created = True
            self.backup_create()
            self.backup_cluster()
            rest_src = RestConnection(self.backupset.cluster_host)
            bk_fxn = rest_src.get_all_functions()
            if bk_fxn != "":
                self._verify_backup_events_definition(json.loads(bk_fxn))
            self.backup_restore()
            self.rest = RestConnection(self.backupset.restore_cluster_host)
            self.wait_for_handler_state(body['appname'], "deployed")
            rs_events_created = True
            self._verify_restore_events_definition(bk_fxn)
        except Exception as e:
            self.fail(e)

        finally:
            master_nodes = [self.backupset.cluster_host,
                            self.backupset.restore_cluster_host]
            for node in master_nodes:
                self.rest = RestConnection(node)
                buckets = self.rest.get_buckets()
                for bucket in buckets:
                    items = self.rest.get_active_key_count(bucket)
                self.undeploy_and_delete_function(body)
            self.rest = RestConnection(self.master)

    def test_cbbackupmgr_with_n_vbuckets_per_shard(self):
        """
            In Mad-Hatter, by default, cbbackupmgr will create a forestdb file per vbucket.
            But user could modify this number of file by flag --shards <number of file>
            By default, if --shards does not pass, it will create 1024 forestdb files.
            Requirement:
              ini file has config of cluster
              to test with memcached restart, pass param force_kill_memcached=True
              to test with shards action, you need param restore_should_fail=True
              self.backupset.number_of_backups = self.input.param("number_of_backups", 1)
        """
        if self.cb_version[:3] < "6.5":
            self.fail("This test only works for version 6.5 later")
        if self.create_gsi:
            self.create_indexes()
        if self.create_views:
            self._create_views()
        self.bk_merged = False
        self.first_bk_num_shards = self.num_shards
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        if self.replace_ttl == "expired":
            if self.bk_with_ttl:
                self._load_all_buckets(self.master, gen, "create", int(self.bk_with_ttl))
            else:
                self._load_all_buckets(self.master, gen, "create", 0)
        else:
            self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        error_messages = ["Expected argument for option: --shards",
                          "Error backing up cluster:",
                          "Unable to process value for flag: --shards"]
        if self.should_fail:
            error_found = False
            output, error = self.backup_cluster()
            for error in error_messages:
                if self._check_output(error, output):
                    error_found = True
                    self.log.info("\n**** This is negative test for --shards flag with value '{0}'"
                                                                          .format(self.num_shards))
                    break
            if not error_found:
                self.fail("--shards flag does not work correctly")
        else:
            if self.force_kill_memcached:
                self.bk_with_memcached_crash_and_restart()
            elif self.force_restart_erlang:
                self.bk_with_erlang_crash_and_restart()
            elif self.force_restart_couchbase_server:
                self.bk_with_cb_server_stop_and_restart()
            elif self.backupset.resume:
                """ this test needs 100K or greater to run.  items=100000  """
                self.bk_with_stop_and_resume()
            else:
                if self.backupset.number_of_backups > 1:
                    count = 0
                    for i in range(1, self.backupset.number_of_backups + 1):
                        if self.multi_num_shards:
                            self.num_shards = randint(int(self.num_shards), 1023)
                            if count == 0:
                                self.first_bk_num_shards = self.num_shards
                        self.backup_cluster_validate()
                        self.log.info("Update docs")
                        self._load_all_buckets(self.master, gen, "update", self.expires)
                        count += 1
                    self._shards_modification(self.shards_action)
                    self.backupset.start = 1
                    self.backupset.end = self.backupset.number_of_backups
                    result, output, _ = self.backup_merge()
                    if self.merge_should_fail:
                        if result:
                           self.fail("merge should fail since shard is '{0}'"
                                                 .format(self.shards_action))
                        else:
                            self.log.info("This is negative test in shards modification")
                            return
                    elif result:
                        self.bk_merged = True
                else:
                    self.backup_cluster_validate()
                    self._compare_vbuckets_each_shard()

            if self.create_views:
                if not self.backupset.disable_views:
                    view_status, view_mesg = \
                                 self.validate_backup_views(self.backupset.backup_host)
                    if view_status:
                        self.log.info(view_mesg)
                    else:
                        self.fail(view_mesg)
                else:
                    view_status, view_mesg = \
                                 self.validate_backup_views(self.backupset.backup_host)
                    if not view_status:
                        self.log.info(view_mesg)
                    else:
                        self.fail(view_mesg)

            if self.bk_with_ttl:
                self.sleep(int(self.bk_with_ttl) + 10, "wait items to be expired in backup")

        if not self.should_fail:
            if self.restore_should_fail:
                if self.shards_action is not None:
                    self._shards_modification(self.shards_action)
                    output, error = self.backup_restore()
                if not self._check_output("Error restoring cluster", output):
                    self.fail("Restore should fail due to {0} shard".format(self.shards_action))
            else:
                if self.multi_num_shards:
                    self.num_shards = self.first_bk_num_shards
                result, mesg = self._validate_num_files(".sqlite", self.num_shards, self.buckets[0].name)
                if not result:
                    self.fail(mesg)
                self._shards_modification(self.shards_action)
                if self.bk_merged:
                    self.backupset.start = 0
                    self.backupset.end = 0
                if self.do_verify:
                    compare_function = "=="
                    if self.replace_ttl_with:
                        compare_function = "<="
                    self.backup_restore_validate(compare_uuid=False,
                                                 seqno_compare_function=compare_function)
                else:
                    self.backup_restore()
                if self.create_gsi:
                    self.verify_gsi()

    def test_bkrs_with_n_vbuckets_per_shard_and_move_repo(self):
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self.backup_cluster_validate()
        self.log.info("Copy repo to new location")
        shell = RemoteMachineShellConnection(self.backupset.backup_host)
        new_path = "{0}newbkrs".format(self.tmp_path)
        original_path = self.backupset.directory
        shell.execute_command("cp -r {0} {1} ".format(self.backupset.directory, new_path))
        self.backupset.directory = new_path #+ "/"
        self.backup_restore_validate(compare_uuid=False,
                                    seqno_compare_function="==")
        shell.execute_command("rm -rf {0}".format(new_path))
        self.backupset.directory = original_path
        shell.disconnect()
