import os, re, copy, json, subprocess, datetime
from random import randrange, randint, choice
from threading import Thread

from couchbase_helper.cluster import Cluster
from membase.helper.rebalance_helper import RebalanceHelper
from couchbase_helper.documentgenerator import BlobGenerator, DocumentGenerator
from ent_backup_restore.enterprise_backup_restore_base import EnterpriseBackupRestoreBase
from ent_backup_restore.backup_service_upgrade import BackupServiceHook
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
                          ENT_BKRS, ENT_BKRS_FTS

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


class EnterpriseBackupRestoreTest(EnterpriseBackupRestoreBase, NewUpgradeBaseTest):
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
        self.document_type = self.input.param("document_type", "json")

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
                self.log.info("\n*** start to reset cluster")
                self.backup_reset_clusters(self.cluster_to_restore)
                cmd_init = 'node-init'
                if self.same_cluster:
                    self.log.info("Same cluster")
                    self._initialize_nodes(Cluster(), self.servers[:self.nodes_init])
                    if self.hostname and self.master.ip.endswith(".com"):
                        options = '--node-init-hostname ' + self.master.ip
                        shell = RemoteMachineShellConnection(self.master)
                        output, _ = shell.execute_couchbase_cli(cli_command=cmd_init,
                                                    options=options,
                                                    cluster_host="localhost",
                                                    user=self.master.rest_username,
                                                    password=self.master.rest_password)
                        shell.disconnect()
                        if not self._check_output("SUCCESS: Node initialize", output):
                            raise("Failed to set hostname")
                else:
                    self.log.info("Different cluster")
                    shell = RemoteMachineShellConnection(self.backupset.restore_cluster_host)
                    shell.enable_diag_eval_on_non_local_hosts()
                    rest = RestConnection(self.backupset.restore_cluster_host)
                    rest.force_eject_node()
                    rest.init_node()
                    if self.hostname and self.backupset.restore_cluster_host.ip.endswith(".com"):
                        options = '--node-init-hostname ' + self.backupset.restore_cluster_host.ip
                        output, _ = shell.execute_couchbase_cli(cli_command=cmd_init, options=options,
                                                    cluster_host="localhost",
                                                    user=self.backupset.restore_cluster_host.rest_username,
                                                    password=self.backupset.restore_cluster_host.rest_password)
                        if not self._check_output("SUCCESS: Node initialize", output):
                            raise("Failed to set hostname")
                    shell.disconnect()
                self.log.info("\n*** Done reset cluster")
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
        rebalance = self.cluster.async_rebalance(self.cluster_to_backup, serv_in, serv_out)
        self.sleep(10)
        count = 0
        while rebalance.state != "FINISHED":
            if count == 0:
                self.backup_cluster_validate()
            count += 1
        if not self.same_cluster:
            self._initialize_nodes(Cluster(), self.input.clusters[0][:self.nodes_init])
            serv_in = self.input.clusters[0][self.nodes_init: self.nodes_init + self.nodes_in]
            serv_out = self.input.clusters[0][self.nodes_init - self.nodes_out: self.nodes_init]
            rebalance = self.cluster.async_rebalance(self.cluster_to_restore, serv_in, serv_out)
        else:
            rebalance = self.cluster.async_rebalance(self.cluster_to_restore, serv_out, serv_in)
        self.sleep(10)
        count = 0
        while rebalance.state != "FINISHED":
            if count == 0:
                self.backup_restore_validate(compare_uuid=False, seqno_compare_function="<=")
            count += 1

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
        if output and output[0]:
            output = json.loads(output[0])
        if self.buckets[0].name == output["name"]:
            name = True
            items = output["items"]
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
        if output and output[0]:
            bk_info = json.loads(output[0])
            bk_info = bk_info["repos"][0]["backups"]
        else:
            return False, "No output content"

        self.assertEqual(len(bk_info), len(self.backups),
                         "Number of backups did not match.  In repo: {0} != in bk: {1}"\
                         .format(len(bk_info), len(self.backups)))
        for backup in bk_info:
            if backup["date"] not in self.backups:
                raise("backup date does not match")
        self.log.info("Number of backups matched")

    def _take_n_backups(self, n=1, validate=False):
        for i in range(1, n + 1):
            if validate:
                self.backup_cluster_validate()
            else:
                self.backup_cluster()

    def test_backup_info_with_start_end_flag(self):
        """
        1. Create default bucket and load items to bucket
        2. Run number of backups pass by param number_of_backups=x
        3. Run subcommand info with random start and end values.  Value could be index, date or bk nam
        4. conf file name: bkrs-info-with-start-end-flag.conf
        """
        if self.bkinfo_date_start_ago:
            conn = RemoteMachineShellConnection(self.backupset.backup_host)
            start_date_cmd = "date --date=\"{} days ago\" '+%d-%m-%Y' "\
                                        .format(self.bkinfo_date_start_ago)
            output, error = conn.execute_command(start_date_cmd)
            start_date = output[0]
            end_date_cmd =  "date '+%d-%m-%Y' "
            output, error = conn.execute_command(end_date_cmd)
            end_date = output[0]
            conn.disconnect()

        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size,
                                                      end=self.num_items)
        initial_gen = copy.deepcopy(gen)
        self.log.info("Start to load items to all buckets")
        self._load_all_buckets(self.master, gen, "create", 0)
        self.log.info("Create backup repo ")
        self.backup_create()
        for i in range(1, self.backupset.number_of_backups + 1):
            self.backup_cluster()
        self.log.info("done running backup")

        if self.bkinfo_start_end_with_bkname:
            bkname_start_index = int(self.bkinfo_start_end_with_bkname.split(":")[0])
            bkname_start = self.backups[bkname_start_index]
            bkname_end_index = int(self.bkinfo_start_end_with_bkname.split(":")[1])
            bkname_end = self.backups[bkname_end_index]

        if self.bkinfo_date_start_ago:
            o, e = self.backup_info(start=start_date,end=end_date)
        elif self.bkinfo_start_end_with_bkname:
            o, e = self.backup_info(start=bkname_start,end=bkname_end)
        else:
            o, e = self.backup_info(start=self.bkinfo_start,end=self.bkinfo_end)
        if o and o[0]:
            bk_info = json.loads(o[0])
            bk_info = bk_info["backups"]
            if self.debug_logs:
                print("\nbk info : ", bk_info)
                print("\n bkinfo len: ", len(bk_info))
                print("\nbk info date : ", bk_info[0]["date"])
                print("\nbk info type : ", bk_info[0]["type"])
                print("\nnubmer backup : ", self.backups)
        if self.bkinfo_start == 1 and self.bkinfo_end == 1:
            if "FULL" not in bk_info[0]["type"]:
                self.fail("First backup is not full backup")
        elif self.bkinfo_start > 1 and self.bkinfo_end > 1:
            if "INCR" not in bk_info[0]["type"]:
                self.fail("> 0th backup is not incr backup")
        if self.bkinfo_date_start_ago:
            if len(bk_info) != len(self.backups):
                self.fail("bkrs info failed to show all backups today")
        elif self.bkinfo_start_end_with_bkname:
            if len(bk_info) != (bkname_end_index - bkname_start_index + 1):
                self.fail("bkrs info does not show correct nubmer of backups with backup name")
        elif len(bk_info) != (self.bkinfo_end - self.bkinfo_start + 1):
            self.fail("bkrs info does not show correct nubmer of backups")

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
        if output and not self._check_output("Backup completed successfully", output):
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
        version = RestConnection(self.backupset.backup_host).get_nodes_version()
        if version[:5] == "6.5.0":
            self.log.info("\n\n******* Due to issue in MB-36904, \
                               \nthis test will be skipped in 6.5.0 ********\n")
            return
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
        error_msg = "Error merging data: Unable to read bucket settings because bucket-config.json is corrupt"
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
            if not self._check_output(error_msg, output):
                self.fail("read bucket config should fail since bucket-config.json is invalid")
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
        shell = RemoteMachineShellConnection(self.backupset.backup_host)
        os_dist = shell.info.distribution_version.replace(" ", "").lower()
        if "debian" in os_dist:
            shell.execute_command("apt install -y gawk")
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
        status, output, error = self.backup_merge()
        if status:
            self.fail("This merge should fail due to last backup killed, not complete yet")
        elif "Merging backup failed" in error:
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
            if subcommand == "list":
                subcommand = "info"
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
                           '  backup                Backup up data in your Couchbase cluster']
            else:
                content = ['cbbackupmgr {0} [<args>]'.format(subcommand), '',
                           'Required Flags:']
            self.validate_help_content(output[:3], content)
        elif display_option == "--help":
            content = None
            if subcommand == "":
                content = \
                    ['CBBACKUPMGR(1) Couchbase Server Manual CBBACKUPMGR(1)']
                self.validate_help_content(output, content)
            else:
                subcmd_cap = subcommand.upper()
                content = \
                    ['CBBACKUPMGR-{0}(1) Couchbase Server Manual CBBACKUPMGR-{1}(1)'\
                     .format(subcmd_cap, subcmd_cap)]
                self.validate_help_content(output, content)
            if self.bkrs_flag is not None:
                self.assertTrue(self._check_output(self.bkrs_flag, output),
                                 "Missing flag {0} in help content".format(self.bkrs_flag))
        shell.disconnect()

    def test_cbbackupmgr_help_contains_objstore_info(self):
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)

        supports_read_only = ['restore']
        for sub_command in ['backup', 'collect-logs', 'config', 'examine', 'info', 'remove', 'restore']:
            output, error = remote_client.execute_command(f"{self.cli_command_location}/cbbackupmgr {sub_command} -h")
            if error:
                self.fail(f"Expected to be able to get help for {sub_command}")

            arguments = ['--obj-access-key-id', '--obj-cacert', '--obj-endpoint', '--obj-no-ssl-verify',
                             '--obj-region', '--obj-secret-access-key', '--obj-staging-dir', '--s3-force-path-style',
                             '--obj-log-level']

            if sub_command in supports_read_only:
                arguments.append('--obj-read-only')

            for argument in arguments:
                found = False
                for line in output:
                    found = found or argument in line

                self.assertTrue(found, f"Expected to find help about {argument}")

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
                if self._check_output("Backup completed successfully", output):
                    self.fail("Taking cluster backup failed.")
                elif self._check_output("Error", output):
                    verify_data = False
            else:
                if not self._check_output("Backup completed successfully", output):
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

            NOTE: This test requires a specific config/ini to run correctly; if provided with an incorrect config
            testrunner will restore data into the bucket that was backed up on the same cluster without performing a
            flush. This will mean cbbackupmgr will restore with conflict resolution enabled and the validation will find
            an unexpected amount of keys (all of them) in the target bucket.
        """
        key_name = "ent-backup"
        if self.backupset.random_keys:
            key_name = "random_keys"
        self.validate_keys = self.input.param("validate_keys", False)
        if self.validate_keys:
            gen = BlobGenerator(key_name, "ent-backup-", self.value_size,
                                end=self.num_items)
        else:
            gen = DocumentGenerator('random_keys', '{{"age": {0}}}', list(range(100)),
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
        backup_failed = False
        if self.create_fts_index:
            gen = DocumentGenerator('test_docs', '{{"age": {0}}}', list(range(100)), start=0,
                                    end=self.num_items)
            index_definition = INDEX_DEFINITION
            index_name = index_definition['name'] = "age"
            fts_server = self.get_nodes_from_services_map(service_type="fts")
            rest_fts = RestConnection(fts_server)
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
            if "-" in self.cluster_new_role:
                self.cluster_new_role = "[*],".join(self.cluster_new_role.split("-")) + "[*]"
            else:
                self.cluster_new_role = self.cluster_new_role + "[*]"
            admin_roles = ["cluster_admin", "eventing_admin"]
            for role in admin_roles:
               if role in self.cluster_new_role:
                   self.cluster_new_role = self.cluster_new_role.replace(role + "[*]", role)

        self.log.info("\n***** Create new user: {0} with role: {1} to do backup *****"\
                      .format(self.cluster_new_user, self.cluster_new_role))
        testuser = [{"id": "{0}".format(self.cluster_new_user),
                     "name": "{0}".format(self.cluster_new_user),
                     "password": "password"}]
        rolelist = [{"id": "{0}".format(self.cluster_new_user),
                     "name": "{0}".format(self.cluster_new_user),
                     "roles": "{0}".format(self.cluster_new_role)}]
        users_can_backup_all = ["admin", "bucket_full_access[*]",
                                "data_backup[*]", "eventing_admin",
                                "cluster_admin", "backup_admin"]
        users_can_not_backup_all = ["views_admin[*]", "replication_admin",
                                    "replication_target[*]", "data_monitoring[*]",
                                    "data_writer[*]", "data_reader[*]",
                                    "data_dcp_reader[*]", "fts_searcher[*]",
                                    "fts_admin[*]", "query_manage_index[*]",
                                    "ro_admin", "bucket_admin[*]", "cluster_admin"]

        try:
            status = self.add_built_in_server_user(testuser, rolelist)
            if not status:
                self.fail("Fail to add user: {0} with role: {1} " \
                          .format(self.cluster_new_user,
                             self.cluster_new_role))
            output, error = self.backup_cluster()
            success_msg = 'Backup completed successfully'
            fail_msg = ["Error backing up cluster:"]
            for bucket in self.buckets:
                fail_msg.append('Backed up bucket "{0}" failed'.format(bucket.name))
            if self.cluster_new_role in users_can_backup_all:
                if not self._check_output(success_msg, output):
                    rest_bk = RestConnection(self.backupset.cluster_host)
                    eventing_service_in = False
                    bk_cluster_services = list(rest_bk.get_nodes_services().values())
                    for srv in bk_cluster_services:
                        if "eventing" in srv:
                            eventing_service_in = True
                    eventing_err = ["Invalid permissions to backup eventing data",
                                    "cluster.eventing.functions!manage"]
                    if eventing_service_in and self._check_output(eventing_err, output) and \
                        ("admin" not in self.cluster_new_role or \
                         "eventing_admin" not in self.cluster_new_role):
                        self.log.info("Only admin or eventing_admin role could backup eventing service")
                    else:
                        self.fail("User {0} failed to backup data.\n"
                                          .format(self.cluster_new_role) + \
                                          "Here is the output {0} ".format(output))
            elif self.cluster_new_role in users_can_not_backup_all:
                if not self._check_output(fail_msg, output):
                    self.fail("cbbackupmgr failed to block user to backup")
                else:
                    backup_failed = True

            status, _, message = self.backup_list()
            if not status:
                self.fail(message)
            if self.do_verify and not backup_failed:
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
                print(("Exception error:   ", e))
            if self.cluster_new_role in users_can_not_backup_all:
                error_found = False
                error_messages = ["Error backing up cluster: Forbidden",
                                  "Could not find file shard_0.sqlite",
                                  "Error backing up cluster: Invalid permissions",
                                  "Database file is empty",
                                  "Error backing up cluster: Unable to find the latest vbucket",
                                  "Failed to backup bucket"]
                if self.do_verify:
                    if str(e) in error_messages or backup_failed:
                        error_found = True
                    if not error_found:
                        raise Exception("cbbackupmgr does not block user role: {0} to backup" \
                                        .format(self.cluster_new_role))
                    if self.cluster_new_role == "views_admin[*]" and self.create_views:
                        status, mesg = self.validate_backup_views(self.backupset.backup_host)
                        if not status:
                            raise Exception(mesg)
                if "Expected error message not thrown" in str(e):
                    raise Exception("cbbackupmgr does not block user role: {0} to backup" \
                                    .format(self.cluster_new_role))
            if self.cluster_new_role in users_can_backup_all:
                if not self._check_output(success_msg, output):
                    self.fail(e)

        finally:
            if backup_failed:
                self.log.info("cbbackupmgr blocked user: {0} to backup"\
                                                 .format(self.cluster_new_role))
            self.log.info("Delete new create user: {0} ".format(self.cluster_new_user))
            shell = RemoteMachineShellConnection(self.backupset.backup_host)
            curl_path = ""
            if self.os_name == "windows":
                curl_path = self.cli_command_location
            cmd = "{0}curl{1} -g -X {2} -u {3}:{4} http://{5}:8091/settings/rbac/users/local/{6}"\
                  .format(curl_path,
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
            This test need to copy entbackup-mh.tgz
            to /root or /cygdrive/c/Users/Administrator in backup host.
            Files location: 172.23.121.227:/root/entba*.tgz
        """
        all_buckets = self.input.param("all_buckets", False)
        self.log.info("Copy backup dataset to tmp dir")
        shell = RemoteMachineShellConnection(self.backupset.backup_host)

        # Since we are just wiping out the archive here, we can just run the object store teardown
        if self.objstore_provider:
            self.objstore_provider.teardown(shell.extract_remote_info().type.lower(), shell)
        else:
            shell.execute_command("rm -rf {0} ".format(self.backupset.directory))
            shell.execute_command("rm -rf {0} ".format(self.backupset.directory.split("_")[0]))

        backup_file = ENT_BKRS
        backup_dir_found = False
        backup_dir = "entbackup_{0}".format(self.master.ip)
        output, error = shell.execute_command("ls | grep entbackup")
        self.log.info("check if %s dir exists on this server " % backup_dir)
        if output:
            for x in output:
                if x == backup_dir:
                    backup_dir_found = True
        if not backup_dir_found:
            self.log.info("%s dir does not exist on this server.  Downloading.. "
                                                                   % backup_dir)
            shell.execute_command("{0} -q {1} --no-check-certificate -O {2}.tgz "
                                       .format(self.wget, backup_file, backup_dir))
            shell.execute_command("tar -zxvf {0}.tgz ".format(backup_dir))
            shell.execute_command("mv {0} {1}".format(backup_dir.split("_")[0], backup_dir))
        if "-" in self.cluster_new_role:
            self.cluster_new_role = self.cluster_new_role.replace("-", ",")
        if self.objstore_provider and self.objstore_provider.schema_prefix() == "s3://":
            command = ""
            if self.backupset.objstore_region or self.backupset.objstore_access_key_id or self.backupset.objstore_secret_access_key:
                command += "env"
            if self.backupset.objstore_region:
                command += f" AWS_REGION={self.backupset.objstore_region}"
            if self.backupset.objstore_access_key_id:
                command += f" AWS_ACCESS_KEY_ID={self.backupset.objstore_access_key_id}"
            if self.backupset.objstore_secret_access_key:
                command += f" AWS_SECRET_ACCESS_KEY={self.backupset.objstore_secret_access_key}"

            command += " aws"

            if self.backupset.objstore_endpoint:
                command += f" --endpoint={self.backupset.objstore_endpoint}"

            command += f" s3 sync entbackup_{self.master.ip} s3://{self.backupset.objstore_bucket}/{self.backupset.directory}"

            _, error = shell.execute_command(command, debug=False) # Contains senstive info so don't log
            if error:
                self.fail(f"Failed to sync backup to S3: {error}")
        else:
            shell.execute_command("cp -r entbackup_{0}/ {1}/entbackup_{0}"\
                                           .format(self.master.ip, self.tmp_path))
        status, _, message = self.backup_list()
        if not status:
            self.fail(message)

        self.log.info("Restore data from backup files")

        if all_buckets:
            if "bucket_full_access" in self.cluster_new_role and \
                "bucket_full_access[*]" not in self.cluster_new_role:
                self.cluster_new_role = self.cluster_new_role.replace("bucket_full_access",
                                                                      "bucket_full_access[*]")
            else:
                self.cluster_new_role = self.cluster_new_role + "[*]"
            if "data_backup" in self.cluster_new_role and \
                 "data_backup[*]" not in self.cluster_new_role:
                self.cluster_new_role = self.cluster_new_role.replace("data_backup",
                                                                      "data_backup[*]")
            if "fts_admin" in self.cluster_new_role and \
               "fts_admin[*]" not in self.cluster_new_role:
                self.cluster_new_role = self.cluster_new_role.replace("fts_admin",
                                                                      "fts_admin[*]")
            admin_roles = ["cluster_admin", "eventing_admin"]
            for role in admin_roles:
               if role in self.cluster_new_role:
                   self.cluster_new_role = self.cluster_new_role.replace(role + "[*]", role)

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
                                     "data_backup[*]", "eventing_admin"]
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
            rest_rs = RestConnection(self.backupset.restore_cluster_host)
            eventing_service_in = False
            rs_cluster_services = list(rest_rs.get_nodes_services().values())
            for srv in rs_cluster_services:
                if "eventing" in srv:
                    eventing_service_in = True
            eventing_err = "User needs one of the following permissions: cluster.eventing"
            if eventing_service_in and self._check_output(eventing_err, output) and \
                ("admin" not in self.cluster_new_role or \
                 "eventing_admin" not in self.cluster_new_role):
                self.log.info("Only admin role could backup eventing service")
                return
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
            print(("\nActual keys in default bucket: %s \n" % actual_keys))
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
                    if int(actual_keys) != 10000:
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

    def test_backup_restore_of_users(self):
        """
        Backup/Restore of Users Test
        1. Create a cluster
        2. Create a bucket and load data
        3. Create a user with specific role
            param in conf: cluster_new_user
            param in conf: cluster_new_role
        4. Attempt backup with newly created user
        5. Handle backup result based on admin or non-admin role
            Only 'admin' backup should succeed
            Check no. users/groups
        6. If backup successful, attempt restore similarly
            Delete 'cbadminbucket' user before restoring
            Again only 'admin' role permitted
            Deleted user should be recreated
            Conflict message included in restore output
        """
        all_buckets = self.input.param("all_buckets", False)
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self.backupset.enable_users = True
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()

        if all_buckets:
            if "-" in self.cluster_new_role:
                self.cluster_new_role = "[*],".join(self.cluster_new_role.split("-")) + "[*]"
            else:
                self.cluster_new_role = self.cluster_new_role + "[*]"
            admin_roles = ["cluster_admin", "eventing_admin"]
            for role in admin_roles:
               if role in self.cluster_new_role:
                   self.cluster_new_role = self.cluster_new_role.replace(role + "[*]", role)

        self.log.info("\n***** Create new user: {0} with role: {1} to backup user info *****"\
                      .format(self.cluster_new_user, self.cluster_new_role))
        testuser = [{"id": "{0}".format(self.cluster_new_user),
                     "name": "{0}".format(self.cluster_new_user),
                     "password": "password"}]
        rolelist = [{"id": "{0}".format(self.cluster_new_user),
                     "name": "{0}".format(self.cluster_new_user),
                     "roles": "{0}".format(self.cluster_new_role)}]

        status = self.add_built_in_server_user(testuser, rolelist)
        if not status:
            self.fail("Fail to add user: {0} with role: {1} " \
                    .format(self.cluster_new_user, self.cluster_new_role))
        if not self.create_new_group("testgroup", "test group"):
            self.fail("Failed to create new user group")
        backup_failed = False
        output, error = self.backup_cluster()
        fail_msg = ["Error backing up cluster:"]
        for bucket in self.buckets:
            fail_msg.append('Backed up bucket "{0}" failed'.format(bucket.name))
        if "admin" == self.cluster_new_role:
            if error or self._check_output(fail_msg, output):
                self.fail("User {0} failed to backup data.\n"
                            .format(self.cluster_new_role) + \
                            "Here is the output {0} ".format(output))
            info, error = self.backup_info()
            if error:
                self.fail("Error getting backup info: {0}".format(error))
            bk_info = json.loads(info[0])
            bk_info = bk_info["backups"]
            user_count = bk_info[0]["users_count"]
            group_count = bk_info[0]["groups_count"]
            expected_no_users = 2
            expected_no_groups = 1
            if user_count != expected_no_users:
                self.fail("User {0} failed to backup users properly.\n"
                            .format(self.cluster_new_role) + \
                            "Expected {0} != {1} Observed.".format(user_count, expected_no_users))
            elif group_count != expected_no_groups:
                self.fail("User {0} failed to backup groups properly.\n"
                            .format(self.cluster_new_role) + \
                            "Expected {0} != {1} Observed.".format(group_count, expected_no_groups))
        elif not self._check_output(fail_msg, output):
            self.fail("User {0} performed backup with enable-users flag.\n"
                        .format(self.cluster_new_role) + \
                        "Only admin role should be able to backup users.")
        else:
            backup_failed = True
            self.log.info("cbbackupmgr blocked user: {0} from backup with --enable-users flag"\
                        .format(self.cluster_new_role))

        if not backup_failed:
            self.log.info("Beginning --enable-users sanity restore")
            status = self.remove_user()
            if not status:
                self.fail("Error removing user: cbadminbucket")
            rst_output, error = self.backup_restore()
            users_post_restore = []
            for user in self.get_all_users():
                users_post_restore.append(user["id"])
            if "admin" == self.cluster_new_role:
                if error or not self._check_output("Restore completed successfully", rst_output):
                    self.fail("Restoring backup failed: {0}".format(rst_output))
                elif not self._check_output(
                "Restore has skipped some users and/or groups. Please check the logs for more information", rst_output):
                    self.fail("Expected conflict resolution message not found: {0}".format(rst_output))
                elif not users_post_restore.__contains__("cbadminbucket"):
                    self.fail("User 'cbadminbucket' not recreated from backup as expected.")
            elif not self._check_output("Error restoring cluster:", rst_output):
                self.fail("User {0} performed restore with --enable-users flag.\n"
                            .format(self.cluster_new_role)) + \
                            "Only admin role should be able to restore users."
            else:
                self.log.info("cbbackupmgr blocked user: {0} from restore with --enable-users flag"\
                            .format(self.cluster_new_role))

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
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [])
        rebalance.result()
        self.log.info("Restoring to {0} after group change".format(self.backupset.cluster_host.ip))
        try:
            self.log.info("Flush bucket")
            rest_conn.flush_bucket()
            self.backup_restore_validate()
        except Exception as ex:
            self.fail(str(ex))
        finally:
            self.log.info("Moving {0} back to old zone {1}".format(self.backupset.cluster_host.ip, source_zone))
            rest_conn.shuffle_nodes_in_zones(["{0}".format(self.backupset.cluster_host.ip)], target_zone, source_zone)
            self.log.info("Deleting new zone " + target_zone)
            rest_conn.delete_zone(target_zone)
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [])
            rebalance.result()

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
            self.assertIn("failed to connect", output[0],
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
            mesg = "connect: connection refused"
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
        if self.hostname and self.backupset.restore_cluster_host.ip.endswith(".com"):
            self.log.info("\n*** Set node with hostname")
            cmd_init = 'node-init'
            options = '--node-init-hostname ' + self.backupset.restore_cluster_host.ip
            output, _ = conn.execute_couchbase_cli(cli_command=cmd_init, options=options,
                                        cluster_host="localhost",
                                        user=self.backupset.restore_cluster_host.rest_username,
                                        password=self.backupset.restore_cluster_host.rest_password)
            if not self._check_output("SUCCESS: Node initialize", output):
                raise("Failed to set hostname")
        conn.disconnect()
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
        while self._check_output("Backup completed successfully", output):
            gen = BlobGenerator("ent-backup{0}{0}".format(randint(1, 10000)), "ent-backup-",
                                 self.value_size, end=self.num_items)
            self._load_all_buckets(self.master, gen, "create", 0)
            output, error = self.backup_cluster()
        error_msg = "no space left on device"
        self.assertTrue(self._check_output(error_msg, output),
                        "Expected error message not thrown by backup when disk is full")
        self.log.info("Expected error thrown by backup command")
        conn.execute_command("rm -rf /cbqe3043/file")
        conn.disconnect()

    def test_backup_and_restore_with_map_buckets(self):
        """
        1. Creates specified buckets on the cluster and loads it 'num_items'
        2. Creates a backup set, takes backup of the cluster host and validates
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
        status, _, _ = self.backup_list()
        if not status:
            self.fail("Failed to get backup list()")
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
        backup_result = self.cluster.async_backup_cluster(backupset=self.backupset,
                                                          objstore_provider=self.objstore_provider,
                                                          resume=self.backupset.resume, purge=self.backupset.purge,
                                                          no_progress_bar=self.no_progress_bar,
                                                          cli_command_location=self.cli_command_location,
                                                          cb_version=self.cb_version)
        self.sleep(10)
        conn = RemoteMachineShellConnection(self.backupset.cluster_host)
        conn.kill_erlang()
        conn.start_couchbase()
        output = backup_result.result(timeout=200)
        self.assertTrue(self._check_output("Backup completed successfully", output),
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
        backup_result = self.cluster.async_backup_cluster(backupset=self.backupset,
                                                          objstore_provider=self.objstore_provider,
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
        self.assertTrue(self._check_output("Backup completed successfully", output),
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
        backup_result = self.cluster.async_backup_cluster(backupset=self.backupset,
                                                          objstore_provider=self.objstore_provider,
                                                          resume=self.backupset.resume, purge=self.backupset.purge,
                                                          no_progress_bar=self.no_progress_bar,
                                                          cli_command_location=self.cli_command_location,
                                                          cb_version=self.cb_version)
        self.sleep(10)
        conn = RemoteMachineShellConnection(self.backupset.cluster_host)
        conn.pause_memcached()
        conn.unpause_memcached()
        conn.disconnect()
        output = backup_result.result(timeout=200)
        self.assertTrue(self._check_output("Backup completed successfully", output),
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
            backup_result = self.cluster.async_backup_cluster(backupset=self.backupset,
                                                              objstore_provider=self.objstore_provider,
                                                              resume=self.backupset.resume, purge=self.backupset.purge,
                                                              no_progress_bar=self.no_progress_bar,
                                                              cli_command_location=self.cli_command_location,
                                                              cb_version=self.cb_version)
            if self.os_name != "windows":
                self.sleep(10)
            conn = RemoteMachineShellConnection(self.backupset.cluster_host)
            conn.kill_erlang(self.os_name)
            output = backup_result.result(timeout=200)
            if self.debug_logs:
                print(("Raw output from backup run: ", output))
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
            backup_result = self.cluster.async_backup_cluster(backupset=self.backupset,
                                                              objstore_provider=self.objstore_provider,
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
            backup_result = self.cluster.async_backup_cluster(backupset=self.backupset,
                                                              objstore_provider=self.objstore_provider,
                                                              resume=self.backupset.resume, purge=self.backupset.purge,
                                                              no_progress_bar=self.no_progress_bar,
                                                              cli_command_location=self.cli_command_location,
                                                              cb_version=self.cb_version)

            self.sleep(10)
            output = backup_result.result(timeout=200)
            mesg = "Error backing up cluster: Unable to find the latest vbucket sequence numbers"
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
        restore_result = self.cluster.async_restore_cluster(backupset=self.backupset,
                                                            objstore_provider=self.objstore_provider,
                                                            no_progress_bar=self.no_progress_bar,
                                                            cli_command_location=self.cli_command_location,
                                                            cb_version=self.cb_version)
        self.sleep(10)
        conn = RemoteMachineShellConnection(self.backupset.restore_cluster_host)
        conn.kill_erlang(self.os_name)
        conn.start_couchbase()
        conn.disconnect()
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
        restore_result = self.cluster.async_restore_cluster(backupset=self.backupset,
                                                            objstore_provider=self.objstore_provider,
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
        restore_result = self.cluster.async_restore_cluster(backupset=self.backupset,
                                                            objstore_provider=self.objstore_provider,
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
            restore_result = self.cluster.async_restore_cluster(backupset=self.backupset,
                                                                objstore_provider=self.objstore_provider,
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
            restore_result = self.cluster.async_restore_cluster(backupset=self.backupset,
                                                                objstore_provider=self.objstore_provider,
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
        if output and output[0]:
            bk_info = json.loads(output[0])
            bk_info = bk_info["repos"][0]
        else:
            return False, "No output content"

        if bk_info["backups"]:
            for i in range(0, len(bk_info["backups"])):
                backup_name = bk_info["backups"][i]["date"]
                if self.debug_logs:
                    print("backup name ", backup_name)
                    print("backup set  ", self.backups)
                if backup_name in self.backups:
                    backup_count += 1
                    self.log.info("{0} matched in info command output".format(backup_name))
        self.assertEqual(backup_count, len(self.backups), "Initial number of backups did not match")
        self.log.info("Initial number of backups matched")
        self.backupset.start = randrange(1, self.backupset.number_of_backups)
        self.backupset.end = randrange(self.backupset.start + 1, self.backupset.number_of_backups + 1)
        status, output, message = self.backup_merge(check_for_panic=True)
        if not status:
            self.fail(message)
        status, output, message = self.backup_list()
        if not status:
            self.fail(message)
        backup_count = 0
        if output and output[0]:
            bk_info = json.loads(output[0])
            bk_info = bk_info["repos"][0]
        else:
            return False, "No output content"
        if bk_info["backups"]:
            for i in range(0, len(bk_info["backups"])):
                backup_name = bk_info["backups"][i]["date"]
                if self.debug_logs:
                    print("backup name ", backup_name)
                    print("backup set  ", self.backups)
                backup_count += 1
                if backup_name in self.backups:
                    self.log.info("{0} matched in info command output".format(backup_name))
                else:
                    self.fail("Didn't expect backup date {0} from the info command output" \
                              " to be in self.backups (the list of exepected backup dates" \
                              " after a merge)".format(backup_name))

        self.assertEqual(backup_count, len(self.backups), "Merged number of backups did not match")
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
        rest = RestConnection(self.backupset.restore_cluster_host)
        rest.flush_bucket()
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
        backup_result = self.cluster.async_backup_cluster(backupset=self.backupset,
                                                          objstore_provider=self.objstore_provider,
                                                          resume=self.backupset.resume, purge=self.backupset.purge,
                                                          no_progress_bar=self.no_progress_bar,
                                                          cli_command_location=self.cli_command_location,
                                                          cb_version=self.cb_version)
        self.wait_for_DCP_stream_start(backup_result)
        conn = RemoteMachineShellConnection(self.backupset.cluster_host)
        conn.kill_erlang()
        try:
            output = backup_result.result(timeout=200)
            self.log.info(str(output))
            status, output, message = self.backup_list()
            if not status:
                self.fail(message)
            if output and output[0]:
                bk_info = json.loads(output[0])
                bk_info = bk_info["repos"][0]
            else:
                conn.start_couchbase()
                return False, "No output content"
            if bk_info["backups"]:
                for i in range(0, len(bk_info["backups"])):
                    old_backup_name = bk_info["backups"][i]["date"]
                    self.log.info("Backup name before purge: " + old_backup_name)
        except Exception as e:
            conn.start_couchbase()
            raise e
        conn.start_couchbase()
        conn.disconnect()
        self.sleep(30)
        output, error = self.backup_cluster()
        if error or not self._check_output("Backup completed successfully", output):
            self.fail(output)
        status, output, message = self.backup_list()
        if not status:
            self.fail(message)
        if output and output[0]:
            bk_info = json.loads(output[0])
            bk_info = bk_info["repos"][0]
        else:
            return False, "No output content"
        if bk_info["backups"]:
            for i in range(0, len(bk_info["backups"])):
                new_backup_name = bk_info["backups"][i]["date"]
                self.log.info("Backup name after purge: " + new_backup_name)

        # Once the purge (and backup) have completed we shouldn't see any orphaned multipart uploads
        if self.objstore_provider:
            self.assertEqual(
                self.objstore_provider.num_multipart_uploads(), 0,
                "Expected all multipart uploads to have been purged (all newly created ones should have also been completed)"
            )

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
        num_vbuckets = self.input.param("num_vbuckets", None)
        if num_vbuckets:
            remote_client = RemoteMachineShellConnection(self.backupset.cluster_host)
            command = (
                f"curl -X POST -u {self.master.rest_username}:{self.master.rest_password}"
                f" {self.master.ip}:8091/diag/eval -d 'ns_config:set(couchbase_num_vbuckets_default, {num_vbuckets}).'"
            )
            output, _ = remote_client.execute_command(command)
            if 'ok' not in output[0]:
                self.fail(f"failed to reduce the number of vBuckets {num_vbuckets}")
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self.bk_with_stop_and_resume(iterations=self.input.param("iterations", 1),
                                     remove_staging_directory=self.input.param("remove_staging_directory", False))

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
        self.backup_restore_validate(compare_uuid=False, seqno_compare_function="<=")

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
                try:
                    rest.set_recovery_type(otpNode=node.id, recoveryType=self.recoveryType)
                except Exception as e:
                    if "Set RecoveryType failed" in  str(e):
                        self.sleep(15)
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

        Params:
            backup_service_test (bool): Import repository and restore using the backup service.
        """
        upgrade_version = self.input.param("upgrade_version", "5.0.0-3330")
        if upgrade_version == "5.0.0-3330":
            self.fail("\n *** Need param 'upgrade_version=' to run")

        backup_service_test = self.input.param("backup_service_test", False)

        if backup_service_test:
            backup_service_hook = BackupServiceHook(self.servers[1], self.servers, self.backupset, self.objstore_provider)
            self.cli_command_location = "/opt/couchbase/bin"

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

        # Create a backup node and perform a backup service import repository and restore
        if backup_service_test:
            backup_service_hook.backup_service.replace_services(self.servers[1], ['kv,backup'])
            backup_service_hook.backup_service.import_repository(self.backupset.directory, self.backupset.name, "my_repo")
            backup_service_hook.backup_service.take_one_off_restore("imported", "my_repo", 20, 20)
            backup_service_hook.cleanup()
            return

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
        if self.initial_version[:1] == "5" and self.upgrade_versions[0][:1] >= "7":
            self.log.error("\n\n\n*** ERROR: Direct upgrade from {0} to {1} does not support.\
                            Test will skip\n\n"\
                           .format(self.initial_version[:5], self.upgrade_versions[0][:5]))
            return
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
        try:
            bin_path = bin_path.decode()
        except AttributeError:
            pass
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

        self.sleep(100)
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
                if not self.backupset.force_updates:
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

    def test_objstore_negative_args(self):
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = f"{self.cli_command_location}/cbbackupmgr"

        # Run all the sub_commands with the (non-objstore) required arguments (so that we are actually checking the
        # correct error)
        for sub_command in ['backup -a archive -r repo -c localhost -u admin -p password',
                            'collect-logs -a archive',
                            'config -a archive -r repo',
                            'examine -a archive -r repo -k asdf --bucket asdf',
                            'info -a archive',
                            'remove -a archive -r repo',
                            'restore -a archive -r repo -c localhost -u admin -p password']:

            # Check all the object store arguments (ones that require an argument have one provided so that we are
            # validating cbbackupmgr and not cbflag).
            for argument in ['--obj-access-key-id asdf',
                             '--obj-cacert asdf',
                             '--obj-endpoint asdf',
                             '--obj-log-level asdf',
                             '--obj-no-ssl-verify',
                             '--obj-region asdf',
                             '--obj-secret-access-key asdf']:

                # Check all the common object store commands
                output, error = remote_client.execute_command(f"{command} {sub_command} {argument}")
                remote_client.log_command_output(output, error)
                self.assertNotEqual(len(output), 0)
                error_mesg = "cloud arguments provided without the cloud scheme prefix"
                if "bucket" in sub_command:
                    error_mesg = "Unknown flag: --bucket"
                self.assertIn(error_mesg, output[0],
                                "Expected an error about providing cloud arguments without the cloud schema prefix")

                # Check all the S3 specific arguments
                if self.objstore_provider.schema_prefix() == 's3://':
                    for argument in ['--s3-force-path-style']:
                        output, error = remote_client.execute_command(f"{command} {sub_command} {argument}")
                        remote_client.log_command_output(output, error)
                        self.assertNotEqual(len(output), 0)
                        error_mesg_obj = "s3 arguments provided without the archive 's3://' schema prefix"
                        if "bucket" in sub_command:
                            error_mesg_obj = "Unknown flag: --bucket"
                        self.assertIn(error_mesg_obj, output[0],
                                        "Expected an error about providing S3 specific arguments without the s3:// schema prefix")

            # Check all the common objstore flags that require arguments without providing arguments. This is testing
            # cbflag.
            for argument in ['--obj-access-key-id',
                             '--obj-cacert',
                             '--obj-endpoint',
                             '--obj-log-level',
                             '--obj-region',
                             '--obj-secret-access-key']:

                # Check that common object store arguments that require a value throw the correct error when a value
                # is omitted.
                output, error = remote_client.execute_command(
                    f"{command} {sub_command.replace('archive', self.objstore_provider.schema_prefix() + 'archive')} --obj-staging-dir staging {argument}"
                )
                remote_client.log_command_output(output, error)
                self.assertNotEqual(len(output), 0)
                error_mesg = f"Expected argument for option: {argument}"
                if "bucket" in sub_command:
                    error_mesg = "Unknown flag: --bucket"
                self.assertIn(error_mesg, output[0],
                                "Expected an error about providing cloud arguments without a value")

            # Test omitting the staging directory argument
            output, error = remote_client.execute_command(
                f"{command} {sub_command.replace('archive', self.objstore_provider.schema_prefix() + 'archive')}"
            )
            remote_client.log_command_output(output, error)
            self.assertNotEqual(len(output), 0)
            error_mesg = "you must provide the '--obj-staging-dir' argument"
            if "bucket" in sub_command:
                error_mesg = "Unknown flag: --bucket"
            self.assertIn(error_mesg, output[0],
                            "Expected an error about not supplying the '--obj-staging-dir' argument")

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
            if "7.0.1" in self.cb_version:
                self.assertIn("Error restoring cluster: Backup backup doesn't contain any backups", output[-1])
            else:
                self.assertIn("Error restoring cluster: Repository 'backup' doesn't contain any backups", output[-1])
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
        cmd = cmd_to_test + " --archive xyz -c http://localhost:8091 -u Administrator -p password -r aa"
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertTrue(self._check_output("archive '{0}xyz' does not exist".format(self.root_path), output))
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
        self.assertIn("cluster credentials required, expected --username/--password or --client-cert/--client-key", output[0],
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
        self.assertIn("the --username/--password flags must be supplied together", output[0],
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
        self.assertIn(f"Error {part_message} cluster: failed to bootstrap client: failed to connect to any host(s) from the connection string", output[-1])
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
        cmd = "info"
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], "cbbackupmgr info [<args>]", "Expected error message not thrown")
        cmd = "info --archive"
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], "Expected argument for option: --archive", "Expected error message not thrown")
        cmd = "info --archive xyz".format(self.backupset.directory)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        remote_client.disconnect()
        self.assertTrue(self._check_output("archive '{0}xyz' does not exist".format(self.root_path), output))

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
        self.assertEqual(output[0], "Flag required, but not specified: --backup",
                         "Expected error message not thrown")
        cmd = "compact --archive {0} --repo {1} --backup" \
            .format(self.backupset.directory, self.backupset.name)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], "Expected argument for option: --backup",
                         "Expected error message not thrown")
        cmd = "compact --archive xyz --repo {0} --backup {1}" \
            .format(self.backupset.name, self.backups[0])
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertIn(f"archive '/root/xyz' does not exist", output[-1])
        cmd = "compact --archive {0} --repo abc --backup {1}" \
            .format(self.backupset.directory, self.backups[0])
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertTrue(self._check_output("Backup Repository `abc` not found", output),
                        "Expected error message not thrown")
        cmd = "compact --archive {0} --repo {1} --backup abc".format(self.backupset.directory,
                                                                     self.backupset.name)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        remote_client.disconnect()
        self.assertTrue("Compacting incr backup `backup` of backup `abc` failed:" in output[-1],
                        "Expected error message not thrown")

    def test_backup_merge_negative_args(self):
        """
        Validates error messages for negative inputs of merge command
        """
        # This error message is thrown when an invalid date range format is supplied to cbbackupmgr.
        invalid_range_format_error = "Error merging data: invalid range format, expected two indexes or two dates; the keywords [start, oldest, end, latest] are also valid"

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
        self.assertEqual(output[0], "Error merging data: Repository 'backup' doesn't contain any backups",
                         "Expected error message not thrown")
        self._take_n_backups(n=2)
        cmd = "merge --archive {0} --repo {1}".format(self.backupset.directory, self.backupset.name)
        command = "{0}/cbbackupmgr {1} --start bbb --end end".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], invalid_range_format_error, "Expected error message not thrown")
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
        self.assertEqual(output[0], invalid_range_format_error, "Expected error message not thrown")
        cmd = "merge --archive {0} --repo {1} --start {2} --end".format(self.backupset.directory,
                                                                        self.backupset.name, self.backups[0])
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertEqual(output[0], "Expected argument for option: --end", "Expected error message not thrown")
        cmd = "merge --archive xyz --repo {0} --start {1} --end {2}".format(self.backupset.name,
                                                                            self.backups[0], self.backups[1])
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertTrue("Error merging data: archive '/root/xyz' does not exist" in output[-1],
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
        self.assertTrue(invalid_range_format_error in output[-1], "Expected error message not thrown")
        cmd = "merge --archive {0} --repo {1} --start {2} --end abc".format(self.backupset.directory,
                                                                            self.backupset.name, self.backups[0])
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertTrue(invalid_range_format_error in output[-1], "Expected error message not thrown")
        cmd = "merge --archive {0} --repo {1} --start {2} --end {3}".format(self.backupset.directory,
                                                                            self.backupset.name,
                                                                            self.backups[1], self.backups[0])
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        remote_client.disconnect()
        self.assertTrue("Error merging data: invalid range start cannot be before end" in output[-1], "Expected error message not thrown")

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
        cmd = "remove --archive xyz --repo {0}".format(self.backupset.name)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.assertTrue("Removing backup repository failed: archive '{0}xyz' does not exist".format(self.root_path) in output[-1],
                        "Expected error message not thrown")
        cmd = "remove --archive {0} --repo xyz".format(self.backupset.directory)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        remote_client.disconnect()
        self.assertIn("Backup Repository `xyz` not found", output[-1])

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
            self.log.info("\n****** views are not supported on ephemeral bucket ******")
            return

        if self.bucket_storage == "magma":
            self.log.info("\n****** views are not supported on magma storage ******")
            return

        rest_src = RestConnection(self.backupset.cluster_host)
        if "community" in self.cb_version:
            rest_src.add_node(self.servers[1].rest_username, self.servers[1].rest_password,
                            self.servers[1].cluster_ip, services=['kv', 'index', 'n1ql'])
        else:
            rest_src.add_node(self.servers[1].rest_username, self.servers[1].rest_password,
                            self.servers[1].cluster_ip, services=['index', 'kv'])
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
        if self.input.clusters[0][1].ip != self.servers[1].ip:
            rest_target.add_node(self.input.clusters[0][1].rest_username,
                                 self.input.clusters[0][1].rest_password,
                                 self.input.clusters[0][1].cluster_ip, services=['kv', 'index'])
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
        if "community" in self.cb_version:
            rest_src.add_node(self.servers[1].rest_username, self.servers[1].rest_password,
                            self.servers[1].cluster_ip, services=['kv', 'index', 'n1ql'])
        else:
            rest_src.add_node(self.servers[1].rest_username, self.servers[1].rest_password,
                            self.servers[1].cluster_ip, services=['kv', 'index'])
        rebalance = self.cluster.async_rebalance(self.cluster_to_backup, [], [])
        rebalance.result()
        self.test_storage_mode = self.cluster_storage_mode
        if "ephemeral" in self.bucket_type:
            self.log.info("ephemeral bucket needs to set backup cluster to memopt for gsi.")
            self.test_storage_mode = "memory_optimized"
            self.quota = self._reset_storage_mode(rest_src, self.test_storage_mode)
            rest_src.add_node(self.servers[1].rest_username, self.servers[1].rest_password,
                          self.servers[1].cluster_ip, services=['kv', 'index'])
            rebalance = self.cluster.async_rebalance(self.cluster_to_backup, [], [])
            rebalance.result()
            rest_src.create_bucket(bucket='default', ramQuotaMB=int(self.quota) - 1,
                                   bucketType=self.bucket_type,
                                   evictionPolicy="noEviction")
            self.add_built_in_server_user(node=self.backupset.cluster_host)

        gen = DocumentGenerator('test_docs', '{{"age": {0}}}', list(range(100)),
                                start=0, end=self.num_items)
        self.buckets = rest_src.get_buckets()
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()

        cmd = "cbindex -type create -bucket default -using %s -index age -fields=age " \
              " -auth %s:%s" % (self.test_storage_mode,
                                self.master.rest_username,
                                self.master.rest_password)
        if self.input.param("enforce_tls", False):
            if self.input.param("x509", False):
                cmd += f" -use_tls -cacert {self.x509.CACERTFILEPATH}all/all_ca.pem"
            else:
                cmd += " -use_tls -cacert /opt/couchbase/var/lib/couchbase/config/certs/ca.pem"
        shell = RemoteMachineShellConnection(self.backupset.cluster_host)
        cli_location = self.cli_command_location if not self.input.param("tools_package", False) else self.previous_cli
        command = "{0}/{1}".format(cli_location, cmd)
        output, error = shell.execute_command(command)
        shell.log_command_output(output, error)
        shell.disconnect()
        if error or "Index created" not in output[-1]:
            self.fail("GSI index cannot be created")
        self.backup_cluster_validate()

        rest_target = RestConnection(self.backupset.restore_cluster_host)
        if "ephemeral" not in self.bucket_type and self.backupset.restore_cluster_host.cluster_ip != self.backupset.cluster_host.cluster_ip:
            rest_target.add_node(self.input.clusters[0][1].rest_username,
                                 self.input.clusters[0][1].rest_password,
                                 self.input.clusters[0][1].cluster_ip, services=['kv', 'index'])
            rebalance = self.cluster.async_rebalance(self.cluster_to_restore, [], [])
            rebalance.result()
        self.backup_restore_validate(compare_uuid=False, seqno_compare_function=">=")

        cmd = "cbindex -type list -auth %s:%s" % (self.master.rest_username,
                                                  self.master.rest_password)
        if self.input.param("enforce_tls", False):
            if self.input.param("x509", False):
                cmd += f" -use_tls -cacert {self.x509.CACERTFILEPATH}all/all_ca.pem"
            else:
                cmd += " -use_tls -cacert /opt/couchbase/var/lib/couchbase/config/certs/ca.pem"
        shell = RemoteMachineShellConnection(self.backupset.restore_cluster_host)
        command = "{0}/{1}".format(cli_location, cmd)
        output, error = shell.execute_command(command)
        shell.log_command_output(output, error)
        shell.disconnect()

        try:
            if len(output) > 1:
                index_name_path =  "Index:{0}/{1}".format(self.buckets[0].name, "age")
                version = RestConnection(
                              self.backupset.restore_cluster_host).get_nodes_version()
                if version[:1] >= "7":
                    index_name_path =  "Index:{0}/_{0}/_{0}/{1}".format(self.buckets[0].name, "age")
                self.assertTrue(self._check_output(index_name_path, output),
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
                          self.servers[1].cluster_ip, services=['index'])
        rebalance = self.cluster.async_rebalance(self.cluster_to_backup, [],
                                                 [])
        rebalance.result()
        gen = DocumentGenerator('test_docs', '{{"Num1": {0}, "Num2": {1}}}',
                                list(range(100)), list(range(100)),
                                start=0, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        cmd = "cbindex -type create -bucket default -using forestdb -index " \
              "num1 -fields=Num1"
        remote_client = RemoteMachineShellConnection(
            self.backupset.cluster_host)
        cli_location = self.cli_command_location if not self.input.param("tools_package", False) else self.previous_cli
        command = "{0}/{1}".format(cli_location, cmd)
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
        command = "{0}/{1}".format(cli_location, cmd)
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
                             self.input.clusters[0][1].cluster_ip, services=['index'])
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
        command = "{0}/{1}".format(cli_location, cmd)
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
        self.test_fts = True
        rest_src = RestConnection(self.backupset.cluster_host)
        if "community" in self.cb_version:
            rest_src.add_node(self.servers[1].rest_username, self.servers[1].rest_password,
                            self.servers[1].cluster_ip, services=['kv', 'index', 'n1ql', 'fts'])
        else:
            rest_src.add_node(self.servers[1].rest_username, self.servers[1].rest_password,
                            self.servers[1].cluster_ip, services=['kv', 'fts'])
        rebalance = self.cluster.async_rebalance(self.cluster_to_backup, [], [])
        rebalance.result()
        gen = DocumentGenerator('test_docs', '{{"age": {0}}}', list(range(100)), start=0,
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
        if self.bucket_type != "ephemeral":
            self._create_restore_cluster()
        self.backup_restore_validate(compare_uuid=False, seqno_compare_function=">=")
        if self.enforce_tls:
            self.master.port = '8091'
            self.master.protocol = "http://"
            for server in self.servers:
                server.port = '8091'
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
            kwargs = {}
            if self.input.param("enforce_tls", False):
                kwargs["demandEncryption"] = 1
                trusted_ca = rest_dest.get_trusted_CAs()[-1]["pem"]
                kwargs["certificate"] = trusted_ca
            rest_src.add_remote_cluster(self.servers[1].ip, self.servers[1].port, self.backupset.cluster_host_username,
                                        self.backupset.cluster_host_password, "C2", **kwargs)
            rest_dest.create_bucket(bucket='default', ramQuotaMB=512)
            self.sleep(10)
            repl_id = rest_src.start_replication('continuous', 'default', "C2")
            if repl_id is not None:
                self.log.info("Replication created successfully")
            gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
            tasks = self._async_load_all_buckets(self.master, gen, "create", 0)

            reps = rest_src.get_replications()
            start_time = datetime.datetime.now()
            while reps[0]["status"] != "running" or reps[0]["changesLeft"] > 0:
                if (datetime.datetime.now() - start_time).total_seconds() > 600:
                    self.fail("Timed out waiting for replications")
                self.sleep(10, "Waiting for replication...")
                reps = rest_src.get_replications()
            self.backup_create()
            self.backup_cluster_validate()
            self.backup_restore_validate(compare_uuid=False, seqno_compare_function="<=")
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

    def test_resume_restore(self):
        """
        1. Creates specified bucket on the cluster and loads it
        2. Performs a backup
        3. Starts, then kills a restore
        4. Performs and validates a restore using resume
        """
        if not self.backupset.resume:
            self.fail("Resume must be True for this test")
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self.backup_cluster_validate()
        self.log.info("Start to flush bucket")
        self._all_buckets_flush()
        restore_result = self.cluster.async_restore_cluster(backupset=self.backupset,
                                                            objstore_provider=self.objstore_provider,
                                                            no_progress_bar=self.no_progress_bar,
                                                            cli_command_location=self.cli_command_location,
                                                            cb_version=self.cb_version,
                                                            force_updates=self.backupset.force_updates,
                                                            no_resume=True)
        state = ""
        while state not in ("FINISHED", "EXECUTING"):
            state = restore_result.state
        self._kill_cbbackupmgr()
        self.assertFalse(self._check_output("success", restore_result.result()))
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
                if "entbackup" in line:
                    continue
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

    def test_backup_logs_for_keywords(self):
        """
        Inspired by CBQE-6034.

        1. Perform a Backup.
        2. Scan backup logs for bad keywords.

        Keywords:
        1. CBQE-6034/MB-41131 - Check cbbackupmgr's build version/hash set correctly at build time
        by scanning for 'cbbackupmgr version Unknown' in the logs.
        2. Scan for 'panic' in the logs.
        """
        # Populate the default bucket on self.master with documents
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)

        # Create backup archive and repository.
        self.backup_create()

        # Perform backup.
        self.backup_cluster()

        # Keywords to fail on (Keyword: str, at_start: bool, lines_before: int, lines_after: int)
        bad_keywords = [
                ("cbbackupmgr version Unknown", False, 0,  0), # Checks cbbackupmgr build version/hash set correctly at build time
                (                      "panic",  True, 0, 12)  # Checks for the panic keyword at start of sentence
        ]

        # Scan logs for keywords in bad_keywords
        for keyword, at_start, lines_before, lines_after in bad_keywords:

            found, output, error = \
                    self._check_output_in_backup_logs(keyword, at_start = at_start, lines_before = lines_before, lines_after = lines_after)

            if found:
                self.fail(f"Found bad keyword(s) '{keyword}' in backup logs:\n" + "\n".join(output))


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
        self.backup_cluster()
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
        if self.should_fail:
            self.backup_cluster()
        else:
            self.backup_cluster_validate()
            if self.restore_should_fail:
                self.backup_restore()
            else:
                self.backup_restore_validate()

    def test_cbbackupmgr_with_eventing(self):
        """
            Create backup cluster with saslbucket (default_bucket=False).
            Backup cluster (backup_before_eventing=True for MB-34077)
            Create events
            Backup cluster
            Create restore cluster
            Restore data back to restore cluster
            Check if metadata restored (backup_before_eventing=True)
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
        self.backup_before_eventing = self.input.param('backup_before_eventing', False)
        bucket_params = self._create_bucket_params(server=self.master, size=256,
                                                       replicas=self.num_replicas)
        self.cluster.create_standard_bucket(name=self.src_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
        self.buckets = RestConnection(self.master).get_buckets()
        self.src_bucket = RestConnection(self.master).get_buckets()
        self.cluster.create_standard_bucket(name=self.dst_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
        self.backup_create()
        if (self.backup_before_eventing):
            self.backup_cluster()
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
            self.backup_cluster()
            rest_bk = RestConnection(self.backupset.cluster_host)
            bk_fxn = rest_bk.get_all_functions()

            backup_index = 0

            if self.backup_before_eventing:
                backup_index = 1
                self.backupset.start = 1
                self.backupset.end = 2

            if bk_fxn != "":
                self._verify_backup_events_definition(json.loads(bk_fxn), body, backup_index = backup_index)

            self.backup_restore()

            rest_rs = RestConnection(self.backupset.restore_cluster_host)

            if self.backup_before_eventing:
                self.assertTrue('metadata' in [bucket.name for bucket in rest_rs.get_buckets()])

            self.bkrs_resume_function(body, rest_rs)
            rs_events_created = True
            self._verify_restore_events_definition(bk_fxn)
        except Exception as e:
            self.fail(e)
        finally:
            master_nodes = [self.backupset.cluster_host,
                            self.backupset.restore_cluster_host]
            for node in master_nodes:
                rest = RestConnection(node)
                self.bkrs_undeploy_and_delete_function(body, rest, node)
            self.rest = RestConnection(self.master)

    def test_bkrs_logs_when_no_mutations_received(self):
        """
        Test that we log an expected message when we don't receive any
        mutations for more than 60 seconds. MB-33533.
        """
        version = RestConnection(self.backupset.backup_host).get_nodes_version()
        if "6.5" > version[:3]:
            self.fail("Test not supported for versions pre 6.5.0. "
                      "Version was run with {}".format(version))

        rest_conn = RestConnection(self.backupset.cluster_host)
        rest_conn.update_autofailover_settings(enabled=False, timeout=0)

        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size,
                            end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        cluster_client = RemoteMachineShellConnection(self.backupset.cluster_host)
        cluster_client.install_psmisc()

        backup_result = self.cluster.async_backup_cluster(backupset=self.backupset,
                                                          objstore_provider=self.objstore_provider,
                                                          resume=self.backupset.resume, purge=self.backupset.purge,
                                                          no_progress_bar=self.no_progress_bar,
                                                          cli_command_location=self.cli_command_location,
                                                          cb_version=self.cb_version)

        # We need to wait until the data transfer starts before we pause memcached.
        # Read the backup file output until we find evidence of a DCP connection,
        # or the backup finishes.
        backup_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "tail -n 1 {}/logs/backup-*.log | grep ' (DCP) '"\
                                               .format(self.backupset.directory)
        Future.wait_until(
            lambda: (bool(backup_client.execute_command(command)[0]) or backup_result.done()),
            lambda x: x is True,
            200,
            interval_time=0.1,
            exponential_backoff=False)
        # If the backup finished and we never saw a DCP connection something's not right.
        if backup_result.done():
            self.fail("Never found evidence of open DCP stream in backup logs.")
        # Pause memcached to trigger the log message.
        cluster_client.pause_memcached(self.os_name, timesleep=200)
        cluster_client.unpause_memcached(self.os_name)
        cluster_client.disconnect()
        backup_result.result(timeout=200)

        expected_message = "(timed out after 3m0s|Stream has been inactive for 1m0s)"
        command = "cat {}/logs/backup-*.log | grep -E '{}' " \
                         .format(self.backupset.directory, expected_message)
        output, _ = backup_client.execute_command(command)
        if not output:
            self.fail("Mutations were blocked for over 60 seconds, "
                      "but this wasn't logged.")
        backup_client.disconnect()

    def test_log_to_stdout(self):
        """
        Test that if the log-to-stdout flag is provided cbbackupmgr will log to stdout
        :return:
        """

        version = RestConnection(self.backupset.backup_host).get_nodes_version()
        if "6.5" > version[:3]:
            self.fail("Test not supported for versions pre 6.5.0"
                      "Version was run with {}".format(version))

        self.backupset.log_to_stdout = True
        # Test config
        output, err = self.backup_create()
        if err:
            self.fail("Could not create backup directory")

        # This is a line that is normally printed in the logs but should now instead be printed to stdout
        if "(Cmd) cbbackupmgr version" not in " ".join(output):
            self.fail("Did not log to standard out")

        # Test backup
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        output, err = self.backup_cluster()
        if err:
            self.fail("Could not backup")

        if "(Cmd) cbbackupmgr version" not in " ".join(output):
            self.fail("Did not log to standard out")

        self.backupset.force_updates = True

        # Test restore
        output, err = self.backup_restore()
        if err:
            self.fail("Could not restore")

        if "(Cmd) cbbackupmgr version" not in " ".join(output):
            self.fail("Did not log to standard out")

    def test_auto_select_threads(self):
        """
        Test that the --auto-select-threads flag actually selects the threads
        :return:
        """

        version = RestConnection(self.backupset.backup_host).get_nodes_version()
        if "6.5" > version[:3]:
            self.fail("Test not supported for versions pre 6.5.0"
                      "Version was run with {}".format(version))

        self.backupset.auto_select_threads = True

        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self.backup_cluster()

        # If the threads where auto-selected then a log message should appear
        shell = RemoteMachineShellConnection(self.backupset.backup_host)
        output, _ = shell.execute_command("cat {}/logs/backup-*.log | grep"
                                          " '(Cmd) Automatically set the number"
                                          " of threads to'".format(self.backupset.directory))
        if not output:
            self.fail("Threads were not automatically selected")

        # Remove the logs and test the same thing for restore
        shell.execute_command("rm -r {}/logs".format(self.backupset.directory))

        self.backupset.force_updates = True
        self.backup_restore()
        output, _ = shell.execute_command("cat {}/logs/backup-*.log | grep"
                                          " '(Cmd) Automatically set the number"
                                          " of threads to'".format(self.backupset.directory))
        if not output:
            self.fail("Threads were not automatically selected")

        shell.disconnect()

    def test_backup_remove_take_backup_range(self):
        """
        Test the remove --backups flag it should be able to take:
         - backup indexes e.g (0,3)
         - backup directory names range
         - dd-mm-yyyy ranges

        To do this the steps are as follow:
        1. Load some data to cluster
        2. Create 3 backups
        3. Try the different inputs and verify expected outputs

        :return:
        """
        version = RestConnection(self.backupset.backup_host).get_nodes_version()
        if "6.5" > version[:3]:
            self.fail("Test not supported for versions pre 6.5.0"
                      "Version was run with {}".format(version))

        # Test based on actual directory names have to be dynamically created based on the directory names.
        test_ranges_positive_cases = [
            "1,3", # valid index range
            "10-01-2000,10-01-3000", # valid date range
        ]

        test_range_invalid_cases = [
            "1,-10", # invalid end range negative number
            "0,100", # invalid range as there are only 3 backups
            "2,0", # invalid range start bigger than end
            "01/01/2000,01/01/3000", # invalid date format
            "01-30-2000,01-30-3000", # invalid date format
        ]

        # Load some data into the cluser
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)

        for test in test_ranges_positive_cases:
            # create the backup repository and make three backups
            self.backup_create()
            self._take_n_backups(n=3)

            # remove the backup directory
            success, _, _ = self.backup_remove(test)
            if not success:
                self.fail("Failed to remove backups")

            self._verify_backup_directory_count(0)
            self._delete_repo()

        for test in test_range_invalid_cases:
            # create the backup repository and make three backups
            self.backup_create()
            self._take_n_backups(n=3)

            success, _, _ = self.backup_remove(test)
            if success:
                self.fail("Test should have failed")

            self._verify_backup_directory_count(3)
            self._delete_repo()

        #  Test based on dynamic file names
        self.backup_create()
        self._take_n_backups(n=3)

        shell = RemoteMachineShellConnection(self.backupset.backup_host)
        command = (
            f"ls -l {self.backupset.objstore_staging_directory + '/' if self.objstore_provider else ''}"
            f"{self.backupset.directory}/{self.backupset.name}"
        )
        list_dir, _ = shell.execute_command(command)
        list_dir = " ".join(list_dir)
        shell.disconnect()

        dir_names = re.findall(r'(?P<dir>\d{4}-\d{2}-\d{2}T\d{2}_\d{2}_\d{2}\.\d+(?:(?:[+-]\d{2}_\d{2})|Z))', list_dir)
        dir_names.sort()

        if len(dir_names) != 3:
            self.fail("Expected 3 backups instead have {0}".format(len(dir_names)))

        # test non existent directory name
        success, _, _ = self.backup_remove("3000-09-30T10_42_37.64647+01_00")
        if success:
            self.fail("Should not be able to remove non existent directory")

        self._verify_backup_directory_count(3)

        # test start > backup start
        success, _, _ = self.backup_remove("3000-09-30T10_42_37.64647+01_00,3000-09-30T10_43_37.64647+01_00")
        if success:
            self.fail("Should not be able to remove by directory range where the start is in the future")

        self._verify_backup_directory_count(3)

        # test start == backup start end > backup end
        success, _, _ = self.backup_remove("{0}.64647+01_00,3000-09-30T10_43_37.64647+01_00".format(dir_names[0]))
        if success:
            self.fail("Should not be able to remove by directory range where the end is in the future")

        self._verify_backup_directory_count(3)

        # test start before end
        success, _, _ = self.backup_remove("{0},{1}".format(dir_names[-1], dir_names[0]))
        if success:
            self.fail("Should not be able to remove by directory range where start is after end")

        self._verify_backup_directory_count(3)

        # test valid single directory
        success, _, _ = self.backup_remove("{0}".format(dir_names[0]))
        if not success:
            self.fail("Should not have failed to remove directories by backup directory name")

        self._verify_backup_directory_count(2)

        # test valid
        success, _, _ = self.backup_remove("{0},{1}".format(dir_names[1], dir_names[-1]))
        if not success:
            self.fail("Should not have failed to remove directories by backup directory name range")

        self._verify_backup_directory_count(0)

    def test_backup_merge_date_range(self):
        """
        Test the merge --date-range flag it should be able to take:
         - backup indexes e.g (0,3)
         - backup directory names range
         - dd-mm-yyyy ranges

        To do this the steps are as follow:
        1. Load some data to cluster
        2. Create 3 backups
        3. Try the different inputs and verify expected outputs

        :return:
        """
        version = RestConnection(self.backupset.backup_host).get_nodes_version()
        if "6.5" > version[:3]:
            self.fail("Test not supported for versions pre 6.5.0"
                      "Version was run with {}".format(version))

        # Test based on actual directory names have to be dynamically created based on the directory names.
        test_ranges_positive_cases = [
            "0,2", # valid index range
            "10-01-2000,10-01-3000", # valid date range
        ]

        test_range_invalid_cases = [
            "1,-10", # invalid end range negative number
            "0,100", # invalid range as there are only 3 backups
            "2,0", # invalid range start bigger than end
            "01/01/2000,01/01/3000", # invalid date format
            "01-30-2000,01-30-3000", # invalid date format
        ]

        # Load some data into the cluser
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)

        for test in test_ranges_positive_cases:
            # create the backup repository and make three backups
            self.backup_create()
            self._take_n_backups(3)

            self.backupset.date_range = test
            status, output , _ = self.backup_merge()
            if not status:
                self.fail("Failed to merge backups: {0}".format(output))



            self._verify_backup_directory_count(1)
            self._delete_repo()

        for test in test_range_invalid_cases:
            # create the backup repository and make three backups
            self.backup_create()
            self._take_n_backups(3)

            self.backupset.date_range = test
            status, output, _ = self.backup_merge()
            if status:
                self.fail("Test should have failed")

            self._verify_backup_directory_count(3)

        #  Test based on dynamic file names
        shell = RemoteMachineShellConnection(self.backupset.backup_host)
        command = (
            f"ls -l {self.backupset.objstore_staging_directory + '/' if self.objstore_provider else ''}"
            f"{self.backupset.directory}/{self.backupset.name}"
        )
        list_dir, _ = shell.execute_command(command)
        list_dir = " ".join(list_dir)
        shell.disconnect()

        dir_names = re.findall(r'(?P<dir>\d{4}-\d{2}-\d{2}T\d{2}_\d{2}_\d{2}\.\d+(?:(?:[+-]\d{2}_\d{2})|Z))', list_dir)
        dir_names.sort()

        if len(dir_names) != 3:
            self.fail("Expected 3 backups instead have {0}".format(len(dir_names)))

        # test start > backup start
        self.backupset.date_range = "3000-09-30T10_42_37.64647+01_00,3000-09-30T10_43_37.64647+01_00"
        status, _, _ = self.backup_merge()
        if status:
            self.fail("Should not be able to merge by directory range where the start is in the future")

        self._verify_backup_directory_count(3)

        # test start == backup start end > backup end
        self.backupset.date_range = "{0}.64647+01_00,3000-09-30T10_43_37.64647+01_00".format(dir_names[0])
        status, _, _ = self.backup_merge()
        if status:
            self.fail("Should not be able to merge by directory range where the end is in the future")

        self._verify_backup_directory_count(3)

        # test start before end
        self.backupset.date_range = "{0},{1}".format(dir_names[-1], dir_names[0])
        status, _, _ = self.backup_merge()
        if status:
            self.fail("Should not be able to merge by directory range where the start is after the end")

        self._verify_backup_directory_count(3)

        # test valid
        self.backupset.date_range = "{0},{1}".format(dir_names[0], dir_names[-1])
        status, _, _ = self.backup_merge()
        if not status:
            self.fail("Should not have failed to merge")

        self._verify_backup_directory_count(1)

    def test_info_while_other_task_runs(self):
        """
        Test that info can run at the same time as other backup tasks
        1. Load some data to the cluster
        2. Create a backup repository
        3. Start an async backup
        4. Constantly run info
        4. It should not expect error
        :return:
        """
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()

        # Test with backup
        backup_result = self.cluster.async_backup_cluster(backupset=self.backupset,
                                                          objstore_provider=self.objstore_provider,
                                                          resume=self.backupset.resume, purge=self.backupset.purge,
                                                          no_progress_bar=self.no_progress_bar,
                                                          cli_command_location=self.cli_command_location,
                                                          cb_version=self.cb_version)

        for i in range(10):
            _, err = self.backup_info(True)
            if err:
                self.fail("Should have been able to run at the same time as the backup")
            self.sleep(2)

        output = backup_result.result(timeout=200)
        self.assertTrue(self._check_output("Backup completed successfully", output),
                        "Backup failed with concurrent info")

        # Test with merge
        self._take_n_backups(5)
        merge_result = self.cluster.async_merge_cluster(backup_host=self.backupset.backup_host,
                                                        backups=self.backups,
                                                        start=1, end=5,
                                                        directory=self.backupset.directory,
                                                        name=self.backupset.name,
                                                        cli_command_location=self.cli_command_location)
        for i in range(10):
            _, err = self.backup_info(True)
            if err:
                self.fail("Should have been able to run at the same time as the merge")
            self.sleep(2)

        output = merge_result.result(timeout=200)
        self.assertTrue(self._check_output("Merge completed successfully", output),
                        "Merge failed while running info at the same time")

    def test_config_without_objstore_bucket(self):
        self.assertIsNotNone(self.objstore_provider, "Test requires an object store provider")
        temp_bucket = self.backupset.objstore_bucket
        self.backupset.objstore_bucket = "invalidbucket"
        output, _ = self.backup_create(del_old_backup=False)
        self.backupset.objstore_bucket = temp_bucket
        self.assertRegex(output[0].lower(), self.objstore_provider.not_found_error)

    def test_backup_without_objstore_bucket(self):
        self.assertIsNotNone(self.objstore_provider, "Test requires an object store provider")
        temp_bucket = self.backupset.objstore_bucket
        self.backupset.objstore_bucket = "invalidbucket"
        output, _ = self.backup_cluster()
        self.backupset.objstore_bucket = temp_bucket
        self.assertRegex(output[0].lower(), self.objstore_provider.not_found_error)

    def test_info_without_objstore_bucket(self):
        self.assertIsNotNone(self.objstore_provider, "Test requires an object store provider")
        temp_bucket = self.backupset.objstore_bucket
        self.backupset.objstore_bucket = "invalidbucket"
        output, _ = self.backup_info()
        error_string = ""
        if self.objstore_provider.schema_prefix() == "gs://":
            error_string = "bucket doesn't exist"
        elif self.objstore_provider.schema_prefix() == "s3://":
            error_string = f"remote archive does not exist"
        self.backupset.objstore_bucket = temp_bucket
        self.assertIn(error_string, output[0].lower())

    def test_restore_without_objstore_bucket(self):
        self.assertIsNotNone(self.objstore_provider, "Test requires an object store provider")
        temp_bucket = self.backupset.objstore_bucket
        self.backupset.objstore_bucket = "invalidbucket"
        self.restore_only = True
        output, _ = self.backup_restore()
        self.backupset.objstore_bucket = temp_bucket
        self.assertRegex(output[0].lower(), self.objstore_provider.not_found_error)

    def test_remove_without_objstore_bucket(self):
        self.assertIsNotNone(self.objstore_provider, "Test requires an object store provider")
        temp_bucket = self.backupset.objstore_bucket
        self.backupset.objstore_bucket = "invalidbucket"
        _, output, _ = self.backup_remove()
        self.backupset.objstore_bucket = temp_bucket
        self.assertRegex(output[0].lower(), self.objstore_provider.not_found_error)

    def test_config_create_multiple_repos_with_remove_staging_directory(self):
        self.assertIsNotNone(self.objstore_provider, "Test requires an object store provider")
        self.backup_create_validate()
        self.backupset.name = "another_repo"
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        self.objstore_provider._remove_staging_directory(remote_client.extract_remote_info().type.lower(), remote_client)
        self.backup_create_validate()

    def test_backup_with_remove_staging_directory(self):
        self.assertIsNotNone(self.objstore_provider, "Test requires an object store provider")
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        self.objstore_provider._remove_staging_directory(remote_client.extract_remote_info().type.lower(), remote_client)
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self.backup_create_validate()
        self._load_all_buckets(self.master, gen, "create")
        self.backup_cluster_validate()

    def test_info_with_remove_staging_directory(self):
        self.assertIsNotNone(self.objstore_provider, "Test requires an object store provider")
        self.backup_create_validate()
        self.backup_cluster_validate()
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        self.objstore_provider._remove_staging_directory(remote_client.extract_remote_info().type.lower(), remote_client)
        output, error = self.backup_info()
        if error:
            self.fail(f"Expected to be able to info backup where staging directory has been removed: {error}")
        self.assertEqual(json.loads(output[0])['count'], 1,
                         "Expected to find a single backup even though the staging directory was removed")

    def test_restore_with_remove_staging_directory(self):
        self.assertIsNotNone(self.objstore_provider, "Test requires an object store provider")
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create")
        self.backup_create_validate()
        self.backup_cluster_validate()
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        self.objstore_provider._remove_staging_directory(remote_client.extract_remote_info().type.lower(), remote_client)
        self.backup_restore_validate()

    def test_remove_with_remove_staging_directory(self):
        self.assertIsNotNone(self.objstore_provider, "Test requires an object store provider")
        self.backup_create_validate()
        self.backup_cluster_validate()
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        self.objstore_provider._remove_staging_directory(remote_client.extract_remote_info().type.lower(), remote_client)
        success, _, _ = self.backup_remove()
        self.assertTrue(success, "Expected to have removed backups even though the staging directory was removed")

    def test_restore_start_after_end(self):
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create")
        self.backup_create_validate()
        for _ in range(2):
            self.backup_cluster_validate()
        self.backupset.start = 2
        self.backupset.end = 1
        output, _ = self.backup_restore()
        self.assertEqual(len(output), 1)
        self.assertIn("range start", output[0])
        self.assertIn("cannot be before end", output[0])

    def test_restore_single_full_backup(self):
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create")
        self.backup_create_validate()
        self.backup_cluster_validate()
        self.backupset.start = 1
        self.backupset.end = 1
        self._all_buckets_flush()
        compare = ">=" if self.objstore_provider and self.objstore_provider.schema_prefix() == "gs://" else "=="
        self.backup_restore_validate(seqno_compare_function=compare)

    def test_restore_single_incr_backup(self):
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create")
        self.backup_create_validate()
        for _ in range(2):
            self._load_all_buckets(self.master, gen, "create")
            self.backup_cluster_validate()
        self.backupset.start = 2
        self.backupset.end = 2
        self._all_buckets_flush()
        self.backup_restore_validate(seqno_compare_function=">=")

    def test_start_full_end_incr(self):
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create")
        self.backup_create_validate()
        for _ in range(2):
            self._load_all_buckets(self.master, gen, "create")
            self.backup_cluster_validate()
        self.backupset.start = 1
        self.backupset.end = 2
        self._all_buckets_flush()
        self.backup_restore_validate(seqno_compare_function=">=")

    def test_start_incr_end_full(self):
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create")
        self.backup_create_validate()
        for _ in range(2):
            self.backup_cluster_validate()
        self._load_all_buckets(self.master, gen, "create")
        self.backupset.full_backup = True
        self.backup_cluster_validate()
        self.backupset.start = 2
        self.backupset.end = 3
        self._all_buckets_flush()
        self.backup_restore_validate(seqno_compare_function=">=")

    def test_cbbackup_with_big_rev(self):
        # automation ticket MB-38683
        # verified test failed in build 6.6.0-7680 and passed in 6.6.0-7685
        from ep_mc_bin_client import MemcachedClient, MemcachedError

        bucket = 'default'
        value = "value"
        expiry = 0
        rev_seq = 2**64-1
        key = 'test_with_meta'

        mc = MemcachedClient(self.master.ip, 11210)
        mc.sasl_auth_plain('Administrator', 'password')
        mc.bucket_select(bucket)
        self.log.info("pushing a key with large rev_seq {0} to bucket".format(rev_seq))

        try:
            mc.setWithMeta(key, 'value', 0, 0, rev_seq, 0x1512a3186faa0000)
            meta_key = mc.getMeta(key)
            self.log.info("key meta: {0}".format(meta_key))
        except MemcachedError as error:
            msg = "unable to push key : {0} error : {1}"
            self.log.error(msg.format(key, error.status))
            self.fail(msg.format(key, error.status))

        client = RemoteMachineShellConnection(self.backupset.backup_host)
        client.execute_command("rm -rf {0}/backup".format(self.tmp_path))
        client.execute_command("mkdir {0}backup".format(self.tmp_path))
        cmd = "{0}cbbackup{1} -u Administrator -p password http://{2}:8091 {3}backup"\
                .format(self.cli_command_location, self.cmd_ext, self.master.ip, self.tmp_path)
        try:
            cbbackup_run = False
            output, error = client.execute_command(cmd, timeout=20)
            cbbackup_run = True
            if not self._check_output("done", error):
                self.fail("Failed to run cbbackup with large rev_seq")
        except Exception as e:
            if e and not cbbackup_run:
                self.fail("Failed to run cbbackup with large rev_seq")
        finally:
            client.execute_command("rm -rf {0}/backup".format(self.tmp_path))
            client.disconnect()

    def test_backup_consistent_metadata(self):
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create")
        self.backup_create_validate()
        backup_threads = []
        backup_thread_1 = Thread(target=self.backup_cluster)
        backup_threads.append(backup_thread_1)
        backup_thread_1.start()
        backup_thread_2 = Thread(target=self.backup_cluster)
        backup_threads.append(backup_thread_2)
        backup_thread_2.start()
        for backup_thread in backup_threads:
            backup_thread.join()
        consistent_metadata = False
        for output in self.backup_outputs:
            if self._check_output("Error backing up cluster: failed to lock archive", output):
                consistent_metadata = True
        if not consistent_metadata:
            self.fail("Backup does not lock while running backup")

    def test_restore_consistent_metadata(self):
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create")
        self.backup_create_validate()
        self.backup_cluster()
        restore_threads = []
        restore_thread_1 = Thread(target=self.backup_restore)
        restore_threads.append(restore_thread_1)
        restore_thread_1.start()
        restore_thread_2 = Thread(target=self.backup_restore)
        restore_threads.append(restore_thread_2)
        self.create_bucket_count = 1
        restore_thread_2.start()
        count = 0
        for restore_thread in restore_threads:
            restore_thread.join()
        consistent_metadata = False
        for output in self.restore_outputs:
            if self._check_output("Error restoring cluster: failed to lock archive", output):
                consistent_metadata = True
                break
        if not consistent_metadata:
            self.fail("Restore does not lock while running restore")

    def test_info_backup_merge_remove(self, cluster, no_of_backups):
        """ Test Scenario: Create Buckets, Load Documents, Take 'no_of_backups' backups, Merge and Remove a Bucket

        This function creates a scenario in which:

        1. Buckets are created and loaded with documents.
        2. A variable number of Backups >=6 are taken.
        3. Backups 2 to 4 are merged.
        4. The 2nd last bucket from the end is removed.

        Args:
            cluster list: A list of 'ServerInfo' that form a cluster to backup.
            no_of_backups (int): The number of backups to perform.
        """
        # Add built-in user cbadminbucket to backup cluster
        self.add_built_in_server_user(node=self.backupset.cluster_host)

        # Assemble cluster if more than 1 node in cluster
        if len(cluster) > 1:
            self.cluster.async_rebalance(cluster, cluster[1:], []).result()

        # Take 'no_of_backups' backups
        self.backup_create()
        self._take_n_backups(n=no_of_backups)

        # Merge
        self.backupset.start, self.backupset.end = 2, 4
        self.backup_merge()

        # Delete a bucket
        self.backup_remove(self.backups.pop(-2), verify_cluster_stats=False)

    def test_magma_couchstore_compatibility(self):
        """ Test that couchstore and magma are compatible

        Backup couchstore > restore to magma
        Backup magma > restore to couchstore
        """
        restore_backend = "couchstore" if self.input.param("bucket_storage", "") == "magma" else "magma"

        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self.log.info("*** start to load items to all buckets")
        self._load_all_buckets(self.master, gen, "create", 0)
        self.log.info("*** done loading items to all buckets")

        self.backup_create_validate()
        self.backup_cluster_validate()

        # Tear down and replace bucket with opposite storage backend
        rest_client = RestConnection(self.master)
        rest_client.delete_bucket()
        rest_client.create_bucket(bucket="default", ramQuotaMB=256,
                                  storageBackend=restore_backend, replicaNumber=0)

        self.backup_restore_validate()

    def test_ee_only_features(self):
        """ Test that EE only features do not work on CE servers

        NOTE: PITR currently does nothing, so succeeds on CE.
        This should be included when PITR is added properly
        This is also true for:
        Backing up users,
        Auto rebuild of indexes

        Params:
            examine (bool): Whether to test examine.
            merge (bool): Whether to test merge.
            s3 (bool): Whether to test s3 cloud backup.
            consistency_check (bool): Whether to test consistency_check.
            coll_restore (bool): Whether to test collection/scope level restore.
        """
        examine = self.input.param('examine', False)
        merge = self.input.param('merge', False)
        s3 = self.input.param('s3', False)
        consistency_check = self.input.param('consistency_check', False)
        coll_restore = self.input.param('coll_restore', False)

        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = f"{self.cli_command_location}/cbbackupmgr"
        sub_command = ""

        self.backup_create()

        if examine:
            sub_command = 'examine -a archive -r repo -k asdf --collection-string asdf.asdf.asdf'
        elif merge:
            sub_command = 'merge -a archive -r repo'
        elif s3:
            sub_command = f'backup -a s3://backup -r {self.backupset.name}\
            -c {self.backupset.backup_host.ip}:{self.backupset.backup_host.port}\
            -u Administrator -p password'
        elif consistency_check:
            sub_command = f'backup -a {self.backupset.directory} -r {self.backupset.name}\
            -c {self.backupset.backup_host.ip}:{self.backupset.backup_host.port}\
            -u Administrator -p password --consistency-check 1'
        elif coll_restore:
            sub_command = f'restore -a {self.backupset.directory} -r {self.backupset.name}\
            -c {self.backupset.backup_host.ip}:{self.backupset.backup_host.port}\
            -u Administrator -p password --include-data asdf.asdf.asdf'

        if not sub_command:
            self.fail("Must provide a subcommand!")

        output, error = remote_client.execute_command(f"{command} {sub_command}")
        self.log.info(f"ERROR from command: {error}")
        self.log.info(f"OUTPUT from command: {output}")
        if s3 and "7.0.0" in self.cb_version:
            # The s3 error message differs slightly in 7.0.0
            self.assertIn("an enterprise only feature", output[0])
        else:
            self.assertIn("an Enterprise Edition feature", output[0])

    def test_negative_read_only_archive(self):
        # Load docs into cluster
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self.log.info("*** start to load items to all buckets")
        self._load_all_buckets(self.master, gen, "create", self.expires)
        self.log.info("*** done to load items to all buckets")

        # Enable SQLite if we are doing compaction
        self.backupset.sqlite = self.input.param("compact", False)

        # Create a backup and make it read-only
        self.backup_create_validate()
        self.backup_cluster_validate()

        # Make auth invalid if required
        if self.input.param("invalid_auth", False):
            self.backupset.cluster_host_username = "BADUSERNAME"
            self.backupset.cluster_host_password = "BADPASSWORD"

        # Compaction does not work on rift, so require parameter
        if self.input.param("compact", False):
            status, output, msg = self.backup_compact()
            self.assertIn("read-only", output[0])
        else:
            # Perform backup
            try:
                self.backup_cluster_validate()
            except Exception as e:
                if "read-only" not in str(e):
                    raise e
            # Perform merge
            status, output, msg = self.backup_merge()
            self.assertIn("read-only", output[0])
            # Perform remove
            status, output, msg = self.backup_remove()
            self.assertIn("read-only", output[0])

    def test_analytics_synonyms(self):
        """ Test analytics synonyms can be restored

        Params:
            dataverses (int): Number of dataverses to create.
            datasets (int): Number of datasets to create.
            synonyms (int): Number of synonyms to create.
        """
        class Query:
            """ A class to execute analytics queries """

            def __init__(self, server, username, password):
                self.restconn = RestConnection(server)

            def execute(self, query):
                return self.restconn.execute_statement_on_cbas(query, None)

            def get_synonyms(self):
                synonyms = set()

                for result in json.loads(self.execute("select * from Metadata.`Synonym`"))['results']:
                    synonym = result['Synonym']
                    synonym_name = synonym['SynonymName']
                    synonym_target = synonym['ObjectDataverseName'] + '.' + synonym['ObjectName']
                    synonym_dataverse = synonym['DataverseName']
                    synonyms.add((synonym_name, synonym_target, synonym_dataverse))

                return synonyms

            def get_synonyms_count(self):
                return json.loads(self.execute("select count(*) as count from Metadata.`Synonym`;"))['results'][0]['count']

        class Dataset:

            def __init__(self, name, bucket, clause=None):
                self.name, self.bucket, self.clause = name, bucket, clause

            def get_where_clause(self):
                return f" WHERE {self.clause}" if self.clause else ""

        class Synonym:

            def __init__(self, name, target):
                self.name, self.target = name, target

        class Dataverse:

            def __init__(self, name):
                self.name = name
                self.datasets = set()
                self.synonyms = set()

            def add_dataset(self, dataset):
                self.datasets.add(dataset)

            def add_synonym(self, synonym):
                self.synonyms.add(synonym)

            def next_dataset_name(self):
                return f"dat_{len(self.datasets)}"

            def next_synonym_name(self):
                return f"syn_{len(self.synonyms)}"

        class Analytics:

            def __init__(self, query):
                self.query, self.dataverses = query, set()

            def add_dataverse(self, dataverse):
                self.dataverses.add(dataverse)

            def next_dataverse_name(self):
                return f"dtv_{len(self.dataverses)}"

            def pick_target_for_synonym(self):
                choices =  [f"{dataverse.name}.{dataset.name}" for dataverse in self.dataverses for dataset in dataverse.datasets]

                if choices:
                    return choice(choices)

                return None

            def create(self):
                # Create daterverses and datasets
                for dataverse in self.dataverses:
                    self.query.execute(f"CREATE dataverse {dataverse.name}")

                    for dataset in dataverse.datasets:
                        self.query.execute(f"CREATE DATASET {dataverse.name}.{dataset.name} ON {dataset.bucket}{dataset.get_where_clause()}")

                # Create synonyms
                for dataverse in self.dataverses:
                    for synonym in dataverse.synonyms:
                        self.query.execute(f"CREATE analytics synonym {dataverse.name}.{synonym.name} FOR {synonym.target}")

            def delete(self):
                for dataverse in self.dataverses:
                    for dataset in dataverse.datasets:
                        self.query.execute(f"DROP DATASET {dataverse.name}.{dataset.name}")

                    for synonym in dataverse.synonyms:
                        self.query.execute(f"DROP analytics synonym {dataverse.name}.{synonym.name}")

                    self.query.execute(f"DROP dataverse {dataverse.name}")

        class AnalyticsTest:

            def __init__(self, backup, no_of_dataverses, no_of_datasets, no_of_synonyms, analytics_server):
                # The base class
                self.backup = backup

                # Test parameters
                self.no_of_dataverses, self.no_of_datasets, self.no_of_synonyms = no_of_dataverses, no_of_datasets, no_of_synonyms

                # The number of synonyms that get created
                self.no_of_synonyms_created = no_of_dataverses * no_of_synonyms

                # The object thats used to run queries on the server running analytics
                self.query = Query(analytics_server, analytics_server.rest_username, analytics_server.rest_password)

                # The object that represents our current model of analytics
                self.analytics = Analytics(self.query)

            def test_analytics(self):
                # Define the analytics model (i.e. which dataverses, datasets and synonyms are present)
                for i in range(self.no_of_dataverses):
                    dataverse = Dataverse(self.analytics.next_dataverse_name())
                    self.analytics.add_dataverse(dataverse)

                    for j in range(self.no_of_datasets):
                        dataset = Dataset(dataverse.next_dataset_name(), 'default')
                        dataverse.add_dataset(dataset)

                    for j in range(self.no_of_synonyms):
                        synonym = Synonym(dataverse.next_synonym_name(), self.analytics.pick_target_for_synonym())
                        dataverse.add_synonym(synonym)

                # Create dataverses, datasets and synonyms
                self.analytics.create()
                self.backup.assertEqual(self.query.get_synonyms_count(), self.no_of_synonyms_created)

                # Create a repository
                self.backup.backup_create()

                # Take a backup
                self.backup.backup_cluster()

                # Delete all analytics related stuff
                self.analytics.delete()
                self.backup.assertEqual(self.query.get_synonyms_count(), 0)

                # Perform a one off restore
                self.backup.backup_restore()
                synonyms = self.query.get_synonyms()

                # Check synonyms have been restored
                for dataverse in self.analytics.dataverses:
                    for synonym in dataverse.synonyms:
                        self.backup.assertIn((synonym.name, synonym.target, dataverse.name), synonyms)

        # The server that will be reprovisioned with analytics
        analytics_server = self.restore_cluster_host = self.servers[2]

        # Add a server and provision it with analytics
        self.add_server_with_custom_services(analytics_server, services=["cbas"])

        # A little sleep for services to warmup
        self.assertTrue(RestConnection(analytics_server).wait_until_cbas_is_ready(100))

        # Run the analytics test
        AnalyticsTest(self, self.input.param("dataverses", 5), self.input.param("datasets", 5), self.input.param("synonyms", 5), analytics_server).test_analytics()

    def test_info_after_backup_merge_remove(self):
        """ CBQE-5475: Test cbbackupmgr info comprehensively after performing backup, merge and remove

        Test params:
        flag_depth     = [0,1,2,3]
        check_tabular  = [True, False]
        check_all_flag = [True, False]
        dgm_run        = [True, False]
        sasl_buckets   >= 1

        Comprehensive test: flag_depth=3,check_tabular=True,check_all_flag=True,dgm_run=True,sasl_buckets=2

        Scenario:
        Perform backup, merge and remove to mutate info output.

        Cases tested:
        flag_depth>=0: --archive,
        flag_depth>=1: --archive --repo
        flag_depth>=2: --archive --repo --backup
        flag_depth>=3: --archive --repo --backup --collection-string in version>7.0/--bucket in version<=6.6

        Output types tested for each of the previous cases:
        check_tabular>=False: using --json flag (Checks JSON output)
        check_tabular = True:    no --json flag (Parses tabular output to reflect JSON output)

        State of all flag:
        check_all_flag>=False:
            using --all flag (e.g. for --archive --all checks all repos in archive, backups in repos, buckets in backups)
        check_all_flag = True:
            --all flag (e.g. for --archive checks contents of archive only)

        Total number of cases: 4 (cases) * 2 (output types) * 2 (all flag state) = 16
        """
        import os
        import pprint
        import itertools
        import parse_cbbackupmgr_info as parse_info

        pp = pprint.PrettyPrinter(indent=4)

        # Params
        flag_depth = self.input.param('flag_depth', 3)
        check_tabular = self.input.param('check_tabular', True)
        check_all_flag = self.input.param('check_all_flag', True)

        # The minimum number of backups is 6
        min_backups = 6
        no_of_backups = max(self.backupset.number_of_backups, min_backups)
        if self.backupset.number_of_backups < min_backups:
            self.log.warn("number_of_backups increased from {} to {}".format(self.backupset.number_of_backups, min_backups))

        # Select backup cluster
        cluster = [self.backupset.cluster_host]

        # Create Buckets, Load Documents, Take n backups, Merge and Remove a Bucket
        self.test_info_backup_merge_remove(cluster, no_of_backups)

        # Create lists of expected output from the info command
        types = set(['FULL', 'MERGE - FULL', 'MERGE - INCR', 'INCR'])
        expected_archs = [os.path.basename(self.backupset.directory)]
        expected_repos = [self.backupset.name]
        expected_backs = {self.backupset.name: self.backups}
        expected_bucks = [bucket.name for bucket in self.buckets]


        def check_arch(arch, tabular=False):
            """ Checks the archive dictionary.

            Args:
                arch (dict): A dictionary containing archive information.

            Returns:
                list: A list containing the repositories in the archive.

            """
            expected_keys = [u'archive_uuid', u'name', u'repos']
            self.assertTrue(set(expected_keys).issubset(list(arch.keys())), "Expected {} to be a subset of {}, in a unicode format".format(str(expected_keys), str(list(arch.keys()))))

            archive_uuid, name, repos = [arch[key] for key in expected_keys]

            # Check archive name is correct
            self.assertTrue(name in expected_archs)

            # Check repos names are correct
            self.assertEqual(set(expected_repos), set(repo['name'] for repo in repos))
            # Check repo size is > 0
            self.assertTrue(all(repo['size'] > 0 for repo in repos))
            # Check backup sizes are correct
            self.assertTrue(all(repo['count'] == len(expected_backs[repo['name']]) for repo in repos))

            return repos


        def check_repo(repo, tabular=False):
            """ Checks the repository dictionary.

            Args:
                repo (dict): A dictionary containing repository information.

            Returns:
                list: A list containing the backups in the repository.

            """
            expected_keys = [u'count', u'backups', u'name', u'size']
            self.assertTrue(set(expected_keys).issubset(repo.keys()))

            count, backups, name, size = [repo[key] for key in expected_keys]

            # Check repo name is correct
            self.assertTrue(name in expected_repos)
            # Check repo size is greater than 0
            self.assertTrue(size > 0)

            # Check number of backups is correct
            self.assertEqual(len(backups), len(expected_backs[name]))
            # Check backup names
            self.assertEqual(set(backup['date'] for backup in backups), set(expected_backs[name]))
            # Check backup types
            self.assertTrue(set(backup['type'] for backup in backups).issubset(types))
            # Check complete status
            self.assertTrue(all(backup['complete'] for backup in backups))

            return backups


        def check_back(backup, tabular=False):
            """ Checks the backup dictionary.

            Args:
                backup (dict): A dictionary containing backup information.

            Returns:
                list: A list containing the buckets in the backup.
            """
            expected_keys = [u'complete', u'fts_alias', u'buckets',
                    u'source_cluster_uuid', u'source', u'date', u'type', u'events', u'size']
            self.assertTrue(set(expected_keys).issubset(backup.keys()))

            complete, fts_alias, buckets, source_cluster_uuid, source, date, _type_, events, size = \
                    [backup[key] for key in expected_keys]

            # Check backup name is correct
            self.assertTrue(date in self.backups)
            # Check backup size is greater than 0
            self.assertTrue(size > 0)
            # Check type exists
            self.assertTrue(_type_ in types)

            # Check bucket names
            self.assertEqual(set(bucket['name'] for bucket in buckets), set(expected_bucks))
            # Check bucket sizes
            self.assertTrue(all(bucket['size'] >= 0 for bucket in buckets))
            # Check items are equal to self.num_items
            self.assertTrue(all(bucket['items'] in [0, self.num_items] for bucket in buckets))

            return buckets

        def check_buck(bucket, tabular=False):
            """ Checks the bucket dictionary.

            Args:
                bucket (dict): A dictionary containing bucket information.

            Returns:
                None

            """
            expected_keys = [u'index_count', u'views_count', u'items', u'mutations',
                             u'tombstones', u'fts_count', u'analytics_count', u'size', u'name']
            self.assertTrue(set(expected_keys).issubset(bucket.keys()))

            index_count, views_count, items, mutations, tombstones, fts_count, \
            analytics_count, size, name = [bucket[key] for key in expected_keys]

            # Check bucket name
            self.assertTrue(name in expected_bucks)
            # Check bucket size
            self.assertTrue(size >= 0)
            # Check bucket items
            self.assertTrue(items in [0, self.num_items])

        def print_tree(tree):
            if self.debug_logs:
                pp.pprint(tree)

        def parse_output(use_json, output):
            """ Parses the JSON/Tabular output into a Python dictionary

            Args:
                use_json (bool): If True expects JSON output to parse. Otherwise, expects tabular data to parse.
                output   (list): JSON or Tabular data to parse into a dictionary.

            Returns:
                dict: A dictionary containing the parsed output.

            """
            return json.loads(output[0]) if use_json else parse_info.construct_tree(output)

        # Configure initial flags
        json_options, all_flag_options = [True], [False]

        # Enable tabular output tests
        if check_tabular:
            json_options.append(False)

        # Enable all flag tests
        if check_all_flag:
            all_flag_options.append(True)

        def output_logs(flag_depth, use_json, all_flag):
            """ Outputs flags tested in current test case."""
            use_json = "--json" if use_json else ""
            all_flag = "--all" if all_flag else ""
            flags = " ".join(["--archive", "--repo", "--backup", "--bucket"][: flag_depth + 1])
            self.log.info("---")
            self.log.info(f"Testing Flags: {flags} {use_json} {all_flag}")
            self.log.info("---")

        # Perform tests
        for use_json, all_flag in itertools.product(json_options, all_flag_options):
            # Gilad: In case that we are testing without the --json flag, don't validate the output,
            # just verify there is no error. The output needs some parsing as it's just a print to the console,
            # and the values are the same as with the --json flag (unless something goes incredibly wrong),
            # so there is little value in adding additional complexity.

            output_logs(0, use_json, all_flag)
            # cbbackupmgr info --archive
            arch = parse_output(use_json, self.get_backup_info(json=use_json, all_flag=all_flag))

            print_tree(arch)
            if use_json:
                repos = check_arch(arch)

            if all_flag and use_json:
                [check_buck(buck) for repo in repos for back in check_repo(repo) for buck in check_back(back)]

            if flag_depth < 1:
                continue

            output_logs(1, use_json, all_flag)

            # cbbackupmgr info --archive --repo
            for repo_name in expected_repos:
                repo = parse_output(use_json, self.get_backup_info(json=use_json, repo=repo_name, all_flag=all_flag))
                print_tree(repo)
                if use_json:
                    backs = check_repo(repo)

                if all_flag and use_json:
                    [check_buck(buck) for back in backs for buck in check_back(back)]

            if flag_depth < 2:
                continue

            output_logs(2, use_json, all_flag)

            # cbbackupmgr info --archive --repo --backup
            for repo_name in expected_repos:
                for back_name in expected_backs[repo_name]:
                    back = parse_output(use_json, self.get_backup_info(json=use_json, repo=repo_name, backup=back_name, all_flag=all_flag))
                    print_tree(back)
                    if use_json:
                        bucks = check_back(back)

                    if all_flag and use_json:
                        [check_buck(buck) for buck in bucks]

            if flag_depth < 3:
                continue

            output_logs(3, use_json, all_flag)

            # cbbackupmgr info --archive --repo --backup --bucket
            for repo_name in expected_repos:
                for back_name in expected_backs[repo_name]:
                    for buck_name in expected_bucks:
                        buck = parse_output(use_json, self.get_backup_info(json=use_json, repo=repo_name,
                                            backup=back_name, collection_string=buck_name, all_flag=all_flag))
                        print_tree(buck)
                        if use_json:
                            check_buck(buck)
