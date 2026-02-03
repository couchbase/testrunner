from random import randrange
import json
from couchbase_helper.cluster import Cluster
from couchbase_helper.documentgenerator import BlobGenerator, DocumentGenerator
from ent_backup_restore.enterprise_backup_restore_base import EnterpriseBackupRestoreBase
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from remote.remote_util import RemoteUtilHelper, RemoteMachineShellConnection
from upgrade.newupgradebasetest import NewUpgradeBaseTest


class EnterpriseBackupRestoreBWCTest(EnterpriseBackupRestoreBase, NewUpgradeBaseTest):
    def setUp(self):
        super(EnterpriseBackupRestoreBWCTest, self).setUp()
        """ This test needs latest_bkrs_version and bwc_version params to run """
        """ Get cb version of cluster """
        self.bk_cluster_version = \
                        RestConnection(self.backupset.cluster_host).get_nodes_version()
        self.bk_cluster_version = "-".join(self.bk_cluster_version.split('-')[:2])
        self.latest_bkrs_version = self.input.param("latest_bkrs_version", None)
        if self.latest_bkrs_version is None:
            self.fail("We need to pass param branch version to run test.")

        self.backupset.backup_host = self.input.bkrs_client
        self.backupset.bkrs_client_upgrade = self.input.param("bkrs_client_upgrade", False)
        self.backupset.bwc_version = self.input.param("bwc_version", None)
        if self.backupset.bwc_version is None:
            self.fail("\nTest needs to get backward compatible version to run test")


        """ Get cb version of cbm client from bkrs client server """
        self.backupset.current_bkrs_client_version = \
                        self._get_current_bkrs_client_version()

        """ Match current bkrs client version in client server with bkrs version
            we need to test
        """
        self.backupset.bkrs_client_version = \
                                  self.input.param("bkrs_with_cbm_version", None)
        if self.backupset.bkrs_client_version is None:
            self.backupset.bkrs_client_version = self.backupset.current_bkrs_client_version

        self.users_check_restore = \
              self.input.param("users-check-restore", '').replace("ALL", "*").split(";")
        if '' in self.users_check_restore:
            self.users_check_restore.remove('')
        """ Terminate all cbm in bkrs client """
        for server in [self.backupset.backup_host]:
            conn = RemoteMachineShellConnection(server)
            conn.extract_remote_info()
            conn.terminate_processes(conn.info, ["cbbackupmgr"])
            conn.disconnect()

        self.bucket_helper = BucketOperationHelper()
        if self.backupset.bkrs_client_upgrade:
            if self.backupset.current_bkrs_client_version[:5] > self.backupset.bwc_version[:5]:
                self.log.info("\n**** Need to downgrade CBM version to {0}"\
                                                    .format(self.backupset.bwc_version))
                self._install([self.backupset.backup_host], self.backupset.bwc_version)
                self.backupset.current_bkrs_client_version = \
                                            self._get_current_bkrs_client_version()
        elif self.backupset.current_bkrs_client_version[:3] < "6.5":
            self.log.info("\n**** Need to bring cbm match with request version")
            self._install([self.backupset.backup_host], self.backupset.bkrs_client_version)

        """ check cluster version """
        if self.bk_cluster_version != self.backupset.bwc_version:
            self.log.info("Need to bring cluster to test version")
            """ set "installParameters": "fts_query_limit=10000000,enable_ipv6=True,init_nodes=False",   in json test suite"""

    def tearDown(self):
        super(EnterpriseBackupRestoreBWCTest, self).tearDown()

    def _get_current_bkrs_client_version(self):
        self.backupset.current_bkrs_client_version = \
                        RestConnection(self.backupset.backup_host).get_nodes_version()
        self.backupset.current_bkrs_client_version = \
                   "-".join(self.backupset.current_bkrs_client_version.split('-')[:2])
        return self.backupset.current_bkrs_client_version

    def test_backup_create(self):
        self.backup_create_validate()

    def test_backup_restore_sanity_bwc(self):
        """
        1. Create default bucket on the cluster and loads it with given number of items
        2. Perform updates and create backups for number of times (param number_of_backups)
           If bkrs client needs to upgrade, test will perform upgrade bkrs client version
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
            if self.backupset.bkrs_client_upgrade:
                self.backupset.current_bkrs_client_version = \
                    self._get_current_bkrs_client_version()
                if i == 3 and self.backupset.number_of_backups >= 5 and \
                    self.backupset.current_bkrs_client_version[:5] == self.bk_cluster_version[:5]:
                    self.log.info("\nNeed to upgrade CBM version to {0} to run bkrs bwc upgrade"\
                                      .format(self.latest_bkrs_version))
                    self._install([self.backupset.backup_host],
                                  self.latest_bkrs_version)
            self.log.info("*** start to validate backup cluster")
            self.backup_cluster_validate()
            i += 1

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
                    self._initialize_nodes(Cluster(), self.input.clusters[0][:self.nodes_init])
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

    def _take_n_backups(self, n=1, validate=False):
        for i in range(1, n + 1):
            if validate:
                self.backup_cluster_validate()
            else:
                self.backup_cluster()

    def test_merge_backup_from_old_and_new_bucket_bwc(self):
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
            self.log.error(output)
            self.fail(message)
        self.log.info("Start to merge backup")
        self.backupset.start = randrange(1, self.backupset.number_of_backups)
        self.backupset.end = randrange(self.backupset.start,
                                       self.backupset.number_of_backups + 1)
        self.merged = True
        result, output, _ = self.backup_merge()
        self.backupset.end -= 1
        status, output, message = self.backup_list()
        if not status:
            self.log.error(output)
            self.fail(message)
        current_vseqno = self.get_vbucket_seqnos(self.cluster_to_backup, self.buckets,
                                                 self.skip_consistency, self.per_node)
        self.log.info("*** Start to validate data in merge backup ")
        self.validate_backup_data(self.backupset.backup_host, [self.master],
                                  "ent-backup", False, False, "memory",
                                  self.num_items, "ent-backup1")
        self.backup_cluster_validate(skip_backup=True)

    def test_restore_with_filter_regex_bwc(self):
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
            gen = DocumentGenerator('random_keys', '{{"age": {0}}}', xrange(100),
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

    def test_backup_and_restore_with_map_buckets_bwc(self):
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
        if self.create_gsi:
            self.verify_gsi()

    def test_backup_merge_bwc(self):
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
            self.log.error(output)
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
        status, output, message = self.backup_merge()
        if not status:
            self.log.error(output)
            self.fail(message)
        status, output, message = self.backup_list()
        if not status:
            self.log.error(output)
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

    def test_backup_merge_with_restore_bwc(self):
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
            self.log.error(output)
            self.fail(message)
        self.backupset.start = 1
        self.backupset.end = 1
        output, error = self.backup_restore()
        if error:
            self.fail("Restoring backup failed")
        self.log.info("Finished restoring backup after merging")

    def test_cbbackupmgr_restore_with_ttl_bwc(self):
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

    def test_cbbackupmgr_restore_with_vbuckets_filter_bwc(self):
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
