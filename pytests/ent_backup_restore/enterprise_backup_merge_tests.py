from couchbase_helper.cluster import Cluster
from ent_backup_restore.enterprise_backup_restore_base import EnterpriseBackupMergeBase
from remote.remote_util import RemoteMachineShellConnection


class EnterpriseBackupMergeTest(EnterpriseBackupMergeBase):
    def setUp(self):
        super(EnterpriseBackupMergeTest, self).setUp()
        for server in [self.backupset.backup_host,
                       self.backupset.restore_cluster_host]:
            conn = RemoteMachineShellConnection(server)
            conn.extract_remote_info()
            conn.terminate_processes(conn.info, ["cbbackupmgr"])

    def tearDown(self):
        super(EnterpriseBackupMergeTest, self).tearDown()

    def test_multiple_backups_merges(self):
        self.log.info("*** start to load items to all buckets")
        self.expected_error = self.input.param("expected_error", None)
        if self.expires:
            for bucket in self.buckets:
                cb = self._get_python_sdk_client(self.master.ip, bucket, self.backupset.cluster_host)
                for i in range(1, self.num_items + 1):
                    cb.upsert("doc" + str(i), {"key":"value"})
        else:
            self._load_all_buckets(self.master, self.initial_load_gen,
                               "create", self.expires)
        self.log.info("*** done to load items to all buckets")
        self.backup_create_validate()
        for i in range(1, self.number_of_repeats + 1):
            self.do_backup_merge_actions()
        start = self.number_of_backups_taken
        end = self.number_of_backups_taken
        if self.reset_restore_cluster:
            self.log.info("*** start to reset cluster")
            self.backup_reset_clusters(self.cluster_to_restore)
            if self.same_cluster:
                self._initialize_nodes(Cluster(),
                                       self.servers[:self.nodes_init])
            else:
                self._initialize_nodes(Cluster(), self.input.clusters[0][
                                                  :self.nodes_init])
            self.log.info("Done reset cluster")
        self.sleep(10)
        """ Add built-in user cbadminbucket to second cluster """
        self.add_built_in_server_user(
            node=self.input.clusters[0][:self.nodes_init][0])

        self.backupset.start = start
        self.backupset.end = end
        self.log.info("*** start restore validation")
        self.backup_restore_validate(compare_uuid=False,
                                     seqno_compare_function=">=",
                                     expected_error=self.expected_error)

    def test_multiple_backups_merge_with_tombstoning(self):
        self.log.info("*** start to load items to all buckets")
        self.expected_error = self.input.param("expected_error", None)
        if self.expires:
            for bucket in self.buckets:
                cb = self._get_python_sdk_client(self.master.ip, bucket)
                for i in range(1, self.num_items + 1):
                    cb.upsert("doc" + str(i), {"key": "value"})
        else:
            self._load_all_buckets(self.master, self.initial_load_gen,
                                   "create", self.expires)
        self.log.info("*** done to load items to all buckets")
        self.backup_create_validate()
        self.backup()
        self.set_meta_purge_interval()
        self._load_all_buckets(self.master, self.delete_gen, "delete",
                               self.expires)
        self.sleep(360, "Sleep for 6 minutes for the meta-data purge "
                        "interval to be completed")
        self.compact_buckets()
        self.backup()
        self.backupset.start = 1
        self.backupset.end = len(self.backups)
        self.merge()
        start = self.number_of_backups_taken
        end = self.number_of_backups_taken
        if self.reset_restore_cluster:
            self.log.info("*** start to reset cluster")
            self.backup_reset_clusters(self.cluster_to_restore)
            if self.same_cluster:
                self._initialize_nodes(Cluster(),
                                       self.servers[:self.nodes_init])
            else:
                self._initialize_nodes(Cluster(), self.input.clusters[0][
                                                  :self.nodes_init])
            self.log.info("Done reset cluster")
        self.sleep(10)
        self.backupset.start = start
        self.backupset.end = end
        self.log.info("*** start restore validation")
        self.backup_restore_validate(compare_uuid=False,
                                     seqno_compare_function=">=",
                                     expected_error=self.expected_error)
