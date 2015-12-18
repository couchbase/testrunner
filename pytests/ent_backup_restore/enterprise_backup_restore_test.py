from random import randrange

from couchbase_helper.cluster import Cluster
from couchbase_helper.documentgenerator import BlobGenerator
from ent_backup_restore.enterprise_backup_restore_base import EnterpriseBackupRestoreBase


class EnterpriseBackupRestoreTest(EnterpriseBackupRestoreBase):
    def setUp(self):
        super(EnterpriseBackupRestoreTest, self).setUp()

    def tearDown(self):
        super(EnterpriseBackupRestoreTest, self).tearDown()

    def test_backup_create(self):
        self.backup_create_validate()

    def test_backup_restore_sanity(self):
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.ops_type = self.input.param("ops-type", "update")
        self.backup_create_validate()
        for i in range(1, self.backupset.number_of_backups + 1):
            if self.ops_type == "update":
                self._load_all_buckets(self.master, gen, "update", 0)
            elif self.ops_type == "delete":
                self._load_all_buckets(self.master, gen, "delete", 0)
            self.backup_cluster_validate()
        self.targetMaster = True
        self.backup_reset_clusters(self.cluster_to_backup)
        self._initialize_nodes(Cluster(), self.servers[:self.nodes_init])
        start = randrange(0, self.backupset.number_of_backups) + 1
        if start == self.backupset.number_of_backups:
            end = start
        else:
            end = randrange(start, self.backupset.number_of_backups + 1)
        restored = {"{0}/{1}".format(start, end): ""}
        for i in range(1, self.backupset.number_of_backups + 1):
            self.backup_reset_clusters(self.cluster_to_backup)
            self._initialize_nodes(Cluster(), self.servers[:self.nodes_init])
            self.backupset.start = start
            self.backupset.end = end
            self.backup_restore_validate(compare_uuid=False, seqno_compare_function=">=")
            if self.backupset.number_of_backups == 1:
                continue
            while "{0}/{1}".format(start, end) in restored:
                start = randrange(0, self.backupset.number_of_backups) + 1
                if start == self.backupset.number_of_backups:
                    end = start
                else:
                    end = randrange(start, self.backupset.number_of_backups + 1)
            restored["{0}/{1}".format(start, end)] = ""

    def test_backup_restore_with_rebalance(self):
        serv_in = self.servers[self.nodes_init:self.nodes_init + self.nodes_in]
        serv_out = self.servers[self.nodes_init - self.nodes_out:self.nodes_init]
        gen = BlobGenerator("ent-backup", "ent-backup", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create_validate()
        self.backupset.number_of_backups = 1
        self.cluster.async_rebalance(self.cluster_to_backup, serv_in, serv_out)
        self.sleep(10, "Waiting for 10 sec for rebalance to be in progress before tyring to take backup")
        self.backup_cluster_validate()
        if self.same_cluster:
            self.backup_reset_clusters(self.cluster_to_backup)
            self._initialize_nodes(Cluster(), self.servers[:self.nodes_init])
            compare_uuid = False
        else:
            self._initialize_nodes(Cluster(), self.input.clusters[0][:self.nodes_init])
            serv_in = self.input.clusters[0][self.nodes_init: self.nodes_init + self.nodes_in]
            serv_out = self.input.clusters[0][self.nodes_init - self.nodes_out: self.nodes_init]
            compare_uuid = False
        self.cluster.async_rebalance(self.cluster_to_restore, serv_in, serv_out)
        self.backup_restore_validate(compare_uuid=compare_uuid, seqno_compare_function="<=")

    def test_backup_restore_with_ops(self):
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.ops_type = self.input.param("ops-type", "update")
        self.backup_create()
        for i in range(1, self.backupset.number_of_backups + 1):
            self._backup_restore_with_ops()
        start = randrange(0, self.backupset.number_of_backups) + 1
        if start == self.backupset.number_of_backups:
            end = start
        else:
            end = randrange(start, self.backupset.number_of_backups + 1)
        restored = {"{0}/{1}".format(start, end): ""}
        for i in range(1, self.backupset.number_of_backups + 1):
            self.backupset.start = start
            self.backupset.end = end
            self._backup_restore_with_ops(backup=False)
            if self.backupset.number_of_backups == 1:
                continue
            while "{0}/{1}".format(start, end) in restored:
                start = randrange(0, self.backupset.number_of_backups) + 1
                if start == self.backupset.number_of_backups:
                    end = start
                else:
                    end = randrange(start, self.backupset.number_of_backups + 1)
            restored["{0}/{1}".format(start, end)] = ""

    def _backup_restore_with_ops(self, exp=0, backup=True, compare_uuid=False, compare_function="==", replicas=False,
                                 mode="memory"):
        self.ops_type = self.input.param("ops-type", "update")
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, self.ops_type, exp)
        if backup:
            self.backup_cluster_validate()
        else:
            self.backup_restore_validate(compare_uuid=compare_uuid, seqno_compare_function=compare_function,
                                         replicas=replicas, mode=mode)