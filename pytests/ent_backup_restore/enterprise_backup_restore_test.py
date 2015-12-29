from random import randrange

from couchbase_helper.cluster import Cluster
from couchbase_helper.documentgenerator import BlobGenerator
from ent_backup_restore.enterprise_backup_restore_base import EnterpriseBackupRestoreBase
from membase.api.rest_client import RestConnection


class EnterpriseBackupRestoreTest(EnterpriseBackupRestoreBase):
    def setUp(self):
        super(EnterpriseBackupRestoreTest, self).setUp()

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
        self._load_all_buckets(self.master, gen, "create", self.expires)
        self.ops_type = self.input.param("ops-type", "update")
        if self.auto_failover:
            self.log.info("Enabling auto failover on " + str(self.backupset.cluster_host))
            rest_conn = RestConnection(self.backupset.cluster_host)
            rest_conn.update_autofailover_settings(self.auto_failover, self.auto_failover_timeout)
        self.backup_create_validate()
        for i in range(1, self.backupset.number_of_backups + 1):
            if self.ops_type == "update":
                self._load_all_buckets(self.master, gen, "update", self.expires)
            elif self.ops_type == "delete":
                self._load_all_buckets(self.master, gen, "delete", self.expires)
            self.backup_cluster_validate()
        self.targetMaster = True
        start = randrange(1, self.backupset.number_of_backups + 1)
        if start == self.backupset.number_of_backups:
            end = start
        else:
            end = randrange(start, self.backupset.number_of_backups + 1)
        restored = {"{0}/{1}".format(start, end): ""}
        for i in range(1, self.backupset.number_of_backups + 1):
            if self.reset_restore_cluster:
                self.backup_reset_clusters(self.cluster_to_restore)
                if self.same_cluster:
                    self._initialize_nodes(Cluster(), self.servers[:self.nodes_init])
                else:
                    self._initialize_nodes(Cluster(), self.input.clusters[0][:self.nodes_init])
            self.backupset.start = start
            self.backupset.end = end
            self.backup_restore_validate(compare_uuid=False, seqno_compare_function=">=")
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
        self._load_all_buckets(self.master, gen, "create", 0)
        self.ops_type = self.input.param("ops-type", "update")
        self.backup_create()
        for i in range(1, self.backupset.number_of_backups + 1):
            self._backup_restore_with_ops()
        start = randrange(1, self.backupset.number_of_backups + 1)
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
                start = randrange(1, self.backupset.number_of_backups + 1)
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
