from random import randrange

from couchbase_helper.cluster import Cluster
from couchbase_helper.documentgenerator import BlobGenerator
from enterprise_backup_restore.enterprise_backup_base import EnterpriseBackupRestoreBase
from membase.api.rest_client import RestConnection


class EnterpriseBackupRestoreTest(EnterpriseBackupRestoreBase):
    def setUp(self):
        super(EnterpriseBackupRestoreTest, self).setUp()

    def tearDown(self):
        super(EnterpriseBackupRestoreTest, self).tearDown()

    def test_enterprise_backup_restore_sanity(self):
        nodes_in = self.servers[self.nodes_init + 1:self.nodes_init + 2]
        opt_nodes = [self.servers[0]] + self.servers[self.nodes_init + 1: self.nodes_init + 2]
        self.cluster.rebalance(opt_nodes, nodes_in, [])
        gen = BlobGenerator('mike', 'mike-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        self.backup_cluster()
        prev_seq_no = self.get_vbucket_seqnos(opt_nodes, self.buckets)
        rest = RestConnection(self.servers[0])
        buckets = rest.get_buckets()
        bucketnames = []
        for bucket in buckets:
            bucketname = "{0}-{1}".format(bucket.name, bucket.uuid)
            bucketnames.append(bucketname)
        self.backupset.buckets_list(bucketnames)
        self._load_all_buckets(self.master, gen, "update", 0)
        self.withMutationOps = False
        self.backup_restore()
        cur_seq_no = self.get_vbucket_seqnos(opt_nodes, self.buckets)
        status, msg = self.validation_helper.compare_vbucket_stats(prev_seq_no, cur_seq_no, compare_uuid=True,
                                                                   seqno_compare="<=")
        if status:
            self.log.info(msg)

    def test_backup_create(self):
        self.backup_create()

    def test_backup_restore_sanity(self):
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.ops_type = self.input.param("ops-type", "update")
        self.backup_create()
        backup_vseqno = []
        for i in range(1, self.number_of_backups + 1):
            if self.ops_type == "update":
                self._load_all_buckets(self.master, gen, "update", 0)
            elif self.ops_type == "delete":
                self._load_all_buckets(self.master, gen, "delete", 0)
            self.backup_cluster()
            vseqno = self.get_vbucket_seqnos(self.servers[:self.nodes_init], self.buckets)
            backup_vseqno.append(vseqno)
        self.reset_cluster()
        self._initialize_nodes(Cluster(), self.servers[:self.nodes_init])
        if self.number_of_backups == 1:
            self.backup_restore()
            vseqno = self.get_vbucket_seqnos(self.servers[:self.nodes_init], self.buckets)
            status, msg = self.validation_helper.compare_vbucket_stats(backup_vseqno[0], vseqno, compare_uuid=True,
                                                                       seqno_compare="<=")
            if status:
                self.log.info(msg)
        else:
            start = randrange(0, self.number_of_backups) + 1
            if start == self.number_of_backups:
                end = start
            else:
                end = randrange(start, self.number_of_backups + 1)
            restored = {"{0}/{1}".format(start, end): ""}
            for i in range(1, self.number_of_backups + 1):
                self.backupset.start = start
                self.backupset.end = end
                self.backup_restore()
                vseqno = self.get_vbucket_seqnos(self.servers[:self.nodes_init], self.buckets)
                status, msg = self.validation_helper.compare_vbucket_stats(backup_vseqno[end - 1], vseqno,
                                                                           compare_uuid=True, seqno_compare="<=")
                if status:
                    self.log.info(msg)
                while "{0}/{1}".format(start, end) in restored:
                    start = randrange(0, self.number_of_backups) + 1
                    if start == self.number_of_backups:
                        end = start
                    else:
                        end = randrange(start, self.number_of_backups + 1)
                restored["{0}/{1}".format(start, end)] = ""

    def test_backup_restore_with_mutations_ops(self):
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.ops_type = self.input.param("ops-type", "update")
        self.backup_create()
        backup_seqno = []
        for i in range(1, self.number_of_backups + 1):
            self.backup_restore_with_ops()
            backup_seqno.append(self.get_vbucket_seqnos(self.servers[:self.nodes_init], self.buckets))
        start = randrange(0, self.number_of_backups) + 1
        if start == self.number_of_backups:
            end = start
        else:
            end = randrange(start, self.number_of_backups + 1)
        restored = {"{0}/{1}".format(start, end): ""}
        for i in range(1, self.number_of_backups + 1):
            self.backupset.start = start
            self.backupset.end = end
            self.backup_restore_with_ops("restore")
            restored_seqno = self.get_vbucket_seqnos(self.servers[:self.nodes_init], self.buckets)
            status, msg = self.validation_helper.compare_vbucket_stats(backup_seqno[end - 1], restored_seqno,
                                                                       compare_uuid=True, seqno_compare="<=")
            if status:
                self.log.info(msg)
            if self.number_of_backups == 1:
                continue
            while "{0}/{1}".format(start, end) in restored:
                start = randrange(0, self.number_of_backups) + 1
                if start == self.number_of_backups:
                    end = start
                else:
                    end = randrange(start, self.number_of_backups + 1)
            restored["{0}/{1}".format(start, end)] = ""

    def test_backup_restore_while_rebalance(self):
        serv_in = self.servers[self.nodes_init:self.nodes_init + self.nodes_in]
        serv_out = self.servers[self.nodes_init - self.nodes_out:self.nodes_init]
        gen = BlobGenerator("ent-backup", "ent-backup", self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        self.backup_create()
        for i in range(1, self.number_of_backups + 1):
            task = self.cluster.async_rebalance(self.servers,
                                                serv_in, serv_out)
            self.sleep(10)
            self.backup_cluster()
            task.result()
        self.reset_cluster()
        self._initialize_nodes(Cluster(), self.servers[:self.nodes_init])
        self._load_all_buckets(self.master, gen, "create", 0)
        start = randrange(0, self.number_of_backups) + 1
        if start == self.number_of_backups:
            end = start
        else:
            end = randrange(start, self.number_of_backups + 1)
        restored = {"{0}/{1}".format(start, end): ""}
        for i in range(1, self.number_of_backups):
            self.backupset.start = start
            self.backupset.end = end
            self.backup_restore_with_ops()
            if self.number_of_backups == 1:
                continue
            while "{0}/{1}".format(start, end) in restored:
                start = randrange(0, self.number_of_backups) + 1
                if start == self.number_of_backups:
                    end = start
                else:
                    end = randrange(start, self.number_of_backups + 1)
            restored["{0}/{1}".format(start, end)] = ""

    def backup_restore_with_ops(self, type="backup"):
        gen = BlobGenerator("ent-backup", "ent-backup-", self.value_size, end=self.num_items)
        if self.ops_type == "update":
            tasks = self._async_load_all_buckets(self.master, gen, self.ops_type, 0)
        if type is "backup":
            self.backup_cluster()
        elif type is "restore":
            self.backup_restore()
        for task in tasks:
            task.result()

    def test(self):
        new_cluster_nodes = self.servers[self.nodes_init + 1:self.nodes_init + 2]
        cluster = Cluster()
        self._initialize_nodes(cluster, new_cluster_nodes)
        initial_seq_no = self.get_vbucket_seqnos([self.servers[0]], self.buckets)
        gen = BlobGenerator('mike', 'mike-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        seq_no_1 = self.get_vbucket_seqnos([self.servers[0]], self.buckets)
        self.backup_create()
        self.backup_cluster()
        rest = RestConnection(self.servers[0])
        buckets = rest.get_buckets()
        bucketnames = []
        bucket_list = []
        for bucket in buckets:
            bucketname = "{0}-{1}".format(bucket.name, bucket.uuid)
            bucketnames.append(bucketname)
            bucket_list.append(bucket.name)
        self.backupset.buckets_list(bucketnames)
        self.backupset.restore_host = new_cluster_nodes[0]
        self.backupset.restore_host_username = new_cluster_nodes[0].rest_username
        self.backupset.restore_host_password = new_cluster_nodes[0].rest_password
        self._create_buckets(new_cluster_nodes[0], bucket_list)
        self.backup_restore()
        restore_client = RestConnection(new_cluster_nodes[0])
        restored_seq_no = self.get_vbucket_seqnos(new_cluster_nodes, restore_client.get_buckets())
        status, msg = self.validation_helper.compare_vbucket_stats(seq_no_1, restored_seq_no)
        if status:
            self.log.info(msg)
