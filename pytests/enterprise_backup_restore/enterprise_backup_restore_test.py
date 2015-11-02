from couchbase_helper.documentgenerator import BlobGenerator
from enterprise_backup_restore.enterprise_backup_base import EnterpriseBackupRestoreBase
from enterprise_backup_restore.validation_helpers.valdation_base import ValidationBase
from membase.api.rest_client import RestConnection, Bucket


class EnterpriseBackupRestoreTest(EnterpriseBackupRestoreBase):
    def setUp(self):
        super(EnterpriseBackupRestoreTest, self).setUp()

    def tearDown(self):
        super(EnterpriseBackupRestoreTest, self).tearDown()

    def test_enterprise_backup_restore_sanity(self):
        #nodes_in = self.servers[self.nodes_init + 1:self.nodes_init + 2]
        #opt_nodes = [self.servers[0]] + self.servers[self.nodes_init + 1: self.nodes_init + 2]
        #self.cluster.create_default_bucket(self.servers[0], 100)
        self.buckets.append(Bucket(name="default", authType="sasl", saslPassword="",
                                   num_replicas=1, bucket_size=100))
        #self.cluster.rebalance(opt_nodes, nodes_in, [])
        gen = BlobGenerator('mike', 'mike-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen, "create", 0)
        status, output, message = self.backup_create()
        if not status:
            self.fail(message)
        status, output, message = self.backup_cluster()
        if not status:
            self.fail(message)
        self.log.info(message)
        rest = RestConnection(self.servers[0])
        buckets = rest.get_buckets()
        bucketnames = []
        for bucket in buckets:
            bucketname = "{0}-{1}".format(bucket.name, bucket.uuid)
            bucketnames.append(bucketname)
        self.backupset.buckets_list(bucketnames)
        validation_helper = ValidationBase(self.backupset)
        status, message = validation_helper.validate_backup()
        if not status:
            self.fail(message)
        self.log.info(message)
        self._load_all_buckets(self.master, gen, "update", 0)
        self.backup_restore()
        status, message = validation_helper.validate_restore()
        if not status:
            self.fail(message)
        self.log.info(message)
