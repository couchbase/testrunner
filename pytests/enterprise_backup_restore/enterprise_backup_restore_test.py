from couchbase_helper.cluster import Cluster
from couchbase_helper.documentgenerator import BlobGenerator
from enterprise_backup_restore.enterprise_backup_base import EnterpriseBackupRestoreBase
from enterprise_backup_restore.validation_helpers.valdation_base import ValidationBase
from membase.api.rest_client import RestConnection
from memcached.helper.data_helper import VBucketAwareMemcached


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
        status, output, msg = self.backup_create()
        if not status:
            self.fail(msg)
        status, output, msg = self.backup_cluster()
        if not status:
            self.fail(msg)
        self.log.info(msg)
        rest = RestConnection(self.servers[0])
        buckets = rest.get_buckets()
        bucketnames = []
        for bucket in buckets:
            client = VBucketAwareMemcached(RestConnection(self.master), bucket)
            bucketname = "{0}-{1}".format(bucket.name, bucket.uuid)
            bucketnames.append(bucketname)
            temp = bucket.kvs[1]
            valid_key, deleted_keys = temp.key_set()
            valid_keys = client.getMulti(valid_key)
            valid = {key: list(valid_keys[key])[2:] for key in valid_keys}
        self.backupset.buckets_list(bucketnames)
        status, msg = self.validation_helper.validate_backup()
        if not status:
            self.fail(msg)
        self.log.info(msg)
        self._load_all_buckets(self.master, gen, "update", 0)
        self.backup_restore()
        status, msg = self.validation_helper.validate_restore()
        if not status:
            self.fail(msg)
        self.log.info(msg)

    def test_cluster_backup_restore(self):
        new_cluster_nodes = self.servers[self.nodes_init + 1:self.nodes_init + 2]
        cluster = Cluster()
        self._initialize_nodes(cluster, new_cluster_nodes)
        status, output, msg = self.backup_create()
        if not status:
            self.fail(msg)
        self.log.info(msg)
        status, output, msg = self.backup_cluster()
        if not status:
            self.fail(msg)
        self.log.info(msg)
        rest = RestConnection(self.servers[0])
        buckets = rest.get_buckets()
        bucketnames = []
        bucket_list = []
        for bucket in buckets:
            bucketname = "{0}-{1}".format(bucket.name, bucket.uuid)
            bucketnames.append(bucketname)
            bucket_list.append(bucket.name)
        self.backupset.buckets_list(bucketnames)
        validation_helper = ValidationBase(self.backupset)
        status, msg = validation_helper.validate_backup()
        if not status:
            self.fail(msg)
        self.log.info(msg)
        self.backupset.restore_host = new_cluster_nodes[0]
        self.backupset.restore_host_username = new_cluster_nodes[0].rest_username
        self.backupset.restore_host_password = new_cluster_nodes[0].rest_password
        self._create_buckets(new_cluster_nodes[0],bucket_list)
        status, output, msg = self.backup_restore()
        if not status:
            self.fail(msg)
        self.log.info(msg)

    def test_backup_create(self):
        status, output, msg = self.backup_create()
        if not status:
            self.fail(msg)
        self.log.info(msg)
        status, msg = self.validation_helper.validate_backup_create()
        if not status:
            self.fail(msg)
        self.log.info(msg)
