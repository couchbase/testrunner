import time
import gc
from backup.backup_base import BackupBaseTest
from couchbase_helper.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection, Bucket

class OpsBeforeBackupTests(BackupBaseTest):

    def setUp(self):
        super(OpsBeforeBackupTests, self).setUp()
        self.num_mutate_items = self.input.param("mutate_items", 1000)

    def tearDown(self):
        super(OpsBeforeBackupTests, self).tearDown()

    def CreateUpdateDeleteBeforeBackup(self):
        """Back up the buckets after doing docs operations: create, update, delete, recreate.

        We load 2 kinds of items into the cluster with different key value prefix. Then we do
        mutations on part of the items according to clients' input param. After backup, we
        delete the existing buckets then recreate them and restore all the buckets. We verify
        the results by comparison between the items in KVStore and restored buckets items."""

        gen_load_mysql = BlobGenerator('mysql', 'mysql-', self.value_size, end=(self.num_items/2-1))
        gen_load_couchdb = BlobGenerator('couchdb', 'couchdb-', self.value_size, start=self.num_items/2, end=self.num_items)
        gen_update = BlobGenerator('mysql', 'mysql-', self.value_size, end=(self.num_items // 2 - 1))
        gen_delete = BlobGenerator('couchdb', 'couchdb-', self.value_size, start=self.num_items // 2, end=self.num_items)
        gen_create = BlobGenerator('mysql', 'mysql-', self.value_size, start=self.num_items // 2 + 1, end=self.num_items *3 // 2)
        self._load_all_buckets(self.master, gen_load_mysql, "create", 0, 1, self.item_flag, True, batch_size=20000, pause_secs=5, timeout_secs=180)
        self._load_all_buckets(self.master, gen_load_couchdb, "create", 0, 1, self.item_flag, True, batch_size=20000, pause_secs=5, timeout_secs=180)

        if(self.doc_ops is not None):
            if("update" in self.doc_ops):
                self._load_all_buckets(self.master, gen_update, "update", 0, 1, self.item_flag, True, batch_size=20000, pause_secs=5, timeout_secs=180)
            if("create" in self.doc_ops):
                self._load_all_buckets(self.master, gen_create, "create", 0, 1, self.item_flag, True, batch_size=20000, pause_secs=5, timeout_secs=180)
            if("delete" in self.doc_ops):
                self._load_all_buckets(self.master, gen_delete, "delete", 0, 1, self.item_flag, True, batch_size=20000, pause_secs=5, timeout_secs=180)
        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])

        self.shell.execute_cluster_backup(self.couchbase_login_info, self.backup_location, self.command_options)

        kvs_before = {}
        for bucket in self.buckets:
            kvs_before[bucket.name] = bucket.kvs[1]
        bucket_names = [bucket.name for bucket in self.buckets]
        self._all_buckets_delete(self.master)
        gc.collect()

        if self.default_bucket:
            default_params=self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                             replicas=self.num_replicas)
            self.cluster.create_default_bucket(default_params)
            self.buckets.append(Bucket(name="default", authType="sasl", saslPassword="", num_replicas=self.num_replicas, bucket_size=self.bucket_size))
        self._create_sasl_buckets(self.master, self.sasl_buckets)
        self._create_standard_buckets(self.master, self.standard_buckets)

        for bucket in self.buckets:
            bucket.kvs[1] = kvs_before[bucket.name]
        del kvs_before
        gc.collect()
        self.shell.restore_backupFile(self.couchbase_login_info, self.backup_location, bucket_names)

        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])
        self.verify_results(self.master)
        self._verify_stats_all_buckets(self.servers[:self.num_servers])

    def CreateUpdateDeleteExpireBeforeBackup(self):
        """Backup up the buckets after operations: update, delete, expire.

        We load a number of items first and then load some extra items. We do update, delete, expire operation
        on those extra items. After these mutations, we backup all the items and restore them for verification """

        gen_load = BlobGenerator('mysql', 'mysql-', self.value_size, end=self.num_items)
        gen_extra = BlobGenerator('couchdb', 'couchdb-', self.value_size, end=self.num_mutate_items)
        self._load_all_buckets(self.master, gen_load, "create", 0, 1, self.item_flag, True, batch_size=20000, pause_secs=5, timeout_secs=180)
        extra_items_deleted_flag = 0

        if(self.doc_ops is not None):
            self._load_all_buckets(self.master, gen_extra, "create", 0, 1, self.item_flag, True, batch_size=20000, pause_secs=5, timeout_secs=180)
            if("update" in self.doc_ops):
                self._load_all_buckets(self.master, gen_extra, "update", 0, 1, self.item_flag, True, batch_size=20000, pause_secs=5, timeout_secs=180)
            if("delete" in self.doc_ops):
                self._load_all_buckets(self.master, gen_extra, "delete", 0, 1, self.item_flag, True, batch_size=20000, pause_secs=5, timeout_secs=180)
                extra_items_deleted_flag = 1
            if("expire" in self.doc_ops):
                if extra_items_deleted_flag == 1:
                    self._load_all_buckets(self.master, gen_extra, "create", 0, 1, self.item_flag, True, batch_size=20000, pause_secs=5, timeout_secs=180)
                self._load_all_buckets(self.master, gen_extra, "update", self.expire_time, 1, self.item_flag, True, batch_size=20000, pause_secs=5, timeout_secs=180)
        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])
        time.sleep(30)

        self.shell.execute_cluster_backup(self.couchbase_login_info, self.backup_location, self.command_options)

        kvs_before = {}
        for bucket in self.buckets:
            kvs_before[bucket.name] = bucket.kvs[1]
        self._all_buckets_delete(self.master)
        gc.collect()

        if self.default_bucket:
            default_params=self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                             replicas=self.num_replicas)
            self.cluster.create_default_bucket(default_params)
            self.buckets.append(Bucket(name="default", authType="sasl", saslPassword="", num_replicas=self.num_replicas, bucket_size=self.bucket_size))
        self._create_sasl_buckets(self.master, self.sasl_buckets)
        self._create_standard_buckets(self.master, self.standard_buckets)

        for bucket in self.buckets:
            bucket.kvs[1] = kvs_before[bucket.name]
        del kvs_before
        gc.collect()
        bucket_names = [bucket.name for bucket in self.buckets]
        self.shell.restore_backupFile(self.couchbase_login_info, self.backup_location, bucket_names)
        time.sleep(self.expire_time) #system sleeps for expired items

        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])
        self.verify_results(self.master)
        self._verify_stats_all_buckets(self.servers[:self.num_servers])

