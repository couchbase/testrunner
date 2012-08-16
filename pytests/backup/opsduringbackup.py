import time
from threading import Thread
from backup.backup_base import BackupBaseTest
from couchbase.documentgenerator import BlobGenerator
from membase.api.rest_client import Bucket
from remote.remote_util import RemoteMachineShellConnection
import gc

class OpsDuringBackupTests(BackupBaseTest):

    def setUp(self):
        super(OpsDuringBackupTests, self).setUp()
        self.backup_items = self.input.param("backup_items", 1000)

    def tearDown(self):
        super(OpsDuringBackupTests, self).tearDown()

    def LoadDuringBackup(self):
        """Backup the items during data loading is running.

        We first load a number of items. Then we start backup while loading another amount number of items into
        cluster as "noise" during the backup. During verification, we want to make sure that every item before backup
        starts can be restored correctly."""

        gen_load_backup = BlobGenerator('couchdb', 'couchdb', self.value_size, end=self.backup_items)
        self._load_all_buckets(self.master, gen_load_backup, "create", 0, 2)  #store items before backup starts to kvstores[2]
        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])

        gen_load = BlobGenerator('mysql', 'mysql-', self.value_size, end=self.num_items)
        data_load_thread = Thread(target=self._load_all_buckets,
                                  name="load_data",
                                  args=(self.master, gen_load, "create", 0))  #store noise items during backup to kvstores[1]

        backup_thread = Thread(target=self.shell.execute_cluster_backup,
                               name="backup",
                               args=(self.couchbase_login_info, self.backup_location, self.command_options))

        backup_thread.start()
        data_load_thread.start()
        data_load_thread.join()
        backup_thread.join()
        #TODO: implement a mechanism to check the backup progress to prevent backup_thread hangs up
        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])

        kvs_before = {}
        for bucket in self.buckets:
            kvs_before[bucket.name] = bucket.kvs[2]
        self._all_buckets_delete(self.master)
        gc.collect()

        if self.default_bucket:
            self.cluster.create_default_bucket(self.master, self.bucket_size, self.num_replicas)
            self.buckets.append(Bucket(name="default", authType="sasl", saslPassword="", num_replicas=self.num_replicas, bucket_size=self.bucket_size))
        self._create_sasl_buckets(self.master, self.sasl_buckets)
        self._create_standard_buckets(self.master, self.standard_buckets)

        for bucket in self.buckets:
            bucket.kvs[2] = kvs_before[bucket.name]
        del kvs_before
        gc.collect()
        bucket_names = [bucket.name for bucket in self.buckets]
        self.shell.restore_backupFile(self.couchbase_login_info, self.backup_location, bucket_names)

        for bucket in self.buckets:
            del bucket.kvs[1]
        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])
        self._verify_all_buckets(self.master, 2, timeout=self.wait_timeout*50) #do verification only with kvstores[2]

    def CreateUpdateDeleteExpireDuringBackup(self):
        """Backup the items during mutation on existing items is running.

        We first load amount of items. After that, when we start backup, we begin do mutations on these existing items."""

        gen_load = BlobGenerator('mysql', 'mysql-', self.value_size, end=self.num_items)
        gen_update = BlobGenerator('mysql', 'mysql-', self.value_size, end=(self.num_items/2-1))
        gen_expire = BlobGenerator('mysql', 'mysql-', self.value_size, start=self.num_items/2, end=(self.num_items*3/4-1))
        gen_delete = BlobGenerator('mysql', 'mysql-', self.value_size, start=self.num_items*3/4, end=self.num_items)
        self._load_all_buckets(self.master, gen_load, "create", 0)
        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])

        mutate_threads = []
        if(self.doc_ops is not None):
            if("update" in self.doc_ops):
                mutate_threads.append(Thread(target=self._load_all_buckets,
                                             name="update",
                                             args=(self.master, gen_update, "update", 0)))
            if("delete" in self.doc_ops):
                mutate_threads.append(Thread(target=self._load_all_buckets,
                                             name="delete",
                                             args=(self.master, gen_delete, "delete", 0)))
            if("expire" in self.doc_ops):
                mutate_threads.append(Thread(target=self._load_all_buckets,
                                             name="expire",
                                             args=(self.master, gen_expire, "update", self.expire_time)))
        for t in mutate_threads:
            t.start()

        first_backup_thread = Thread(target=self.shell.execute_cluster_backup,
                                     name="backup",
                                     args=(self.couchbase_login_info, self.backup_location, self.command_options))
        first_backup_thread.start()
        first_backup_thread.join()
        for t in mutate_threads:
            t.join()

        kvs_before = {}
        for bucket in self.buckets:
            kvs_before[bucket.name] = bucket.kvs[1]
        self._all_buckets_delete(self.master)
        gc.collect()

        if self.default_bucket:
            self.cluster.create_default_bucket(self.master, self.bucket_size, self.num_replicas)
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
        #TODO implement verification for this test case
