import time
import gc
from threading import Thread
from backup.backup_base import BackupBaseTest
from couchbase_helper.documentgenerator import BlobGenerator
from membase.api.rest_client import Bucket

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
        self._load_all_buckets(self.master, gen_load_backup, "create", 0, 2, self.item_flag, True, batch_size=20000, pause_secs=5, timeout_secs=180)
        #store items before backup starts to kvstores[2]
        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])

        gen_load = BlobGenerator('mysql', 'mysql-', self.value_size, end=self.num_items)
        data_load_thread = Thread(target=self._load_all_buckets,
                                  name="load_data",
                                  args=(self.master, gen_load, "create", 0, 1, 0, True))
        #store noise items during backup to kvstores[1]

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
            default_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                               replicas=self.num_replicas)
            self.cluster.create_default_bucket(default_params)
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
        self.verify_results(self.master, 2) #do verification only with kvstores[2]

    def CreateUpdateDeleteExpireDuringBackup(self):
        """Backup the items during mutation on existing items is running.

        We first load amount of items. After that, when we start backup, we begin do mutations on these existing items."""

        gen_load = BlobGenerator('mysql', 'mysql-', self.value_size, end=self.num_items)
        gen_update = BlobGenerator('mysql', 'mysql-', self.value_size, end=(self.num_items // 2 - 1))
        gen_expire = BlobGenerator('mysql', 'mysql-', self.value_size, start=self.num_items // 2, end=(self.num_items * 3 // 4 - 1))
        gen_delete = BlobGenerator('mysql', 'mysql-', self.value_size, start=self.num_items * 3 // 4, end=self.num_items)
        self._load_all_buckets(self.master, gen_load, "create", 0, 1, 0, True, batch_size=20000, pause_secs=5, timeout_secs=180)
        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])

        mutate_threads = []
        if(self.doc_ops is not None):
            if("update" in self.doc_ops):
                mutate_threads.append(self._async_load_all_buckets(self.master, gen_update, "update", 0, 1, 0, True, batch_size=20000))

            if("delete" in self.doc_ops):
                mutate_threads.append(self._async_load_all_buckets(self.master, gen_delete, "delete", 0, 1, 0, True, batch_size=20000))

            if("expire" in self.doc_ops):
                mutate_threads.append(self._async_load_all_buckets(self.master, gen_expire, "update", self.expire_time, 1, 0, True, batch_size=20000))
            if("change_password" in self.doc_ops):
                old_pass = self.master.rest_password
                self.change_password(new_password=self.input.param("new_password", "new_pass"))
            if("change_port" in self.doc_ops):
                self.change_port(new_port=self.input.param("new_port", "9090"))
        try:
            first_backup_thread = Thread(target=self.shell.execute_cluster_backup,
                                         name="backup",
                                         args=(self.couchbase_login_info, self.backup_location, self.command_options))
            first_backup_thread.start()
            first_backup_thread.join()

            for t in mutate_threads:
                for task in t:
                    task.result()

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
            #TODO implement verification for this test case
        finally:
            if self.doc_ops:
                if "change_password" in self.doc_ops:
                    self.change_password(new_password=old_pass)
                elif "change_port" in self.doc_ops:
                    self.change_port(new_port='8091',
                                     current_port=self.input.param("new_port", "9090"))
