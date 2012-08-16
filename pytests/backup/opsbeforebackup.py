import time
import re
import crc32
from backup.backup_base import BackupBaseTest
from couchbase.documentgenerator import BlobGenerator
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection, Bucket
from memcached.helper.data_helper import VBucketAwareMemcached
import gc

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
        gen_update = BlobGenerator('mysql', 'mysql-', self.value_size, end=(self.num_items / 2 - 1))
        gen_delete = BlobGenerator('couchdb', 'couchdb-', self.value_size, start=self.num_items / 2, end=self.num_items)
        gen_create = BlobGenerator('mysql', 'mysql-', self.value_size, start=self.num_items / 2 + 1, end=self.num_items *3 / 2)
        self._load_all_buckets(self.master, gen_load_mysql, "create", 0)
        self._load_all_buckets(self.master, gen_load_couchdb, "create", 0)

        if(self.doc_ops is not None):
            if("update" in self.doc_ops):
                self._load_all_buckets(self.master, gen_update, "update", 0)
            if("create" in self.doc_ops):
                self._load_all_buckets(self.master, gen_create, "create", 0)
            if("delete" in self.doc_ops):
                self._load_all_buckets(self.master, gen_delete, "delete", 0)
        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])

        self.shell.execute_cluster_backup(self.couchbase_login_info, self.backup_location, self.command_options)

        kvs_before = {}
        for bucket in self.buckets:
            kvs_before[bucket.name] = bucket.kvs[1]
        bucket_names = [bucket.name for bucket in self.buckets]
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
        self._load_all_buckets(self.master, gen_load, "create", 0)
        extra_items_deleted_flag = 0

        if(self.doc_ops is not None):
            self._load_all_buckets(self.master, gen_extra, "create", 0)
            if("update" in self.doc_ops):
                self._load_all_buckets(self.master, gen_extra, "update", 0)
            if("delete" in self.doc_ops):
                self._load_all_buckets(self.master, gen_extra, "delete", 0)
                extra_items_deleted_flag = 1
            if("expire" in self.doc_ops):
                if extra_items_deleted_flag == 1:
                    self._load_all_buckets(self.master, gen_extra, "create", 0)
                self._load_all_buckets(self.master, gen_extra, "update", self.expire_time)
        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])

        self.shell.execute_cluster_backup(self.couchbase_login_info, self.backup_location, self.command_options)

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
        self.verify_results(self.master)
        self._verify_stats_all_buckets(self.servers[:self.num_servers])

    def verify_results(self, server, kv_store=1):
        """This is the verification function for test cases implemented under "OpsBeforeBackupTests".

        Args:
          server: the master server in the cluster as self.master.
          kv_store: default value is 1. This is the key of the kv_store of each bucket.

        if the command line assign command options -k and/or -b and/or --single-node, in the verification function
        key_name indicates which keys we need to verify and bucket_name indicates which bucket we need to verify.
        If single node flag is true, the we only need to verify all the buckets at the master node"""

        key_name = None
        bucket_name = None
        single_node_flag = False
        if self.command_options is not None:
            for s in self.command_options:
                if s.find("-k") != -1:
                    sub = s.find(" ")
                    key_name = s[sub+1:]
                if s.find("-b") != -1:
                    sub = s.find(" ")
                    bucket_name = s[sub+1:]
                if "--single-node" in self.command_options:
                    single_node_flag = True

        #we delete the buckets whose name does not match the name assigned to -b in KVStore
        self.buckets = [bucket for bucket in self.buckets if bucket_name is None or bucket.name == bucket_name]
        for bucket in self.buckets:
             if key_name is not None:
                valid_keys, deleted_keys = bucket.kvs[kv_store].key_set()
                for key in valid_keys:
                    matchObj = re.search(key_name, key, re.M|re.S) #use regex match to find out keys we need to verify
                    if matchObj is None:
                        partition = bucket.kvs[kv_store].acquire_partition(key)
                        partition.delete(key)  #we delete keys whose prefix does not match the value assigned to -k in KVStore
                        bucket.kvs[kv_store].release_partition(key)
        if single_node_flag is False:
            self._verify_all_buckets(server, timeout=self.wait_timeout*50)
        else:
            self.verify_single_node(server)

    def verify_single_node(self, server, kv_store=1):
        """This is the verification function for single node backup.

        Args:
          server: the master server in the cluster as self.master.
          kv_store: default value is 1. This is the key of the kv_store of each bucket.

        If --single-node flag appears in backup commad line, we just backup all the items
        from a single node (the master node in this case). For each bucket, we request for the vBucketMap. For every key
        in the kvstore of that bucket, we use hash function to get the vBucketId corresponding to that
        key. By using the vBucketMap, we can know whether that key is in master node or not.
        If yes, keep it. Otherwise delete it."""

        rest = RestConnection(server)
        for bucket in self.buckets:
            VBucketAware = VBucketAwareMemcached(rest, bucket.name)
            memcacheds, vBucketMap, vBucketMapReplica = VBucketAware.request_map(rest, bucket.name)
            valid_keys, deleted_keys = bucket.kvs[kv_store].key_set()
            for key in valid_keys:
                vBucketId = crc32.crc32_hash(key) & (len(vBucketMap) - 1)
                which_server = vBucketMap[vBucketId]
                sub = which_server.find(":")
                which_server_ip = which_server[:sub]
                if which_server_ip != server.ip:
                    partition = bucket.kvs[kv_store].acquire_partition(key)
                    partition.delete(key)
                    bucket.kvs[kv_store].release_partition(key)

        self._verify_all_buckets(server, timeout=self.wait_timeout*50)
