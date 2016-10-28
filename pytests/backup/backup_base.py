import gc
import re
import crc32
from memcached.helper.kvstore import KVStore
from basetestcase import BaseTestCase
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection, Bucket
from memcached.helper.data_helper import VBucketAwareMemcached
from couchbase_helper.document import DesignDocument, View

class BackupBaseTest(BaseTestCase):
    def setUp(self):
        self.times_teardown_called = 1
        super(BackupBaseTest, self).setUp()
        self.shell = RemoteMachineShellConnection(self.master)
        info = self.shell.extract_remote_info()
        self.os = info.type.lower()
        self.value_size = self.input.param("value_size", 256)
        self.expire_time = self.input.param("expire_time", 60)
        self.number_of_backups = self.input.param("number_of_backups", 1)
        self.num_ddocs = self.input.param("num_ddocs", 1)
        self.num_views_per_ddoc = self.input.param("num_views_per_ddoc", 1)
        self.test_with_view = self.input.param("test_with_view", False)
        self.default_map_func = 'function (doc) { emit(doc.age, doc.first_name);}'
        self.dev_view = self.input.param("dev_view", True)
        self.default_view = View("View", self.default_map_func, None, False)
        self.bucket_ddoc_map = {}
        self.backup_type = self.input.param("backup_type", None)
        self.item_flag = self.input.param("item_flag", 0)
        self.couchbase_login_info = "%s:%s" % (self.input.membase_settings.rest_username,
                                               self.input.membase_settings.rest_password)
        self.backup_location = self.input.param("backup_location", "/tmp/backup")
        self.command_options = self.input.param("command_options", '')
        if self.command_options is not '':
            self.command_options = self.command_options.split(";")
        self.doc_ops = self.input.param("doc_ops", None)
        if self.doc_ops is not None:
            self.doc_ops = self.doc_ops.split(";")
        self.backup_x_options = self.input.param("backup_x_options", None)
        if self.backup_x_options is not None:
            temp = self.backup_x_options.split(";")
            temp_x_options = {}
            for element in temp:
                temp_array = element.split()
                temp_x_options[temp_array[0]] = temp_array[1]
            self.backup_x_options = temp_x_options

        self.restore_x_options = self.input.param("restore_x_options", None)
        if self.restore_x_options is not None:
            temp = self.restore_x_options.split(";")
            temp_x_options = {}
            for element in temp:
                temp_array = element.split()
                temp_x_options[temp_array[0]] = temp_array[1]
            self.restore_x_options = temp_x_options
        servers_in = [self.servers[i + 1] for i in range(self.num_servers - 1)]
        for bucket in self.buckets:
            bucket.kvs[2] = KVStore()
        self.cluster.rebalance(self.servers[:1], servers_in, [])

    def tearDown(self):
        if not self.input.param("skip_cleanup", True):
            if self.times_teardown_called > 1 :
                if self.os == 'windows':
                    output, error = self.shell.execute_command("taskkill /F /T /IM cbbackup.exe")
                    self.shell.log_command_output(output, error)
                    self.shell.delete_files("/cygdrive/c%s" % (self.backup_location))
                else:
                    self.shell.delete_files(self.backup_location)
                self.shell.disconnect()
                del self.buckets
                gc.collect()
        if self.input.param("skip_cleanup", True):
            if self.case_number > 1 or self.times_teardown_called > 1:
                if self.os == 'windows':
                    output, error = self.shell.execute_command("taskkill /F /T /IM cbbackup.exe")
                    self.shell.log_command_output(output, error)
                    self.shell.delete_files("/cygdrive/c%s" % (self.backup_location))
                else:
                    self.shell.delete_files(self.backup_location)
                self.shell.disconnect()
                del self.buckets
                gc.collect()
        self.times_teardown_called += 1
        super(BackupBaseTest, self).tearDown()

    def verify_results(self, server, kv_store=1):
        """This is the verification function for test cases of backup/restore.

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
                    key_name = s[sub + 1:]
                if s.find("-b") != -1:
                    sub = s.find(" ")
                    bucket_name = s[sub + 1:]
                if "--single-node" in self.command_options:
                    single_node_flag = True

        #we delete the buckets whose name does not match the name assigned to -b in KVStore
        self.buckets = [bucket for bucket in self.buckets if bucket_name is None or bucket.name == bucket_name]
        for bucket in self.buckets:
             if key_name is not None:
                valid_keys, deleted_keys = bucket.kvs[kv_store].key_set()
                for key in valid_keys:
                    matchObj = re.search(key_name, key, re.M | re.S) #use regex match to find out keys we need to verify
                    if matchObj is None:
                        partition = bucket.kvs[kv_store].acquire_partition(key)
                        partition.delete(key)  #we delete keys whose prefix does not match the value assigned to -k in KVStore
                        bucket.kvs[kv_store].release_partition(key)
        if single_node_flag is False:
            self._verify_all_buckets(server, kv_store, self.wait_timeout * 50, self.max_verify, True, 1)
        else:
            self.verify_single_node(server, kv_store)

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
                vBucketId = VBucketAware._get_vBucket_id(key)
                which_server = vBucketMap[vBucketId]
                sub = which_server.find(":")
                which_server_ip = which_server[:sub]
                if which_server_ip != server.ip:
                    partition = bucket.kvs[kv_store].acquire_partition(key)
                    partition.delete(key)
                    bucket.kvs[kv_store].release_partition(key)

        self._verify_all_buckets(server, kv_store, self.wait_timeout * 50, self.max_verify, True, 1)
