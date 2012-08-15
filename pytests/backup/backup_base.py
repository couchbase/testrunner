from basetestcase import BaseTestCase
from remote.remote_util import RemoteMachineShellConnection
from memcached.helper.kvstore import KVStore
import gc

class BackupBaseTest(BaseTestCase):
    def setUp(self):
        self.times_teardown_called = 1
        super(BackupBaseTest, self).setUp()
        self.shell = RemoteMachineShellConnection(self.master)
        self.value_size = self.input.param("value_size", 256)
        self.couchbase_login_info = self.input.param("login_info", "Administrator:password")
        self.backup_location = self.input.param("backup_location", "/tmp/backup")
        self.command_options = self.input.param("command_options", None)
        if self.command_options is not None:
            self.command_options = self.command_options.split(";")
        self.doc_ops = self.input.param("doc_ops", None)
        if self.doc_ops is not None:
            self.doc_ops = self.doc_ops.split(";")
        servers_in = [self.servers[i+1] for i in range(self.num_servers-1)]
        for bucket in self.buckets:
            bucket.kvs[2]= KVStore()
        self.cluster.rebalance(self.servers[:1], servers_in, [])

    def tearDown(self):
        super(BackupBaseTest, self).tearDown()
        if not self.input.param("skip_cleanup", True):
            if times_tear_down_called > 1 :
                self.shell.delete_backupFile(self.backup_location)
                self.shell.disconnect()
                del self.buckets
                gc.collect()
        if self.input.param("skip_cleanup", True):
            if self.case_number > 1 or self.times_teardown_called >1:
                self.shell.delete_backupFile(self.backup_location)
                self.shell.disconnect()
                del self.buckets
                gc.collect()
        self.times_teardown_called +=1
