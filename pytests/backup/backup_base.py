from basetestcase import BaseTestCase
from remote.remote_util import RemoteMachineShellConnection
from memcached.helper.kvstore import KVStore

class BackupBaseTest(BaseTestCase):
    def setUp(self):
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
        for bucket, kvstores in self.buckets.items():
            self.buckets[bucket][2] = KVStore()
        self.cluster.rebalance(self.servers[:1], servers_in, [])

    def tearDown(self):
        super(BackupBaseTest, self).tearDown()
        self.shell.delete_backupFile(self.backup_location)
        self.shell.disconnect()
