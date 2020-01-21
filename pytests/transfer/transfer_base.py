from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import BlobGenerator
from remote.remote_util import RemoteMachineShellConnection
import time

class TransferBaseTest(BaseTestCase):
    def setUp(self):
        super(TransferBaseTest, self).setUp()
        self.couchbase_login_info = "%s:%s"%(self.input.membase_settings.rest_username,
                                             self.input.membase_settings.rest_password)
        self.value_size = self.input.param("value_size", 256)
        self.expire_time = self.input.param("expire_time", 60)
        self.item_flag = self.input.param("item_flag", 0)
        self.backup_location = self.input.param("backup_location", "/tmp/backup")
        self.win_data_location = self.input.param("win_data_location", "/tmp/data")
        self.server_origin = self.servers[0]
        self.server_recovery = self.servers[1]
        self.doc_ops = self.input.param("doc_ops", None)
        if self.doc_ops is not None:
            self.doc_ops = self.doc_ops.split(";")
        self.shell = RemoteMachineShellConnection(self.server_origin)
        info = self.shell.extract_remote_info()
        self.os = info.type.lower()

    def tearDown(self):
        super(TransferBaseTest, self).tearDown()

    def load_data(self):
        gen_load = BlobGenerator('nosql', 'nosql-', self.value_size, end=self.num_items)
        gen_update = BlobGenerator('nosql', 'nosql-', self.value_size, end=(self.num_items // 2 - 1))
        gen_expire = BlobGenerator('nosql', 'nosql-', self.value_size, start=self.num_items // 2, end=(self.num_items * 3 // 4 - 1))
        gen_delete = BlobGenerator('nosql', 'nosql-', self.value_size, start=self.num_items * 3 // 4, end=self.num_items)
        self._load_all_buckets(self.server_origin, gen_load, "create", 0, 1, self.item_flag, True, batch_size=20000, pause_secs=5, timeout_secs=180)

        if(self.doc_ops is not None):
            if("update" in self.doc_ops):
                self._load_all_buckets(self.server_origin, gen_update, "update", 0, 1, self.item_flag, True, batch_size=20000, pause_secs=5, timeout_secs=180)
            if("delete" in self.doc_ops):
                self._load_all_buckets(self.server_origin, gen_delete, "delete", 0, 1, self.item_flag, True, batch_size=20000, pause_secs=5, timeout_secs=180)
            if("expire" in self.doc_ops):
                self._load_all_buckets(self.server_origin, gen_expire, "update", self.expire_time, 1, self.item_flag, True, batch_size=20000, pause_secs=5, timeout_secs=180)
        self._wait_for_stats_all_buckets([self.server_origin])
        time.sleep(30)


