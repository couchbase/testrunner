from basetestcase import BaseTestCase
from remote.remote_util import RemoteMachineShellConnection

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
        self.doc_ops = self.input.param("doc_ops", None)
        if self.doc_ops is not None:
            self.doc_ops = self.doc_ops.split(";")

    def tearDown(self):
        super(TransferBaseTest, self).tearDown()


