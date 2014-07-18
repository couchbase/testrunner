from basetestcase import BaseTestCase
from couchbase.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection, Bucket
from remote.remote_util import RemoteMachineShellConnection
from testconstants import LINUX_CW_LOG_PATH
from testconstants import MAC_CW_LOG_PATH
from testconstants import WINDOWS_CW_LOG_PATH


class CWCBaseTest(BaseTestCase):

    def setUp(self):
        super(CWCBaseTest, self).setUp()
        self.product = self.input.param("product", "cb")
        self.vbuckets = self.input.param("vbuckets", 128)
        self.version = self.input.param("version", None)
        self.doc_ops = self.input.param("doc_ops", None)
        self.upload = self.input.param("upload", False)
        self.uploadHost = self.input.param("uploadHost", None)
        self.customer = self.input.param("customer", "")
        self.ticket = self.input.param("ticket", "")
        self.collect_nodes = self.input.param("collect_nodes", "*")
        self.shutdown_nodes = self.input.param("shutdown_nodes", None)
        if self.doc_ops is not None:
            self.doc_ops = self.doc_ops.split(";")
        self.defaul_map_func = "function (doc) {\n  emit(doc._id, doc);\n}"

        #define the data that will be used to test
        self.blob_generator = self.input.param("blob_generator", True)
        server = self.servers[0]
        rest = RestConnection(server)
        if self.blob_generator:
            #gen_load data is used for upload before each test(1000 items by default)
            self.gen_load = BlobGenerator('test', 'test-', self.value_size, end=self.num_items)
            #gen_update is used for doing mutation for 1/2th of uploaded data
            self.gen_update = BlobGenerator('test', 'test-', self.value_size, end=(self.num_items / 2 - 1))
            #upload data before each test
            self._load_all_buckets(self.servers[0], self.gen_load, "create", 0)
        else:
            self._load_doc_data_all_buckets()
        shell = RemoteMachineShellConnection(self.master)
        type = shell.extract_remote_info().distribution_type
        shell.disconnect()
        self.log_path = ""
        if type.lower() == 'windows':
            self.log_path = WINDOWS_CW_LOG_PATH
        elif type.lower() in ["ubuntu", "centos", "red hat"]:
            self.log_path = LINUX_CW_LOG_PATH
        elif type.lower() == "mac":
            self.log_path = MAC_CW_LOG_PATH

    def tearDown(self):
        super(CWCBaseTest, self).tearDown()
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            info = shell.extract_remote_info()
            shell.stop_server(info.type.lower())
            shell.start_server(info.type.lower())
            shell.disconnect()