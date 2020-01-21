import testconstants
from basetestcase import BaseTestCase
from xdcr.xdcrbasetests import XDCRReplicationBaseTest
from couchbase_helper.document import View
from couchbase_helper.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection, Bucket
from remote.remote_util import RemoteMachineShellConnection
from testconstants import LINUX_BACKUP_PATH
from testconstants import WIN_BACKUP_PATH, WIN_BACKUP_C_PATH
from testconstants import LINUX_COUCHBASE_BIN_PATH
from testconstants import WIN_COUCHBASE_BIN_PATH


class CommunityBaseTest(BaseTestCase):

    def setUp(self):
        super(CommunityBaseTest, self).setUp()
        self.product = self.input.param("product", "cb")
        self.vbuckets = self.input.param("vbuckets", 128)
        self.version = self.input.param("version", "2.5.1-1082")
        self.type = self.input.param('type', 'community')
        self.doc_ops = self.input.param("doc_ops", None)
        if self.doc_ops is not None:
            self.doc_ops = self.doc_ops.split(";")
        self.defaul_map_func = "function (doc) {\n  emit(doc._id, doc);\n}"
        self.couchbase_login = "%s:%s" % (self.input.membase_settings.rest_username,
                                          self.input.membase_settings.rest_password)
        self.backup_option = self.input.param("backup_option", '')
        #define the data that will be used to test
        self.blob_generator = self.input.param("blob_generator", True)
        self.cli_test = self.input.param("cli_test", False)
        self.rest = RestConnection(self.master)
        if self.rest.is_enterprise_edition():
            raise Exception("This couchbase server is not Community Edition."
                  "Tests require Community Edition to test")
        self.version = self.rest.get_nodes_version()[:5]

        if self.blob_generator:
            #gen_load data is used for upload before each test(1000 items by default)
            self.gen_load = BlobGenerator('test', 'test-', self.value_size, end=self.num_items)
            #gen_update is used for doing mutation for 1/2th of uploaded data
            self.gen_update = BlobGenerator('test', 'test-', self.value_size, end=(self.num_items // 2 - 1))
            #upload data before each test
            self._load_all_buckets(self.servers[0], self.gen_load, "create", 0)
        else:
            self._load_doc_data_all_buckets()
        self.remote = RemoteMachineShellConnection(self.master)
        type = self.remote.extract_remote_info().distribution_type
        self.backup_location = LINUX_BACKUP_PATH
        self.backup_c_location = LINUX_BACKUP_PATH
        self.bin_path = LINUX_COUCHBASE_BIN_PATH
        self.file_extension = ""
        if type.lower() == 'windows':
            self.is_linux = False
            self.backup_location = WIN_BACKUP_PATH
            self.backup_c_location = WIN_BACKUP_C_PATH
            self.bin_path = WIN_COUCHBASE_BIN_PATH
            self.file_extension = ".exe"
        else:
            self.is_linux = True

    def tearDown(self):
        """ Some test involve kill couchbase server.  If the test steps failed
            right after kill erlang process, we need to start couchbase server
            in teardown so that the next test will not be false failed """
        super(CommunityBaseTest, self).tearDown()


class CommunityXDCRBaseTest(XDCRReplicationBaseTest):
    def setup(self):
        super(CommunityXDCRBaseTest, self).setUp()


    def tearDown(self):
        super(CommunityXDCRBaseTest, self).tearDown()
