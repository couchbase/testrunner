import testconstants
from basetestcase import BaseTestCase
from couchbase_helper.document import View
from couchbase_helper.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection, Bucket
from remote.remote_util import RemoteMachineShellConnection
from membase.helper.cluster_helper import ClusterOperationHelper
from testconstants import LINUX_COUCHBASE_BIN_PATH, WIN_COUCHBASE_BIN_PATH,\
                          MAC_COUCHBASE_BIN_PATH


class RackzoneBaseTest(BaseTestCase):

    def setUp(self):
        super(RackzoneBaseTest, self).setUp()
        self.product = self.input.param("product", "cb")
        self.vbuckets = self.input.param("vbuckets", 128)
        self.version = self.input.param("version", "2.5.1-1082")
        self.type = self.input.param('type', 'enterprise')
        self.doc_ops = self.input.param("doc_ops", None)
        if self.doc_ops is not None:
            self.doc_ops = self.doc_ops.split(";")
        self.defaul_map_func = "function (doc) {\n  emit(doc._id, doc);\n}"

        #define the data that will be used to test
        self.blob_generator = self.input.param("blob_generator", True)
        serverInfo = self.servers[0]
        rest = RestConnection(serverInfo)
        if not rest.is_enterprise_edition():
            raise Exception("This couchbase server is not Enterprise Edition.\
                  This RZA feature requires Enterprise Edition to work")
        if self.blob_generator:
            #gen_load data is used for upload before each test(1000 items by default)
            self.gen_load = BlobGenerator('test', 'test-', self.value_size, end=self.num_items)
            #gen_update is used for doing mutation for 1/2th of uploaded data
            self.gen_update = BlobGenerator('test', 'test-', self.value_size, end=(self.num_items // 2 - 1))
            #upload data before each test
            self._load_all_buckets(self.servers[0], self.gen_load, "create", 0)
        else:
            self._load_doc_data_all_buckets()
        shell = RemoteMachineShellConnection(self.master)
        type = shell.extract_remote_info().distribution_type
        shell.disconnect()
        self.os_name = "linux"
        self.is_linux = True
        self.cbstat_command = "%scbstats" % (LINUX_COUCHBASE_BIN_PATH)
        if type.lower() == 'windows':
            self.is_linux = False
            self.os_name = "windows"
            self.cbstat_command = "%scbstats.exe" % (WIN_COUCHBASE_BIN_PATH)
        if type.lower() == 'mac':
            self.cbstat_command = "%scbstats" % (MAC_COUCHBASE_BIN_PATH)
        if self.nonroot:
            self.cbstat_command = "/home/%s%scbstats" % (self.master.ssh_username,
                                                         LINUX_COUCHBASE_BIN_PATH)

    def tearDown(self):
        """ Some test involve kill couchbase server.  If the test steps failed
            right after kill erlang process, we need to start couchbase server
            in teardown so that the next test will not be false failed """
        super(RackzoneBaseTest, self).tearDown()
        ClusterOperationHelper.cleanup_cluster(self.servers, master=self.master)
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            shell.start_couchbase()
            self.sleep(7, "Time needed for couchbase server starts completely.")
        serverInfo = self.servers[0]
        rest = RestConnection(serverInfo)
        zones = rest.get_zone_names()
        for zone in zones:
            if zone != "Group 1":
                rest.delete_zone(zone)
