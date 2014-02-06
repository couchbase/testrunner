import testconstants
from basetestcase import BaseTestCase
from couchbase.document import View
from couchbase.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection, Bucket
from remote.remote_util import RemoteMachineShellConnection



class RackzoneBaseTest(BaseTestCase):

    def setUp(self):
        super(RackzoneBaseTest, self).setUp()
        self.value_size = self.input.param("value_size", 128)
        self.num_buckets = self.input.param("num_buckets", 0)
        self.num_items = self.input.param("items", 10000)

        self.doc_ops = self.input.param("doc_ops", None)
        self.output_time = self.input.param("output_time", False)
        if self.doc_ops is not None:
            self.doc_ops = self.doc_ops.split(";")
        self.defaul_map_func = "function (doc) {\n  emit(doc._id, doc);\n}"

        #define the data that will be used to test
        self.blob_generator = self.input.param("blob_generator", True)
        if self.blob_generator:
            #gen_load data is used for upload before each test(1000 items by default)
            self.gen_load = BlobGenerator('test', 'test-', self.value_size, end=self.num_items)
            #gen_update is used for doing mutation for 1/2th of uploaded data
            self.gen_update = BlobGenerator('test', 'test-', self.value_size, end=(self.num_items / 2 - 1))
            #upload data before each test
            self._load_all_buckets(self.servers[0], self.gen_load, "create", 0)
        else:
            self._load_doc_data_all_buckets()

    def tearDown(self):
        """ Some test involve kill couchbase server.  If the test steps failed
            right after kill erlang process, we need to start couchbase server
            in teardown so that the next test will not be false failed """
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            shell.start_couchbase()
        super(RackzoneBaseTest, self).tearDown()
        serverInfo = self.servers[0]
        rest = RestConnection(serverInfo)
        zones = rest.get_zone_names()
        for zone in zones:
            if zone != "Group 1":
                rest.delete_zone(zone)
        super(RackzoneBaseTest, self).tearDown()
