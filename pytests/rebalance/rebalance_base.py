from basetestcase import BaseTestCase
from couchbase.document import View
from couchbase.documentgenerator import BlobGenerator

class RebalanceBaseTest(BaseTestCase):

    def setUp(self):
        super(RebalanceBaseTest, self).setUp()
        self.value_size = self.input.param("value_size", 256)
        self.nodes_in = self.input.param("nodes_in", 1)
        self.nodes_out = self.input.param("nodes_out", 1)
        self.doc_ops=self.input.param("doc_ops", None)
        if self.doc_ops is not None:
            self.doc_ops = self.doc_ops.split(";")
        self.defaul_map_func = "function (doc) {\n  emit(doc._id, doc);\n}"
        self.default_view_name = "default_view"
        self.default_view = View(self.default_view_name, self.defaul_map_func, None)

        #define the data that will be used to test
        self.blob_generator = self.input.param("blob_generator", True)
        if self.blob_generator:
            #gen_load data is used for upload before each test(1000 items by default)
            self.gen_load = BlobGenerator('mike', 'mike-', self.value_size, end=self.num_items)
            #gen_update is used for doing mutation for 1/2th of uploaded data
            self.gen_update = BlobGenerator('mike', 'mike-', self.value_size, end=(self.num_items / 2 - 1))
            #upload data before each test
            self._load_all_buckets(self.servers[0], self.gen_load, "create", 0)
        else:
            self._load_doc_data_all_buckets()

    def tearDown(self):
        super(RebalanceBaseTest, self).tearDown()

    def perform_verify_queries(self, num_views, prefix, ddoc_name, query, wait_time=120, bucket="default"):
        tasks = []
        for i in xrange(num_views):
            tasks.append(self.cluster.async_query_view(self.servers[0], prefix + ddoc_name, self.default_view_name + str(i), query, self.num_items, bucket))
        try:
            for task in tasks:
                task.result(wait_time)
        except Exception as e:
            print e;
            for task in tasks:
                task.cancel()
            raise Exception("unable to get expected results for view queries during {0} sec".format(wait_time))