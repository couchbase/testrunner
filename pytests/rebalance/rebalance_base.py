from basetestcase import BaseTestCase
from couchbase.document import View
from couchbase.documentgenerator import BlobGenerator

class RebalanceBaseTest(BaseTestCase):

    def setUp(self):
        super(RebalanceBaseTest, self).setUp()
        self.value_size = self.input.param("value_size", 256)
        self.doc_ops = self.input.param("doc_ops", None)
        self.withMutationOps = self.input.param("withMutationOps", True)
        self.total_vbuckets = self.input.param("total_vbuckets", 1024)
        if self.doc_ops is not None:
            self.doc_ops = self.doc_ops.split(":")
        self.defaul_map_func = "function (doc) {\n  emit(doc._id, doc);\n}"
        self.default_view_name = "default_view"
        self.default_view = View(self.default_view_name, self.defaul_map_func, None)
        self.std_vbucket_dist = self.input.param("std_vbucket_dist", None)
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

