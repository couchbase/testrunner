from basetestcase import BaseTestCase
from couchbase.document import View
from couchbase.documentgenerator import BlobGenerator

class RackzoneBaseTest(BaseTestCase):

    def setUp(self):
        super(RackzoneBaseTest, self).setUp()
        self.value_size = self.input.param("value_size", 128)
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
        super(RackzoneBaseTest, self).tearDown()
