from lib.couchbase_helper.documentgenerator import BlobGenerator, JsonDocGenerator, JSONNonDocGenerator
from lib.membase.api.rest_client import RestConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from pytests.eventing.eventing_constants import HANDLER_CODE, EXPORTED_FUNCTION
from pytests.eventing.eventing_base import EventingBaseTest, log


class EventingDataset(EventingBaseTest):
    def setUp(self):
        super(EventingDataset, self).setUp()
        if self.create_functions_buckets:
            self.bucket_size = 100
            log.info(self.bucket_size)
            bucket_params = self._create_bucket_params(server=self.server, size=self.bucket_size,
                                                       replicas=self.num_replicas)
            self.cluster.create_standard_bucket(name=self.src_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.src_bucket = RestConnection(self.master).get_buckets()
            self.cluster.create_standard_bucket(name=self.dst_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.cluster.create_standard_bucket(name=self.metadata_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.buckets = RestConnection(self.master).get_buckets()
        self.gens_load = self.generate_docs(self.docs_per_day)
        self.expiry = 3

    def tearDown(self):
        super(EventingDataset, self).tearDown()

    def test_functions_where_dataset_has_binary_and_json_data(self):
        gen_load = BlobGenerator('binary', 'binary-', self.value_size, end=2016 * self.docs_per_day)
        # load binary and json data
        self.cluster.load_gen_docs(self.master, self.src_bucket_name, gen_load, self.buckets[0].kvs[1], "create",
                                   exp=0, flag=0, batch_size=1000)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete both binary and json documents
        self.cluster.load_gen_docs(self.master, self.src_bucket_name, gen_load, self.buckets[0].kvs[1], "delete",
                                   exp=0, flag=0, batch_size=1000)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        count = 0
        stats_dst = self.rest.get_bucket_stats(bucket=self.dst_bucket_name)
        while stats_dst["curr_items"] != 0 and count < 20:
            self.sleep(30, message="Waiting for handler code to complete all delete doc operations...")
            count += 1
            stats_dst = self.rest.get_bucket_stats(bucket=self.dst_bucket_name)
        self.assertEqual(0, stats_dst["curr_items"],
                         "Bucket delete operations from handler code took lot of time to complete or didn't go through")
        self.undeploy_and_delete_function(body)

    def test_functions_where_documents_change_from_binary_to_json_data(self):
        gen_load_binary = BlobGenerator('binary1000000', 'binary', self.value_size, start=1,
                                        end=2016 * self.docs_per_day + 1)
        gen_load_json = JsonDocGenerator('binary', op_type="create", end=2016 * self.docs_per_day)
        # load binary data
        self.cluster.load_gen_docs(self.master, self.src_bucket_name, gen_load_binary, self.buckets[0].kvs[1], "create",
                                   exp=0, flag=0, batch_size=1000)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE)
        self.deploy_function(body)
        # change it to json data
        self.cluster.load_gen_docs(self.master, self.src_bucket_name, gen_load_binary, self.buckets[0].kvs[1], "delete",
                                   exp=0, flag=0, batch_size=1000)
        self.cluster.load_gen_docs(self.master, self.src_bucket_name, gen_load_json, self.buckets[0].kvs[1], "create",
                                   exp=0, flag=0, batch_size=1000)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete all json docs
        self.cluster.load_gen_docs(self.master, self.src_bucket_name, gen_load_json, self.buckets[0].kvs[1], "delete",
                                   exp=0, flag=0, batch_size=1000)
        # Wait for eventing to catch up with all the delete mutations and verify results
        count = 0
        stats_dst = self.rest.get_bucket_stats(bucket=self.dst_bucket_name)
        while stats_dst["curr_items"] != 0 and count < 20:
            self.sleep(30, message="Waiting for handler code to complete all delete doc operations...")
            count += 1
            stats_dst = self.rest.get_bucket_stats(bucket=self.dst_bucket_name)
        self.assertEqual(0, stats_dst["curr_items"],
                         "Bucket delete operations from handler code took lot of time to complete or didn't go through")
        self.undeploy_and_delete_function(body)

    def test_functions_where_dataset_has_binary_and_non_json_data(self):
        gen_load_binary = BlobGenerator('binary', 'binary-', self.value_size, end=2016 * self.docs_per_day)
        values = ['1', '10']
        gen_load_non_json = JSONNonDocGenerator('non_json_docs', values, start=0, end=2016 * self.docs_per_day)
        # load binary and non json data
        self.cluster.load_gen_docs(self.master, self.src_bucket_name, gen_load_binary, self.buckets[0].kvs[1], "create",
                                   exp=0, flag=0, batch_size=1000)
        self.cluster.load_gen_docs(self.master, self.src_bucket_name, gen_load_non_json, self.buckets[0].kvs[1],
                                   "create",
                                   exp=0, flag=0, batch_size=1000)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete non json documents
        self.cluster.load_gen_docs(self.master, self.src_bucket_name, gen_load_non_json, self.buckets[0].kvs[1],
                                   "delete", exp=0, flag=0, batch_size=1000)
        # Wait for eventing to catch up with all the delete mutations and verify results
        count = 0
        stats_dst = self.rest.get_bucket_stats(bucket=self.dst_bucket_name)
        while stats_dst["curr_items"] != 0 and count < 20:
            self.sleep(30, message="Waiting for handler code to complete all delete doc operations...")
            count += 1
            stats_dst = self.rest.get_bucket_stats(bucket=self.dst_bucket_name)
        self.assertEqual(0, stats_dst["curr_items"],
                         "Bucket delete operations from handler code took lot of time to complete or didn't go through")
        self.undeploy_and_delete_function(body)
