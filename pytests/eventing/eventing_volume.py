from lib.couchbase_helper.tuq_helper import N1QLHelper
from lib.membase.api.rest_client import RestConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from pytests.eventing.eventing_base import EventingBaseTest
import logging

log = logging.getLogger()


class EventingVolume(EventingBaseTest):
    def setUp(self):
        super(EventingVolume, self).setUp()
        self.buckets = RestConnection(self.master).get_buckets()
        self.hostname = "http://qa.sc.couchbase.com/"
        self.create_n_scope(self.dst_bucket_name, 5)
        self.create_n_scope(self.src_bucket_name, 5)
        self.create_n_collections(self.dst_bucket_name, "scope_1", 5)
        self.create_n_collections(self.src_bucket_name, "scope_1", 5)
        self.handler_code = "handler_code/ABO/insert_rebalance.js"
        self.batch_size=10**4

    def tearDown(self):
        super(EventingVolume, self).tearDown()

    def create_save_handlers(self):
        self.create_function_with_collection("bucket_op", "handler_code/ABO/insert_rebalance.js",
                                             collection_bindings=["dst_bucket.dst_bucket.scope_1.coll_0.rw"])
        self.create_function_with_collection("timers", "handler_code/ABO/insert_timer.js",
                                            collection_bindings=["dst_bucket.dst_bucket.scope_1.coll_1.rw"])
        self.create_function_with_collection("sbm", "handler_code/ABO/insert_sbm.js",src_namespace="src_bucket.scope_1.coll_1",
                                             collection_bindings=["src_bucket.src_bucket.scope_1.coll_1.rw"])
        self.create_function_with_collection("curl", "handler_code/ABO/curl_get.js",
                                             collection_bindings=["dst_bucket.dst_bucket.scope_1.coll_3.rw"],is_curl=True)
        self.create_function_with_collection("n1ql", "handler_code/collections/n1ql_insert_rebalance.js",
                                             collection_bindings=["dst_bucket.dst_bucket.scope_1.coll_4.rw"])

    def deploy_all_handlers(self):
        self.deploy_handler_by_name("bucket_op")
        self.deploy_handler_by_name("timers")
        self.deploy_handler_by_name("sbm")
        self.deploy_handler_by_name("curl")
        self.deploy_handler_by_name("n1ql")

    def verify_all_handler(self,number_of_docs):
        self.verify_doc_count_collections("dst_bucket.scope_1.coll_0", number_of_docs)
        self.verify_doc_count_collections("dst_bucket.scope_1.coll_1", number_of_docs)
        self.verify_doc_count_collections("dst_bucket.scope_1.coll_3", number_of_docs)
        self.verify_doc_count_collections("dst_bucket.scope_1.coll_4", number_of_docs)

    def test_eventing_volume(self):
        self.create_save_handlers()
        # load data
        task1=self.load_batch_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default",
                                     wait_for_loading=False)
        task2=self.load_batch_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.scope_1.coll_1",
                                     wait_for_loading=False)
        self.deploy_all_handlers()
        for task in task1:
            task.result()
        for task in task2:
            task.result()
        # Validate the results of all the functions deployed
        self.verify_all_handler(self.docs_per_day * self.num_docs)
        self.verify_doc_count_collections("src_bucket.scope_1.coll_1", self.docs_per_day * self.num_docs * 2)
        # delete json documents
        task1=self.load_batch_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default", is_delete=True,
                                           wait_for_loading=False)
        task2=self.load_batch_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.scope_1.coll_1", is_delete=True,
                                           wait_for_loading=False)
        for task in task1:
            task.result()
        for task in task2:
            task.result()
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_all_handler(0)
        self.verify_doc_count_collections("src_bucket.scope_1.coll_1", self.docs_per_day * self.num_docs)
        self.undeploy_delete_all_functions()
